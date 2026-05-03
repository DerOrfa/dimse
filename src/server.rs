use std::borrow::Cow;
use std::io::{Cursor, Read};
use std::net::SocketAddr;
use dicom::encoding::{TransferSyntax, TransferSyntaxIndex};
use dicom::object::InMemDicomObject;
use dicom::transfer_syntax::TransferSyntaxRegistry;
use dicom_ul::{FullAeAddr, Pdu, ServerAssociationOptions};
use dicom_ul::association::AsyncServerAssociation;
use snafu::ResultExt;
use tokio::net::TcpStream;
use tokio::task::spawn_blocking;
use tokio_util::io::SyncIoBridge;
use tracing::debug;
use crate::association::{Association, ReceivePdu, SendPdu};
use crate::error::{DimseError, Result};

pub struct Server(Option<AsyncServerAssociation<TcpStream>>);
impl Server {
	pub async fn new<'a>(ae_title:impl Into<Cow<'a,str>>, stream: TcpStream, peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>) -> Result<Association<Self>>
	{
		let ae_title = ae_title.into();
		let local = FullAeAddr::new(ae_title.clone(),stream.local_addr()?);
		let peer = FullAeAddr::new(ae_title.clone(),stream.peer_addr()?);
		debug!("New connection from {peer}");
		let srv = Self::new_inner(ae_title.clone(), stream).await?;
		let context = crate::context::Context::new(srv.presentation_contexts());

		Ok(Association::new(local,peer,peers,Server(Some(srv)),context))
	}

	async fn new_inner<'a>(ae_title:Cow<'a,str>, scu_stream: TcpStream) -> Result<AsyncServerAssociation<TcpStream>>
	{
		let mut options = ServerAssociationOptions::new()
			.accept_any()
			.ae_title(ae_title.clone())
			.promiscuous(true);

		for ts in TransferSyntaxRegistry.iter() {
			if !ts.is_unsupported() {
				options = options.with_transfer_syntax(ts.uid());
			}
		}
		options.establish_async(scu_stream).await.map_err(|e|e.into())
	}
}

impl ReceivePdu for Server {
	async fn read_pdu(&mut self) -> Result<Pdu> {
		match self.0 {
			None => panic!("trying to use a checked out connection"),
			Some(ref mut s) => s.receive().await.map_err(|e|e.into()),
		}
	}

	async fn read_pipe(&mut self, prelude: Vec<u8>, ts: &TransferSyntax) -> Result<InMemDicomObject>
	{
		let mut checkout = self.0.take().expect("trying to check out already checked out connection");
		let ts = ts.uid();

		let (o,checkout) = spawn_blocking(move || {
			let source = SyncIoBridge::new(checkout.receive_pdata());
			let source = Read::chain(Cursor::new(prelude), source);
			InMemDicomObject::read_dataset_with_ts(source,TransferSyntaxRegistry::default().get(ts).unwrap())
				.map(|o|(o,checkout))
		}).await??;
		self.0 = Some(checkout);
		Ok(o)
	}
}

impl SendPdu for Server {
	async fn send_obj(&mut self, pc_id: u8, ts: &TransferSyntax, o: InMemDicomObject) -> Result<()> {
		// check out the association
		let mut checkout = self.0.take().expect("trying to check out already checked out connection");
		let ts = ts.uid();

		let checkout = spawn_blocking(move || {
			let mut sink = SyncIoBridge::new(checkout.send_pdata(pc_id));
			let r = o.write_dataset_with_ts(&mut sink, TransferSyntaxRegistry::default().get(ts).unwrap())
				.with_whatever_context::<_,_,DimseError>(|e|format!("Failed sending object: {e}"));
			drop(sink);
			r.map(|_|checkout)
		}).await??;
		self.0 = Some(checkout);
		Ok(())
	}

	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()> {
		match self.0 {
			Some(ref mut a) => a.send(pdu).await.map_err(|e|e.into()),
			None => panic!("trying to write into checked out connection")
		}
	}

	fn get_sink(&mut self, pc_id: u8) -> Result<crate::association::Sink<'_>> {
		let sink = match self.0 {
			None => panic!("trying to use a checked out connection"),
			Some(ref mut s) => Box::new(s.send_pdata(pc_id)),
		};
		Ok(crate::association::Sink::Async(sink))
	}
}
