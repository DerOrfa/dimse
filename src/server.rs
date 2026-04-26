use std::borrow::Cow;
use std::io::{Cursor, Read, SeekFrom};
use std::path::PathBuf;
use dicom::encoding::{TransferSyntax, TransferSyntaxIndex};
use dicom::object::InMemDicomObject;
use dicom::transfer_syntax::TransferSyntaxRegistry;
use dicom_ul::{FullAeAddr, Pdu, ServerAssociationOptions};
use dicom_ul::association::AsyncServerAssociation;
use snafu::ResultExt;
use tokio::io::AsyncSeekExt;
use tokio::net::TcpStream;
use tokio_util::io::SyncIoBridge;
use tracing::debug;
use crate::association::{Association, ReceivePdu, SendPayload, SendPdu};
use crate::error::DimseError::{TransferSyntaxNotFound};
use crate::error::{DimseError, Result};
use crate::lazy_dump_to_string;
use crate::msg::Request;
use crate::payload::{Attachment, Payload};

pub struct Server(AsyncServerAssociation<TcpStream>);
impl Server {
	pub async fn new<'a>(ae_title:impl Into<Cow<'a,str>>, stream: TcpStream) -> Result<Association<Self>>
	{
		let ae_title = ae_title.into();
		let local = FullAeAddr::new(ae_title.clone(),stream.local_addr()?);
		let peer = FullAeAddr::new(ae_title.clone(),stream.peer_addr()?);

		let inner = Server(Self::new_inner(ae_title.clone(), stream).await?);
		debug!("New connection from {peer}");
		let context = crate::context::Context::new(inner.0.presentation_contexts());
		Ok(Association::new(local,peer,inner,context))
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
	async fn send_file(&mut self, filename: PathBuf, offset: u64, pc_id: u8) -> tokio::io::Result<()>
	{
		let mut source = tokio::fs::File::open(filename).await?;
		source.seek(SeekFrom::Start(offset)).await?;
		let mut sink = self.0.send_pdata(pc_id);
		tokio::io::copy(&mut source,&mut sink).await.map(|_|())
	}

}

impl ReceivePdu for Server {
	async fn read_pdu(&mut self) -> Result<Pdu> {self.0.receive().await.map_err(|e|e.into())}

	async fn read_pipe(&mut self, prelude: Vec<u8>, ts: &TransferSyntax) -> Result<InMemDicomObject>
	{
		let source = SyncIoBridge::new(self.0.receive_pdata());
		let mut buffer = Cursor::new(prelude);

		InMemDicomObject::read_dataset_with_ts(Read::chain(&mut buffer,source),ts)
			.map_err(|e| e.into())
	}
}

impl SendPdu for Server {
	async fn send_obj(&mut self, pc_id: u8, ts: &TransferSyntax, o: InMemDicomObject) -> Result<()> {
		let mut sink = self.0.send_pdata(pc_id);
		let mut source = Cursor::new(Vec::new());
		o.write_dataset_with_ts(&mut source, ts)
			.with_whatever_context::<_,_,DimseError>(|e|format!("Failed sending object: {e}"))?;
		source.seek(SeekFrom::Start(0)).await?;
		tokio::io::copy(&mut source, &mut sink).await.map(|_|())
			.with_whatever_context(|e|format!("Failed sending object: {e}"))
	}

	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()> {
		self.0.send(pdu).await.map_err(|e|e.into())
	}

	fn get_sink(&mut self, pc_id: u8) -> Result<crate::association::Sink<'_>> {
		let sink = Box::new(self.0.send_pdata(pc_id));
		Ok(crate::association::Sink::Async(sink))
	}
}

impl SendPayload for Server {
	async fn send_request(&mut self, request: Request) -> Result<()>
	{
		self.send_payload(request.try_into()?).await
	}
	async fn send_payload(&mut self, payload: Payload) -> Result<()>
	{
		let pc_id= payload.command.presentation_context_id;
		self.0.send(&Pdu::PData {data:vec![payload.command]}).await?;
		match payload.attachment {
			Attachment::File{ path, offset, pc_id } =>
				self.send_file(path.clone(), offset, pc_id).await
					.with_whatever_context(|e|
						format!("Failed sending file {}: {e}",path.display())),
			Attachment::Obj(o) => {
				lazy_dump_to_string(&o).map(|s|debug!("sending object:\n{s}"));
				let pc = self.0.presentation_contexts()
					.iter().find(|c|c.id == pc_id)
					.expect("Invalid presentation context");
				let reg = TransferSyntaxRegistry::default();
				let ts = reg.get(pc.transfer_syntax.as_str())
					.ok_or(TransferSyntaxNotFound{ts:pc.transfer_syntax.clone()})?;

				let sink = SyncIoBridge::new(self.0.send_pdata(pc_id));
				o.write_dataset_with_ts(sink,ts)
					.with_whatever_context(|e|format!("Failed sending object: {e}"))
			},
			Attachment::None => Ok(())
		}
	}

}