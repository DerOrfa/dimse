use std::borrow::Cow;
use std::net::SocketAddr;
use dicom::transfer_syntax::TransferSyntaxRegistry;
use dicom_ul::{FullAeAddr, ServerAssociationOptions};
use dicom_ul::association::AsyncServerAssociation;
use tracing::debug;
use crate::error::Result;

pub type ServerAssoc = crate::association::Association<Option<AsyncServerAssociation<tokio::net::TcpStream>>>;
pub struct Server;
impl Server {
	pub async fn new<'a>(ae_title:impl Into<Cow<'a,str>>, stream: tokio::net::TcpStream, peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>) -> Result<ServerAssoc>
	{
		let ae_title = ae_title.into();
		let local = FullAeAddr::new(ae_title.clone(),stream.local_addr()?);
		let peer = FullAeAddr::new(ae_title.clone(),stream.peer_addr()?);
		debug!("New connection from {peer}");
		let srv = Self::new_inner(ae_title.clone(), stream).await?;
		let context = crate::context::Context::new(srv.presentation_contexts());

		Ok(crate::association::Association::new(local,peer,peers,Some(srv),context))
	}

	async fn new_inner<'a>(ae_title:Cow<'a,str>, scu_stream: tokio::net::TcpStream) -> Result<AsyncServerAssociation<tokio::net::TcpStream>>
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
