use std::collections::{HashSet};
use std::net::SocketAddr;
use dicom_ul::{AeAddr, ClientAssociationOptions, FullAeAddr};
use std::time::Duration;
use dicom_ul::association::{Association, AsyncClientAssociation};
use dicom_ul::pdu::{PresentationContextNegotiated};
use snafu::{prelude::*};
use tracing::debug;
use crate::definitions::{QUERY_RETRIEVE_SYNTAXES, STORAGE_SYNTAXES};
use crate::error::{DimseError, Result};

pub type ClientAssociation = crate::association::Association<Option<AsyncClientAssociation<tokio::net::TcpStream>>>;
pub struct Client;
impl Client {
	pub async fn new<S>(addr: AeAddr<SocketAddr>, calling_ae:Option<S>, abstract_syntaxes:impl IntoIterator<Item=&'static str>)
		-> Result<ClientAssociation> where S:AsRef<str>
	{
		debug!("Connecting to {addr}");
		let calling_ae = calling_ae.as_ref().map(AsRef::as_ref);
		let mut inner = Self::new_inner(&addr,abstract_syntaxes,calling_ae).await?;

		let context = crate::context::Context::new(inner.presentation_contexts());
		let peer = inner.inner_stream().peer_addr()?;
		let local = inner.inner_stream().local_addr()?;
		Ok(crate::association::Association::new(
			FullAeAddr::new(calling_ae.unwrap_or("ANY-SCU"), local),
			FullAeAddr::new(inner.peer_ae_title(), peer), [],
			Some(inner),
			context
		))
	}
	async fn new_inner(addr: &AeAddr<SocketAddr>, abstract_syntaxes:impl IntoIterator<Item=&'static str>, calling_ae:Option<&str>) -> Result<AsyncClientAssociation<tokio::net::TcpStream>>
	{
		// https://www.dicomlibrary.com/dicom/sop/
		// https://www.dicomlibrary.com/dicom/transfer-syntax/
		let mut abstract_syntaxes:Vec<_> = abstract_syntaxes.into_iter().collect();
		if abstract_syntaxes.is_empty() // if no abstract syntaxes are specified, all known will be tried
		{
			abstract_syntaxes = STORAGE_SYNTAXES.iter().chain(QUERY_RETRIEVE_SYNTAXES)
				.map(|s|*s)
				.collect::<HashSet<_>>()// hash set to make them sorted and unique
				.into_iter().collect();
		}
		let inner = abstract_syntaxes.iter()
			.fold(ClientAssociationOptions::new(),|a,s|a.with_abstract_syntax(*s));

		debug!("Connecting to {addr}");
		inner
			.read_timeout(Duration::from_secs(60))
			.write_timeout(Duration::from_secs(60))
			.calling_ae_title(calling_ae.unwrap_or("ANY-SCU".into()))
			.called_ae_title(addr.ae_title().unwrap_or("ANY-SCP"))
			.establish_async(addr.socket_addr()).await
			.with_whatever_context::<_,_,DimseError>(|e|format!("DIMSE association with {addr} failed: {e}"))
	}
	pub async fn probe<S>(addr: AeAddr<SocketAddr>, calling_ae:Option<S>)
		-> Result<Vec<PresentationContextNegotiated>> where S:AsRef<str>
	{
		// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_d.3.2.html
		// ask scp for association using all known abstract syntaxes (vec![] means all will be requested)
		let calling_ae = calling_ae.as_ref().map(AsRef::as_ref);
		let contexts = Client::new_inner(&addr,vec![],	calling_ae).await?
			.presentation_contexts().into_iter()
			.map(|c|c.clone())
			.collect();
		Ok(contexts)
	}
}
