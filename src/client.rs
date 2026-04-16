use std::collections::{HashSet};
use std::io::{Cursor, Read};
use std::net::SocketAddr;
use dicom_ul::{AeAddr, ClientAssociation, ClientAssociationOptions, FullAeAddr, Pdu};
use std::time::Duration;
use dicom::encoding::{TransferSyntax};
use dicom::object::InMemDicomObject;
use dicom_ul::association::{Association};
use dicom_ul::pdu::{PresentationContextNegotiated};
use snafu::{prelude::*};
use strum_macros::AsRefStr;
use tracing::debug;
use crate::association::{ReceivePdu, SendPdu, Sink};
use crate::definitions::{QUERY_RETRIEVE_SYNTAXES, STORAGE_SYNTAXES};
use crate::error::{DimseError, Result};

pub struct Client(ClientAssociation<std::net::TcpStream>);

#[derive(AsRefStr, Clone, Debug)]
pub enum RetrieveLevel
{
	PATIENT,
	STUDY,
	SERIES,
	IMAGE
}

impl Client {
	pub fn new<S>(addr: AeAddr<SocketAddr>, calling_ae:Option<S>, abstract_syntaxes:impl IntoIterator<Item=&'static str>)
		-> Result<crate::association::Association<Self>> where S:AsRef<str>
	{
		debug!("Connecting to {addr}");
		let calling_ae = calling_ae.as_ref().map(AsRef::as_ref);
		let mut inner = Client(Self::new_inner(&addr,abstract_syntaxes,calling_ae)?);

		let context = crate::context::Context::new(inner.0.presentation_contexts());
		let peer = inner.0.inner_stream().peer_addr()?;
		let local = inner.0.inner_stream().local_addr()?;
		Ok(crate::association::Association::new(
			FullAeAddr::new(inner.0.peer_ae_title(), peer),
			FullAeAddr::new(calling_ae.unwrap_or("ANY-SCU"), local),
			inner,
			context
		))
	}
	fn new_inner(addr: &AeAddr<SocketAddr>, abstract_syntaxes:impl IntoIterator<Item=&'static str>, calling_ae:Option<&str>) -> Result<ClientAssociation<std::net::TcpStream>>
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
			.establish(addr)
			.with_whatever_context::<_,_,DimseError>(|e|format!("DIMSE association with {addr} failed: {e}"))
	}
	pub fn probe<S>(addr: AeAddr<SocketAddr>, calling_ae:Option<S>)
		-> Result<Vec<PresentationContextNegotiated>> where S:AsRef<str>
	{
		// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_d.3.2.html
		// ask scp for association using all known abstract syntaxes (vec![] means all will be requested)
		let calling_ae = calling_ae.as_ref().map(AsRef::as_ref);
		let contexts = Client::new_inner(&addr,vec![],	calling_ae)?
			.presentation_contexts().into_iter()
			.map(|c|c.clone())
			.collect();
		Ok(contexts)
	}
}

impl ReceivePdu for Client
{
	async fn read_pdu(&mut self) -> Result<Pdu> {
		self.0.receive().map_err(|e|e.into())
	}

	async fn read_pipe(&mut self, prelude: Vec<u8>, ts: &TransferSyntax) -> Result<InMemDicomObject>
	{
		let source = self.0.receive_pdata();
		let mut buffer = Cursor::new(prelude);

		InMemDicomObject::read_dataset_with_ts(Read::chain(&mut buffer,source),ts)
			.map_err(|e| e.into())
	}
}

impl SendPdu for Client
{
	async fn send_obj(&mut self, pc_id:u8, ts: &TransferSyntax, o: InMemDicomObject) -> Result<()> {
		o.write_dataset_with_ts(self.0.send_pdata(pc_id),ts)
			.with_whatever_context(|e|format!("Failed sending object: {e}"))
	}

	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()> {
		self.0.send(&pdu).map_err(|e|e.into())
	}

	fn get_sink(&mut self, pc_id: u8) -> Result<Sink<'_>> {
		let sink = Box::new(self.0.send_pdata(pc_id));
		Ok(Sink::Sync(sink))
	}
}
