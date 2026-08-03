#![allow(async_fn_in_trait)]
// todo https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/async_fn_in_trait/static.ASYNC_FN_IN_TRAIT.html

use std::io::Read;
use std::{format, vec};
use std::collections::HashMap;
use crate::definitions::RequestCode;
use crate::error::DimseError::{UnexpectedEmptyMsg, UnexpectedMessageType, UnexpectedNonDataPdu};
use crate::error::{DimseError, Result};
use crate::msg::{MessageType, Request};
use crate::payload::{Attachment, Payload};
use crate::pending_receive::{PendingGet, PendingReceive, PendingStore};
use dicom::core::{PrimitiveValue, VR};
use dicom::encoding::{TransferSyntax};
use dicom::object::{FileDicomObject, InMemDicomObject};
use dicom_ul::pdu::{PDataValue, PresentationContextNegotiated};
use dicom_ul::{FullAeAddr, Pdu};
use snafu::ResultExt;
use std::io::{Cursor, SeekFrom};
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::path::{Path};
use dicom::object::mem::InMemElement;
use dicom::transfer_syntax::{TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_dictionary_std::{tags,uids};
use dicom_ul::association::AsyncAssociation;
use futures::StreamExt;
use futures::stream::{Fuse, FusedStream};
use tokio::io::{AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::task::spawn_blocking;
use tokio_util::io::SyncIoBridge;
use tracing::{debug, trace, warn};
use crate::{lazy_attachment_to_string, RetrieveLevel};
use crate::definitions::RequestCode::{C_FIND, C_GET, C_MOVE, C_STORE};
use crate::{io, msg, Taker};
use crate::context::Context;
use crate::status::{StatusOk, StatusPending, StatusSuccess};

pub(crate) trait SendPdu
{
	async fn send_obj(&mut self, pc_id:u8, ts:&TransferSyntax, obj:InMemDicomObject) -> Result<()>;
	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()>;
	fn get_sink<'a>(&'a mut self, pc_id: u8) -> Result<Box<dyn AsyncWrite + Send + Unpin +'a>>;
	async fn send_file(&mut self, path: &Path, offset: SeekFrom, pc_id: u8) -> Result<u64> {
		let mut sink = self.get_sink(pc_id)?;
		let mut source = tokio::fs::File::open(path).await?;
		tokio::fs::File::seek(&mut source, offset).await?;
		tokio::io::copy(&mut source, sink.deref_mut()).await.map_err(|e|e.into())
	}
}
pub trait SendPayload
{
	async fn send_request(&mut self, request: Request) -> Result<()>;
	async fn send_payload(&mut self, payload: Payload) -> Result<()>;
}
pub(crate) trait ReceivePdu
{
	async fn read_pdu(&mut self) -> Result<Pdu>;
	async fn read_pipe(&mut self, prelude: Vec<u8>, ts:&TransferSyntax) -> Result<InMemDicomObject>;
}
pub trait ReceiveMessage
{
	async fn receive_message(&mut self) -> Result<MessageType>;
}

pub struct Association<A:Sized>
{
	pub(crate) peer:FullAeAddr<SocketAddr>,
	pub(crate) local:FullAeAddr<SocketAddr>,
	pub(crate) inner:A,
	pub(crate) peers:HashMap<String, SocketAddr>,
	pub(crate) context:Context
}

impl<A> Association<A>
{
	pub(crate) fn new<'a>(local:FullAeAddr<SocketAddr>, peer:FullAeAddr<SocketAddr>, peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>, inner:A, context:Context) -> Self
	{
		let peers=peers.into_iter()
			.map(|addr|(addr.ae_title().to_string(),addr.socket_addr().clone())).collect();
		Self{ local, peer, peers, inner, context }
	}
	pub fn peer_address(&self) -> &FullAeAddr<SocketAddr>{&self.peer}
	pub fn local_address(&self) -> &FullAeAddr<SocketAddr>{&self.local}
	async fn detect_pc(&mut self, prelude: &PDataValue) -> Result<&PresentationContextNegotiated> where A: ReceivePdu
	{
		Ok(self.context.select_pc(prelude.presentation_context_id))
	}
	pub(crate) async fn read_pdu(&mut self) -> Result<Pdu> where A: ReceivePdu
	{
		self.inner.read_pdu().await
	}
	pub(crate) async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()> where A: SendPdu
	{
		self.inner.send_pdu(pdu).await
	}
	pub(crate) async fn read_pdata(&mut self) -> Result<Vec<PDataValue>> where A: ReceivePdu
	{
		let mut ret = vec![];
		loop {
			match self.read_pdu().await?
			{
				Pdu::PData {mut data} => {
					ret.append(&mut data);
					if ret.last().ok_or(UnexpectedEmptyMsg)?.is_last { return Ok(ret) }
				},
				actual => return Err(UnexpectedNonDataPdu { actual}),
			}
		}
	}
	pub(crate) async fn fetch_attachment(&mut self) -> Result<Attachment> where A: ReceivePdu
	{
		let prelude = match self.inner.read_pdu().await?
		{
			Pdu::PData {data} => data,
			actual => return Err(UnexpectedNonDataPdu { actual}),
		};
		let pc = self.detect_pc(prelude.first().expect("Unexpected empty pdu")).await?.id;
		let ts = self.context.select_ts(pc)?;
		let complete = prelude.last().unwrap().is_last;
		let prelude = prelude.into_iter()
			.fold(vec![], |mut i,mut p|{i.append(&mut p.data);i});

		if complete {
			InMemDicomObject::read_dataset_with_ts(Cursor::<Vec<u8>>::new(prelude),ts)
				.with_whatever_context(|e|
					format!("Failed parsing object data from {}:{}",self.peer_address(),e))
				.map(|o|Attachment::SmallObj(o))
		} else {
			self.inner.read_pipe(prelude, ts).await
				.with_whatever_context(|e|
					format!("Failed parsing streamed object data from {}:{}",self.peer_address(),e))
				.map(|o|Attachment::Obj(o))
		}
	}
	pub async fn receive_response(&mut self, source_rq: RequestCode) -> Result<msg::Response>
		where Self:ReceiveMessage
	{
		match self.receive_message().await
			.with_whatever_context::<_,_,DimseError>(|e|
				format!("Failed receiving message from {} ({e})", self.peer_address()))?
		{
			MessageType::Request(_) => {
				Err(UnexpectedMessageType { expected: "response".to_string(), actual: "request".to_string() })
			}
			MessageType::Response(response) => {
				if response.command != source_rq{
					Err(UnexpectedMessageType {
						expected: format!("response to {source_rq}"),
						actual: format!("response to {}", response.command)
					})
				} else {
					Ok(response)
				}
			}
		}
	}
	pub fn new_request(&self, command: RequestCode, sop_class: impl AsRef<str>, attr: impl IntoIterator<Item=InMemElement>)
	                   -> Result<Request> where Self: SendPayload
	{
		self.context.find_pc(sop_class)
			.map(|ctx|Request::new(command, ctx, attr))
	}
	pub async fn find(&'_ mut self, level: RetrieveLevel, attr:impl IntoIterator<Item = InMemElement>)
		-> Result<PendingReceive<'_, A>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		let rq = self.new_request(C_FIND, uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND, vec![])?
			.with_identifier(attr.into_iter().chain([InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,level.as_ref())]));

		self.send_request(rq.clone()).await.map(|_|PendingReceive::new(self, rq))
	}
	pub async fn collect_abstract_syntax(&mut self, filter:impl IntoIterator<Item = InMemElement>)
		-> Result<impl FusedStream<Item=Result<String>>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		let attr = filter.into_iter().chain([
			InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
		]);
		
		Ok(self.find(RetrieveLevel::IMAGE,attr).await?
			.map(|o|o.and_then(|mut o|Taker::take(&mut o, tags::SOP_CLASS_UID))))
	}

	pub async fn move_study(&mut self, uid:impl AsRef<str>, dest_aet:impl AsRef<str>)
	                        -> Result<PendingReceive<'_, A>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		self.move_ident([
			InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,RetrieveLevel::STUDY.as_ref()),
			InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, uid.as_ref()),
		], dest_aet).await
	}
	pub async fn move_ident(&mut self, ident:impl IntoIterator<Item=InMemElement>, dest_aet:impl AsRef<str>)
		-> Result<PendingReceive<'_, A>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		let rq = self.new_request(C_MOVE, uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE, vec![
			InMemElement::new(tags::MOVE_DESTINATION,VR::AE, dest_aet.as_ref()),
		])?.with_identifier(ident);

		self.send_request(rq.clone()).await.map(|_|PendingReceive::new(self,rq))
	}

	pub async fn store_file(&mut self, file:&Path, attr:impl IntoIterator<Item=InMemElement>) -> Result<StatusOk> where Self: SendPayload + ReceiveMessage
	{
		// @todo sop class should not be hardwired
		let mut req= self.new_request(C_STORE, uids::ENHANCED_MR_IMAGE_STORAGE,attr)?;
		let ret = req.attach_file(file,&self.context)?; // this will override the SOP Class above
		self.send_request(req).await?;

		loop {
			// wait for confirmation
			match self.receive_response(C_STORE).await?.status
			{
				Ok(StatusOk::Success(_)) => return Ok(ret),
				Ok(StatusOk::Warning(w)) => {
					warn!("Unexpected Warning reply from c-store {w} ");
					return Ok(w.into())
				},
				Ok(StatusOk::Pending{..}) => {},
				Err(e) =>
					return Err(e).with_whatever_context::<_,_,DimseError>(|e|
						format!("Failure reading result of store request: {e}"))?
			}
		}
	}

	/// Makes a C-GET call for a higher retrieve level streaming all instances below it
	pub async fn get_bulk<'a>(&'a mut self, request_uid:impl AsRef<str>, level:RetrieveLevel) -> Result<PendingStore<'a,A>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		let request_tag = match level {
			RetrieveLevel::PATIENT => todo!(),
			RetrieveLevel::STUDY => tags::STUDY_INSTANCE_UID,
			RetrieveLevel::SERIES => tags::SERIES_INSTANCE_UID,
			RetrieveLevel::IMAGE => {
				panic!("Use get_file instead")
			}
		};
		debug!("Searching for images to get");
		let mut requests = vec![];
		let mut sop_class:Option<String> = None;
		let mut found =self.find(RetrieveLevel::IMAGE, [
			InMemElement::new(request_tag, VR::UI, request_uid.as_ref()),
			InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
			InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		]).await?;
		while let Some(mut obj) = found.next().await.transpose()? {
			let uid:String = Taker::take(&mut obj, tags::SOP_INSTANCE_UID)?;
			let got_sop:String = Taker::take(&mut obj, tags::SOP_CLASS_UID)?;
			if let Some(set_sop) = &sop_class {
				if *set_sop != got_sop {
					warn!("Ignoring conflicting sop class {got_sop} for {uid}, maybe try a lower retrieve level");
				}
			} else {
				sop_class = Some(got_sop);
			}
			requests.push(uid);
		}
		drop(found);

		let req= self.new_request(C_GET, sop_class.expect("Missing SOP class") ,vec![])?
			.with_identifier([
				InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,level.as_ref()),
				InMemElement::new(request_tag, VR::UI, request_uid.as_ref()),
			]);
		self.send_request(req.clone()).await?;
		Ok(PendingStore::new(self, req))
	}

	/// Makes a list of instances based on the given UID and the retrieve level and executes C-GET calls for all found instances.
	pub async fn get<'a>(&'a mut self, request_uid:impl AsRef<str>, level:RetrieveLevel) -> Result<Fuse<PendingGet<'a, A>>> where Self: ReceiveMessage + SendPayload + Unpin
	{
		let request_tag = match level {
			RetrieveLevel::PATIENT => todo!(),
			RetrieveLevel::STUDY => tags::STUDY_INSTANCE_UID,
			RetrieveLevel::SERIES => tags::SERIES_INSTANCE_UID,
			RetrieveLevel::IMAGE => {
				panic!("Use get_file instead")
			}
		};		
		debug!("Searching for images to get");
		let mut requests = vec![];
		let mut found =self.find(RetrieveLevel::IMAGE, [
			InMemElement::new(request_tag, VR::UI, request_uid.as_ref()),
			InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
			InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		]).await?;
		while let Some(mut obj) = found.next().await.transpose()? {
			let uid:Result<String> = Taker::take(&mut obj, tags::SOP_INSTANCE_UID);
			let sop_class:Result<String> = Taker::take(&mut obj, tags::SOP_CLASS_UID);
			requests.push((uid?,sop_class?));
		}
		drop(found);

		PendingGet::new(self, requests)
	}
	/// Executes a C-GET for a single instance.
	pub async fn get_file(&mut self, uid:impl AsRef<str>, sop_class: impl AsRef<str>) -> Result<FileDicomObject<InMemDicomObject>>
		where Self:SendPayload, Self:ReceiveMessage
	{
		// send request
		// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.html#sect_C.4.3.1
		let req= self.new_request(C_GET, sop_class ,vec![])?
			.with_identifier([
				InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,RetrieveLevel::IMAGE.as_ref()),
				InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, uid.as_ref()),
			]);
		self.send_request(req).await?;
		let mut ret:Option<_> = Default::default();

		// wait for responses
		loop {
			match self.receive_message().await?
			{
				MessageType::Request(mut req) => { // this should be a C_STORE request
					if req.command == C_STORE {
						if let Some(o) = req.take_attachment_obj()
						{
							let ts: Option<String> = Taker::get_opt(&o, tags::TRANSFER_SYNTAX_UID)?;
							let ts = ts.as_deref().unwrap_or(self.context.select_ts(req.pc_id)?.uid());
							let response = io::make_file(o, ts).map(|o|{
								ret = Some(o);StatusSuccess.into()
							});
							let response = req.make_response(response);
							self.send_payload(response.try_into()?).await?
						} else { panic!("Expected C-STORE request, got {req} instead") }
					}
				}
				MessageType::Response(resp) => { // either pending or finish (or fail :-( )
					match resp.status {
						Ok(StatusOk::Success(_)) | Ok(StatusOk::Warning {..}) => {
							return Ok(ret.unwrap());
						}
						Ok(StatusOk::Pending (StatusPending{ succeeded, warn, fail, .. })) => {
							debug!("Success:{} Warning:{} Fail:{}",
								succeeded.unwrap_or_default(), warn.unwrap_or_default(), fail.unwrap_or_default()
							);
						}
						Err(e) => return Err(e.into())
					}
				}
			}
		}
	}
}

impl<A> SendPayload for Association<A> where A:SendPdu
{
	async fn send_request(&mut self, mut request: Request) -> Result<()> {
		let sop_class= self.context.select_pc(request.pc_id).abstract_syntax.as_str();
		request.obj.put_str(tags::AFFECTED_SOP_CLASS_UID, VR::UI, sop_class);
		self.send_payload(request.try_into()?).await
	}

	async fn send_payload(&mut self, payload: Payload) -> Result<()>
	{
		let pc_id= payload.command.presentation_context_id;
		self.send_pdu(&Pdu::PData {data:vec![payload.command]}).await?;
		match payload.attachment {
			Attachment::File{ path, offset, pc_id } => {
				self.inner.send_file(path.as_path(), SeekFrom::Start(offset), pc_id).await
					.map(drop)
					.with_whatever_context(|e|
						format!("Failed sending file {} to {}: {e}",path.display(), self.local))
			}
			Attachment::SmallObj(o) => {
				let mut buf = Cursor::new(Vec::new());
				o.write_dataset_with_ts(&mut buf,self.context.select_ts(pc_id)?)?;
				let mut sink = self.inner.get_sink(pc_id)?;
				sink.deref_mut().write_all(buf.get_ref()).await
					.map_err(|e| e.into())
			}
			Attachment::Obj(o) => {
				debug!("{} sending large object to {}", self.local, self.peer);
				let ts = self.context.select_ts(pc_id)?;
				self.inner.send_obj(pc_id, ts, o).await
			},
			Attachment::None => Ok(())
		}
	}
}

impl<A> ReceiveMessage for Association<A> where A:ReceivePdu {
	async fn receive_message(&mut self) -> Result<MessageType> {
		msg::Message::read_from(self).await
			.map(|m|{
				debug!("{} received {} from {}",self.local,	m, self.peer_address());
				let att = match &m {
					MessageType::Request(r) => r.attachment(),
					MessageType::Response(r) => r.attachment(),
				};
				lazy_attachment_to_string(att).map(|s|trace!(s));
				m
			}).with_whatever_context(|e|format!("Failed getting message from {} ({e})", self.peer_address()))
	}
}

impl<T> ReceivePdu for Option<T> where T:AsyncAssociation<tokio::net::TcpStream> + Send + 'static
{
	async fn read_pdu(&mut self) -> Result<Pdu> {
		match self {
			None => panic!("trying to use a checked out connection"),
			Some(s) => AsyncAssociation::receive(s).await.map_err(|e|e.into()),
		}
	}

	async fn read_pipe(&mut self, prelude: Vec<u8>, ts: &TransferSyntax) -> Result<InMemDicomObject>
	{
		let mut checkout = self.take().expect("trying to check out already checked out connection");
		let ts = ts.uid();

		let (o,checkout) = spawn_blocking(move || {
			let source = SyncIoBridge::new(checkout.receive_pdata());
			let source = Read::chain(Cursor::new(prelude), source);
			InMemDicomObject::read_dataset_with_ts(source,TransferSyntaxRegistry::default().get(ts).unwrap())
				.map(|o|(o,checkout))
		}).await??;
		self.replace(checkout);
		Ok(o)
	}
}

impl<T> SendPdu for Option<T>  where T:AsyncAssociation<tokio::net::TcpStream> + Send + 'static
{
	async fn send_obj(&mut self, pc_id: u8, ts: &TransferSyntax, o: InMemDicomObject) -> Result<()> {
		// check out the association
		let mut checkout = self.take().expect("trying to check out already checked out connection");
		let ts = ts.uid();

		let checkout = spawn_blocking(move || {
			let mut sink = SyncIoBridge::new(checkout.send_pdata(pc_id));
			let r = o.write_dataset_with_ts(&mut sink, TransferSyntaxRegistry::default().get(ts).unwrap())
				.with_whatever_context::<_,_,DimseError>(|e|format!("Failed sending object: {e}"));
			drop(sink);
			r.map(|_|checkout)
		}).await??;
		self.replace(checkout);
		Ok(())
	}

	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()> {
		match self {
			Some(a) => AsyncAssociation::send(a,pdu).await.map_err(|e|e.into()),
			None => panic!("trying to write into checked out connection")
		}
	}

	fn get_sink<'a>(&'a mut self, pc_id: u8) -> Result<Box<dyn AsyncWrite + Send + Unpin + 'a>> {
		let sink = match self {
			None => panic!("trying to use a checked out connection"),
			Some(s) => Box::new(s.send_pdata(pc_id)),
		};
		Ok(sink)
	}
}

pub trait Release
{
	async fn release(self) -> Result<()>;
}

impl<T> Release for Association<Option<T>>  where T:AsyncAssociation<tokio::net::TcpStream> + Send + 'static {
	async fn release(mut self) -> Result<()> {
		let assoc = self.inner.take().expect("trying to release already checked out connection");
		assoc.release().await.map_err(|e|e.into())
	}
}
