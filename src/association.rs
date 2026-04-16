#![allow(async_fn_in_trait)]
// todo https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/async_fn_in_trait/static.ASYNC_FN_IN_TRAIT.html

use std::io::Seek;
use std::{format, vec};
use std::collections::HashSet;
use crate::definitions::{RequestCode, StatusOk};
use crate::error::DimseError::{UnexpectedEmptyMsg, UnexpectedMessageType, UnexpectedNonDataPdu};
use crate::error::{DimseError, Result};
use crate::msg::{MessageType, Request};
use crate::payload::{Attachment, Payload};
use crate::pending_receive::PendingReceive;
use dicom::core::{PrimitiveValue, VR};
use dicom::encoding::{TransferSyntax};
use dicom::object::{FileDicomObject, InMemDicomObject};
use dicom_ul::pdu::{PDataValue, PresentationContextNegotiated};
use dicom_ul::{FullAeAddr, Pdu};
use snafu::ResultExt;
use std::io::{stdout, Cursor, SeekFrom, Write};
use std::mem::take;
use std::net::SocketAddr;
use std::ops::DerefMut;
use std::path::{Path};
use dicom::dump::DumpOptions;
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::tags;
use dicom_dictionary_std::uids;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::io::{AsyncSeekExt, AsyncWrite};
use tracing::{debug, warn};
use crate::client::RetrieveLevel;
use crate::definitions::RequestCode::{C_FIND, C_GET, C_MOVE, C_STORE};
use crate::{io, lazy_dump_to_string, msg, Taker};
use crate::context::Context;

pub(crate) enum Sink<'a>
{
	Async(Box<dyn AsyncWrite + Send + Unpin + 'a>),
	Sync(Box<dyn Write + Send + 'a>)
}
pub(crate) trait SendPdu
{
	async fn send_obj(&mut self, pc_id:u8, ts:&TransferSyntax, obj:InMemDicomObject) -> Result<()>;
	async fn send_pdu(&mut self, pdu: &Pdu) -> Result<()>;
	fn get_sink(&mut self, pc_id: u8) -> Result<Sink<'_>>;
	async fn send_file(&mut self, path: &Path, offset: SeekFrom, pc_id: u8) -> Result<u64> {
		match self.get_sink(pc_id)? {
			Sink::Async(mut a) => {
				let mut source = tokio::fs::File::open(path).await?;
				tokio::fs::File::seek(&mut source, offset).await?;
				tokio::io::copy(&mut source, a.deref_mut()).await.map_err(|e|e.into())
			}
			Sink::Sync(mut s) => {
				let mut source = std::fs::File::open(path)?;
				source.seek(offset)?;
				std::io::copy(&mut source, s.deref_mut()).map_err(|e|e.into())
			}
		}
	}
}
pub trait SendPayload
{
	async fn send_request(&mut self, request: Request) -> Result<()>
	{
		self.send_payload(request.try_into()?).await
	}
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
	pub(crate) context:Context
}

impl<A> Association<A>
{
	pub fn new<'a>(peer:FullAeAddr<SocketAddr>, local:FullAeAddr<SocketAddr>, inner:A, context:Context) -> Self
	{
		Self{peer, local, inner, context}
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
	pub(crate) async fn fetch_obj(&mut self) -> Result<InMemDicomObject> where A: ReceivePdu
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
		} else {
			self.inner.read_pipe(prelude, ts).await
				.with_whatever_context(|e|
					format!("Failed parsing streamed object data from {}:{}",self.peer_address(),e))
		}
	}
	pub async fn receive_response(&mut self, source_rq: RequestCode) -> Result<msg::Response>
		where Self: Sized + ReceiveMessage
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
	pub fn new_request(&self, command: RequestCode, sop_class:impl ToString, attr: impl IntoIterator<Item=InMemElement>) -> Result<Request>
		where Self: SendPayload
	{
		self.context.find_pc(sop_class)
			.map(|ctx|Request::new(command, ctx, attr))
	}
	pub async fn find(&mut self, level: RetrieveLevel, attr:impl IntoIterator<Item = InMemElement>) -> Result<impl Stream<Item=Result<InMemDicomObject>>>
		where Self: ReceiveMessage + SendPayload + Unpin
	{
		let rq = self.new_request(C_FIND, uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND, vec![])?
			.with_identifier(attr.into_iter().chain([InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,level.as_ref())]));

		self.send_request(rq).await.map(|_|PendingReceive::new(self, C_FIND))
	}
	pub async fn collect_abstract_syntax(&mut self, filter:impl IntoIterator<Item = InMemElement>) -> Result<impl Stream<Item=Result<String>>>
		where Self: ReceiveMessage + SendPayload + Unpin
	{
		let attr = filter.into_iter().chain([
			InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
		]);
		
		Ok(self.find(RetrieveLevel::IMAGE,attr).await?
			.map(|o|o.and_then(|mut o|Taker::take(&mut o, tags::SOP_CLASS_UID))))
	}

	pub async fn move_study(&mut self, uid:impl AsRef<str>, dest_aet:impl AsRef<str>) -> Result<()>
		where Self: ReceiveMessage + SendPayload
	{
		let rq = self.new_request(C_MOVE, uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE, vec![
			InMemElement::new(tags::MOVE_DESTINATION,VR::AE, dest_aet.as_ref()),
		])?.with_identifier([
			InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,RetrieveLevel::STUDY.as_ref()),
			InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, uid.as_ref()),
		]);

		self.send_request(rq).await?;
		loop {
			let r = self.receive_response(C_MOVE).await?;
			match r.status {
				Ok(StatusOk::Success) => {
					println!("Done:");
					break
				}
				Ok(StatusOk::Pending{ succeeded, warn, fail, to_do }) => {
					println!("Pending: Success:{}/{} Warning:{} Fail:{}",
							 succeeded.unwrap_or(0),
							 to_do.unwrap_or(0),
							 warn.unwrap_or(0),
							 fail.unwrap_or(0),
					);
					if let Attachment::Obj(obj) = r.into_attachment()
					{
						DumpOptions::new().dump_object_to(stdout(),&obj)?;
					}
				},
				Ok(StatusOk::Warning{ code, .. }) =>
					warn!("{code}"),
				Err(e) =>
					return Err(e).with_whatever_context::<_,_,DimseError>(|e|
						format!("Failure reading move results: {e}"))?
			}
		}
		Ok(())
	}

	pub async fn store_file(&mut self, file:&Path) -> Result<()> where Self: SendPayload + ReceiveMessage
	{
		let req= self.new_request(C_STORE, uids::ENHANCED_MR_IMAGE_STORAGE,vec![])?
			.with_file(file,&self.context)?;
		self.send_request(req).await?;

		loop {
			// wait for confirmation
			match self.receive_response(C_STORE).await?.status
			{
				Ok(StatusOk::Success) =>
					return Ok(()),
				Ok(StatusOk::Warning{ code, .. }) => {
					warn!("Unexpected Warning reply from c-store {code:?} ");
					return Ok(())
				},
				Ok(StatusOk::Pending{..}) => {},
				Err(e) =>
					return Err(e).with_whatever_context::<_,_,DimseError>(|e|
						format!("Failure reading result of store request: {e}"))?
			}
		}
	}
	pub async fn get_file(&mut self, uid:impl AsRef<str>) -> Result<FileDicomObject<InMemDicomObject>>
		where Self:SendPayload, Self:ReceiveMessage
	{
		// send request
		// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_C.4.3.html#sect_C.4.3.1
		let req= self.new_request(C_GET, uids::ENHANCED_MR_IMAGE_STORAGE ,vec![])?
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
						if let Attachment::Obj(o) = take(&mut req.attachment)
						{
							let ts: Option<String> = Taker::get_opt(&o, tags::TRANSFER_SYNTAX_UID)?;
							let ts = ts.as_deref().unwrap_or(self.context.select_ts(req.pc_id)?.uid());
							let response = io::make_file(o, ts).await.map(|o|{
								ret = Some(o);StatusOk::Success
							});
							let response = req.make_response(response);
							self.send_payload(response.try_into()?).await?
						} else { panic!("Expected C-STORE request, got {req} instead") }
					}
				}
				MessageType::Response(resp) => { // either pending or finish (or fail :-( )
					match resp.status {
						Ok(StatusOk::Success) | Ok(StatusOk::Warning {..}) => {
							println!("Done:");
							return Ok(ret.unwrap());
						}
						Ok(StatusOk::Pending { succeeded, warn, fail, to_do }) => {
							debug!("Success:{} Warning:{} Fail:{}",
								succeeded.unwrap_or_default(), warn.unwrap_or_default(), fail.unwrap_or_default()
							);
						}
						Err(e) => {
							eprintln!("Error {e}");
							return Err(e.into())
						}
					}
				}
			}
		}

	}
}

impl<A> SendPayload for Association<A> where A:SendPdu
{
	async fn send_payload(&mut self, payload: Payload) -> Result<()>
	{
		let pc_id= payload.command.presentation_context_id;
		self.send_pdu(&Pdu::PData {data:vec![payload.command]}).await?;
		match payload.attachment {
			Attachment::File{ path, offset, pc_id } =>{
				self.inner.send_file(path.as_path(), SeekFrom::Start(offset), pc_id).await
					.map(drop)
					.with_whatever_context(|e|
						format!("Failed sending file {} to {}: {e}",path.display(), self.local))
			}
			Attachment::Obj(o) => {
				lazy_dump_to_string(&o).map(|s|debug!("{} sending object to {}:\n{s}", self.local, self.peer));
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
				let att = match &m {
					MessageType::Request(r) => {
						debug!("{} received {} from {}:\n{}",
							self.local,
							r,
							self.peer,
							lazy_dump_to_string(&r.obj).unwrap_or("".to_string())
						);
						&r.attachment
					}
					MessageType::Response(r) => {
						debug!("{} received {} from {}:\n{}",
							self.local,
							r,
							self.peer_address(),
							lazy_dump_to_string(&r.obj).unwrap_or("".to_string())
						);
						&r.attachment
					}
				};
				if let Attachment::Obj(o) = att {
					debug!("With attachment\n{}", lazy_dump_to_string(o).unwrap_or("".to_string()));
				}
				m
			}).with_whatever_context(|e|format!("Failed getting message from {} ({e})", self.peer_address()))
	}
}
