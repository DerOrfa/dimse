use std::collections::HashMap;
use crate::association::{Association,ReceiveMessage, SendPayload};
use crate::context::Context;
use crate::definitions::{FailureCode, RequestCode};
use crate::msg::{MessageType, Request, Response};
use crate::payload::Payload;
use crate::{io, Taker};
use dicom_dictionary_std::tags;
use dicom_ul::{AeAddr, FullAeAddr};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use dicom::core::VR;
use dicom::dicom_value;
use dicom::object::mem::InMemElement;
use dicom_ul::association::Error::ConnectionClosed;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use tracing::{debug, error};
use crate::error::DimseError::UnsupportedPC;
use crate::io::FileAccess;
use crate::status::*;
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::pin;
use crate::identifier::Identifier;

pub struct Task<B> where B:FileAccess
{
	assoc:Association<TaskPipe>,
	backend:B,
}
pub struct TaskHandle
{
	pub data_sink: Sender<Response>,
	pub msg_register:Arc<RwLock<HashMap<u16, RequestCode>>>,
	pub handle: tokio::task::AbortHandle,
}
pub struct TaskPipe{
	source:Receiver<Response>,
	msg_register:Arc<RwLock<HashMap<u16,RequestCode>>>,
	sink:Sender<Payload>,
}

impl<B> Deref for Task<B> where B:FileAccess
{
	type Target = Association<TaskPipe>;
	fn deref(&self) -> &Self::Target {&self.assoc}
}
impl<B> DerefMut for Task<B> where B:FileAccess
{
	fn deref_mut(&mut self) -> &mut Self::Target {&mut self.assoc}
}
impl<B> Task<B> where B:FileAccess
{
	pub fn new(
		local:FullAeAddr<SocketAddr>,
		peer:FullAeAddr<SocketAddr>,
		peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>,
		to_dispatch_sink:Sender<Payload>,
		from_dispatch_source:Receiver<Response>,
		context:Context,
		backend:B,
		msg_register:Arc<RwLock<HashMap<u16,RequestCode>>>,
	) -> Self where B:FileAccess
	{
		let pipe = TaskPipe{source: from_dispatch_source, msg_register, sink:to_dispatch_sink};
		let assoc = Association::new( local, peer,peers, pipe, context);
		Task{ assoc, backend}
	}
	pub async fn process(&mut self, request: &mut Request) -> Status
	{
		match &request.deref().command
		{
			RequestCode::C_STORE => {
				debug!("Got STORE request");
				if let Some(o) = request.take_attachment_obj(){
					let ts = Taker::get_opt(&o,tags::TRANSFER_SYNTAX_UID).transpose()
						.unwrap_or(self.assoc.context.select_ts(request.pc_id).map(|s|s.uid().to_string()))
						.map_err(|e|{
							error!("{e}");
							failure(FailureCode::CannotUnderstand).comment(e)
						})?;
					match io::make_file(o, &ts)
					{
						Ok(f) => self.backend.store_file(f).await,
						Err(e) => Err(e)
					}
				} else { Err(failure(FailureCode::InvalidArgument)) }
			}
			// Query/Retrieve Service Class
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/chapter_C.html
			RequestCode::C_GET => {
				if let Some(o) = request.take_attachment_obj(){
					let instances = self.backend.lookup(o).await?;
					let mut pend = pending(instances.size_hint().1.map(|e|e as u16));
					pin!(instances);

					while let Some(instance) = instances.next().await {
						match self.c_store(&instance, []).await
						{
							Ok(s) => {
								match s {
									StatusOk::Success(_) => {pend.succeed();},
									StatusOk::Warning (_) => {
										pend.warn();
									}
									_ => unreachable!()
								}
							},
							Err(_) => {
								// @todo handle failed stores in final response
								pend.fail();
							}
						}
						let resp = request.make_response(pend.clone());
						self.assoc.send_payload(resp.try_into()?).await?;
					}
					success().into()
				} else { Err(failure(FailureCode::InvalidArgument)) }
			}
			RequestCode::C_FIND => {
				if let Some(ident) = request.take_attachment_obj().map(Identifier::from){
					let instances =  self.backend.find(ident.clone()).await?;
					let mut pend = pending(instances.size_hint().1.map(|l|l as u16));

					pin!(instances);
					while let Some(mut ob) = instances.try_next().await?
					{
						pend.succeed();
						// @todo spec says we should have things we don't have as empty elements
						let resp = request
							.make_response(pend.clone())
							.with_identifier(ident.extract(&mut ob).0);
						self.assoc.send_payload(resp.try_into()?).await?;
					}
					success().into()
				} else { Err(failure(FailureCode::InvalidArgument)) }
			}
			RequestCode::C_MOVE => {todo!()}
			RequestCode::C_ECHO => Ok(success().into()),
			_ => {todo!()}
		}
	}

	async fn c_store(&mut self, instance:&B::Item, attr:impl IntoIterator<Item=InMemElement>) -> Status where
	{
		Self::c_store_to::<TaskPipe>(&self.backend, &mut self.assoc, instance, attr).await
	}
	async fn c_store_to<T>(backend:&B, assoc:&mut Association<T>, instance:&B::Item, attr:impl IntoIterator<Item=InMemElement>)
		-> Status where Association<T>:SendPayload + ReceiveMessage
	{
		let file= backend.get_path(instance).await
			.map_err(|e|failure(FailureCode::InvalidSOPInstance).comment(e))?;

		match assoc.store_file(&file,attr).await
		{
			Ok(s) => Ok(s),
			Err(UnsupportedPC { uid, name }) => {
				debug!("Store failed because of unsupported storage class {name}");
				Err(failure(FailureCode::SOPClassDoesNotMatch)
					.offending([tags::MEDIA_STORAGE_SOP_CLASS_UID])
					.comment(format!("media storage SOP Class UID of the file was: {name} ({uid})"))
				)
			},
			Err(e) => {
				debug!("Store for C-GET failed ({e})");
				Err(failure(FailureCode::Failure).comment(e))
			}
		}
	}

	pub fn spawn(
		local:FullAeAddr<SocketAddr>,
		address:FullAeAddr<SocketAddr>,
		peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>,
		to_dispatch:Sender<Payload>,
		context: Context,
		backend:B,
		mut request: Request,
		set:&mut JoinSet<(u16,crate::error::Result<()>)>
	) -> TaskHandle where B:'static, <B as FileAccess>::Item: Sync
	{
		let (to_task, from_dispatch) = channel(32);
		let msg_register = Arc::<RwLock<HashMap<u16, RequestCode>>>::default();
		let msg_register2 = msg_register.clone();
		let peers:Vec<_> = peers.into_iter().collect();
		let handle = set.spawn(async move {
			let mut task = Task::new(local, address, peers, to_dispatch, from_dispatch, context, backend, msg_register2);
			let result = task.process(&mut request).await;
			let result = match request.make_response(result).try_into(){
				Ok(p) => task.send_payload(p).await,
				Err(e) => Err(e)
			};
			(request.id,result)
		});
		TaskHandle{ data_sink: to_task, handle, msg_register }
	}
}

impl TaskHandle {
	pub fn is_relevant(self: &Self, id:u16) -> bool
	{
		self.msg_register.read().ok().is_some_and(|o|o.get(&id).is_some())
	}
}

impl SendPayload for Association<TaskPipe> {
	async fn send_request(&mut self, request: Request) -> crate::error::Result<()> {
		self.inner.msg_register.write().unwrap().insert(request.id, request.command.clone());
		self.send_payload(request.try_into()?).await

	}
	async fn send_payload(&mut self, payload: Payload) -> crate::error::Result<()> {
		self.inner.sink.send(payload).await.map_err(|e|e.into())
	}
}

impl ReceiveMessage for Association<TaskPipe> {
	async fn receive_message(&mut self) -> crate::error::Result<MessageType> {
		self.inner.source.recv().await.map(|m|MessageType::Response(m))
			.ok_or(ConnectionClosed.into())
	}
}
