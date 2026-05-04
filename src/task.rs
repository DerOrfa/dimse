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
use std::path::Path;
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
use crate::client::Client;
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
			RequestCode::C_MOVE => {
				let mut ok_response:StatusOk=success().into();
				if let Some(ident) = request.take_attachment_obj().map(Identifier::from){
					let mut instances = self.backend.lookup(ident).await?;
					let mut pend = pending(instances.size_hint().1.map(|l|l as u16));
					let destination:String = Taker::take(&mut request.obj,tags::MOVE_DESTINATION)
						.map_err(|_|failure(FailureCode::MissingAttribute).offending([tags::MOVE_DESTINATION]))?;
					let destination= AeAddr::new(&destination,
						self.assoc.peers.get(&destination).cloned()
							.ok_or(failure(FailureCode::MoveDestinationUnknown))?
					);
					let local_ae = self.assoc.local.ae_title().to_string();
					debug!("Contacting {destination} for C-MOVE");
					if self.assoc.local_address().socket_addr() == destination.socket_addr(){
						panic!("Wont talk to myself");
					}
					let destination_clone = destination.clone();
					let mut client = Client::new(destination_clone.clone(), Some(local_ae), []).await
							.map_err(|e|failure(FailureCode::Failure)
								.comment(format!("Failed to contact {destination_clone} ({e})")))?;
					while let Some(instance) = instances.next().await {
						debug!("Sending {} to {destination}",self.backend.get_uid(&instance).await?);
						let attr= [
							InMemElement::new(tags::MOVE_ORIGINATOR_APPLICATION_ENTITY_TITLE, VR::AE,
								destination.ae_title().unwrap().to_string()),
							InMemElement::new(tags::MOVE_ORIGINATOR_MESSAGE_ID, VR::US, dicom_value!(request.id))
						];
						let file= self.backend.get_path(&instance).await
							.map_err(|e|failure(FailureCode::InvalidSOPInstance).comment(e))?;

						match Self::c_store_file_to(file.as_path(), &mut client, attr).await{
							Ok(StatusOk::Success(_)) => pend.succeed(),
							Ok(StatusOk::Warning(_)) => {
								pend.warn()
							},
							Err(e) => {
								ok_response.warn(StatusWarning::default()
									.comment(e)
									.missing([self.backend.get_uid(&instance).await?])
								);
								pend.fail()
							},
							_ => unreachable!() // should not happen
						};

						let resp = request.make_response(pend.clone());
						self.assoc.send_payload(resp.try_into()?).await?;
					}
					Ok(ok_response)
				} else { Err(failure(FailureCode::InvalidArgument)) }
			}
			RequestCode::C_ECHO => Ok(success().into()),
			_ => {todo!()}
		}
	}

	async fn c_store(&mut self, instance:&B::Item, attr:impl IntoIterator<Item=InMemElement>) -> Status where
	{
		let file= self.backend.get_path(instance).await
			.map_err(|e|failure(FailureCode::InvalidSOPInstance).comment(e))?;

		Self::c_store_file_to::<TaskPipe>(file.as_path(), &mut self.assoc, attr).await
	}
	async fn c_store_file_to<T>(file:&Path, assoc:&mut Association<T>, attr:impl IntoIterator<Item=InMemElement>)
		-> Status where Association<T>:SendPayload + ReceiveMessage
	{
		match assoc.store_file(file,attr).await
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
