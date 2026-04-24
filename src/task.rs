use std::collections::{HashMap, HashSet};
use crate::association::{Association,ReceiveMessage, SendPayload};
use crate::context::Context;
use crate::definitions::{FailureCode, RequestCode, Status, StatusFailure, StatusOk, StatusWarning};
use crate::msg::{MessageType, Request, Response};
use crate::payload::{Attachment, Payload};
use crate::{io, Taker};
use dicom_dictionary_std::tags;
use dicom_ul::{FullAeAddr};
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock};
use dicom_ul::association::Error::ConnectionClosed;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinSet;
use tracing::{debug, error};
use crate::error::DimseError::UnsupportedPC;
use crate::io::FileAccess;

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
		peer:FullAeAddr<SocketAddr>,
		local:FullAeAddr<SocketAddr>,
		to_dispatch_sink:Sender<Payload>,
		from_dispatch_source:Receiver<Response>,
		context:Context,
		backend:B,
		msg_register:Arc<RwLock<HashMap<u16,RequestCode>>>,
	) -> Self where B:FileAccess
	{
		let pipe = TaskPipe{source: from_dispatch_source, msg_register, sink:to_dispatch_sink};
		let assoc = Association::new(peer, local, pipe, context);
		Task{ assoc, backend}
	}
	pub async fn process(&mut self, request: &mut Request) -> Status
	{
		match &request.deref().command
		{
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html
			// https://dicom.nema.org/dicom/2013/output/chtml/part07/sect_9.3.html
			RequestCode::C_STORE => {
				debug!("Got STORE request");
				if let Attachment::Obj(o) = request.take_attachment(){
					let ts = Taker::get_opt(&o,tags::TRANSFER_SYNTAX_UID).transpose()
						.unwrap_or(self.assoc.context.select_ts(request.pc_id).map(|s|s.uid().to_string()))
						.map_err(|e|{
							error!("{e}");
							StatusFailure::new(FailureCode::CannotUnderstand, Some(e.to_string()))
						})?;
					match io::make_file(o, &ts)
					{
						Ok(f) => self.backend.store_file(f).await,
						Err(e) => Err(e)
					}
				} else { FailureCode::InvalidArgument.into() }
			}
			// Query/Retrieve Service Class
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/chapter_C.html
			RequestCode::C_GET => {
				// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.3.html
				if let Attachment::Obj(o) = request.take_attachment(){
					let instances = self.backend.lookup(o).await?;
					let mut failed:HashSet<_> = Default::default();
					let mut fail_comment = Option::<String>::None;
					if instances.is_empty() {
						FailureCode::NoSuchSOPInstance.into()
					} else {
						let (mut succeeded,mut warn,mut fail,mut to_do) = (0,0,0,instances.len() as u16);
						for instance in instances {
							let resp = self.c_store(&instance).await;
							match &resp{
								Ok(s) => {
									match s {
										StatusOk::Success => {succeeded += 1;},
										StatusOk::Warning { offending_element, comment, .. } => {
											warn += 1;
											for e in offending_element.iter().flatten(){failed.insert(*e);	}
											fail_comment = fail_comment.xor(comment.clone());
										}
										_ => {panic!("Unexpected store result");}
									}
								},
								Err(e) => {
									fail_comment = fail_comment.xor(Some(e.to_string()));
									fail += 1;
								}
							}
							to_do -= 1;
							let resp = request.make_response(StatusOk::Pending {
								succeeded: Some(succeeded),
								warn: Some(warn),
								fail: Some(fail),
								to_do: Some(to_do),
							});
							self.assoc.send_payload(resp.try_into()?).await?;
						}
						if failed.is_empty()
						{
							StatusOk::Success.into()
						} else { // todo more details
							StatusOk::Warning {
								code: StatusWarning::AttributeListWarning,
								offending_element: if failed.is_empty(){None} else {Some(failed.into_iter().collect())},
								comment: fail_comment,
							}.into()
						}
					}
				} else { FailureCode::InvalidArgument.into() }
			}
			RequestCode::C_FIND => {todo!()}
			RequestCode::C_MOVE => {todo!()}
			RequestCode::C_ECHO => StatusOk::Success.into(),
			_ => {todo!()}
		}
	}

	async fn c_store(&mut self, instance: &B::Item) -> Status
	{
		let file = self.backend.get_path(&instance).await?;
		match self.assoc.store_file(&file).await
		{
			Ok(s) => Ok(s),
			Err(UnsupportedPC { uid, name }) => {
				debug!("Store for C-GET failed because of unsupported storage class {name}");
				Err(StatusFailure {
					code: FailureCode::SOPClassDoesNotMatch,
					offending_element: Some(vec![tags::MEDIA_STORAGE_SOP_CLASS_UID]),
					comment: Some(format!("media storage SOP Class UID of the file was: {name} ({uid})")),
				})
			},
			Err(e) => {
				debug!("Store for C-GET failed ({e})");
				Err(StatusFailure::new(FailureCode::Failure, Some(e.to_string())))
			}
		}
	}

	pub fn spawn(
		address:FullAeAddr<SocketAddr>,
		local:FullAeAddr<SocketAddr>,
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
		let handle = set.spawn(async move {
			let mut task = Task::new(address, local, to_dispatch, from_dispatch, context, backend, msg_register2);
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
