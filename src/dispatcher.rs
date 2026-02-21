use std::collections::HashMap;
use std::default::Default;
use std::future::poll_fn;
use dicom_ul::{Pdu};
use dicom_ul::pdu::{PDataValue, PDataValueType};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::{JoinSet};
use tracing::{debug, error, info, trace, warn};
use crate::association::{Association, SendPayload};
use crate::definitions::Status;
use crate::error::DimseError::UnexpectedResponse;
use crate::io::FileAccess;
use crate::msg::{Message, MessageType};
use crate::payload::{Payload};
use crate::server::Server;
use crate::task::{Task, TaskHandle};
use super::error::{DimseError, Result};

enum OpenState{
	Open(tokio::sync::watch::Sender<()>),
	Closing,
	Closed
}
pub struct Dispatcher<B>  where B:FileAccess + Clone + Send + 'static
{
	ass:Association<Server>,
	payload_terminal:Receiver<Payload>,
	payload_sink: Sender<Payload>,
	state:OpenState,
	tasks:HashMap<u16,TaskHandle>,
	join_set:JoinSet<Status>,
	file_access:B
}

impl<B> Dispatcher<B> where B:FileAccess + Clone + Send
{
	pub async fn new(ae_title:impl AsRef<str>, scu_stream: TcpStream, watcher:tokio::sync::watch::Sender<()>, file_access:B)  -> Result<Self>
	{
		let ass = Server::new(ae_title.as_ref(),scu_stream).await?;
		let (payload_sink,payload_terminal) = channel(32);
		Ok(Self{
			ass, file_access,
			payload_terminal, payload_sink,
			state: OpenState::Open(watcher),
			tasks: Default::default(),
			join_set: Default::default(),
		})
	}

	async fn check_tasks(set: &mut JoinSet<Status>) {
		match poll_fn(move |cx|set.poll_join_next(cx)).await
		{
			None => (),
			Some(Ok(Ok(e))) => trace!("Task returned success {}", e.as_ref()),
			Some(Ok(Err(f))) => trace!("Task failed with {f}"),
			Some(Err(e)) => trace!("{e}"),
		}
	}
	async fn next(&mut self, sink: &Sender<Payload>) -> Result<()>{
		let pdu = match &self.state {
			OpenState::Open(watcher) => { // normal state receive data while watching for command to shut down
				tokio::select! {
					r = self.ass.read_pdu() => Some(r?),
					_ = watcher.closed() => {
						debug!("Asking {} to release connection", self.ass.peer_address());
						self.ass.send_pdu(&Pdu::ReleaseRQ).await?;
						self.state = OpenState::Closing;
						None
					},
					_ = Self::check_tasks(&mut self.join_set), if !self.join_set.is_empty() => None,
				}
			},
			OpenState::Closing => { // see if there is some stuff left
				match self.ass.read_pdu().await{
					Err(DimseError::AssReadError { source }) => {
						match source {
							dicom_ul::association::Error::ConnectionClosed => {
								self.state = OpenState::Closed;
								None
							}
							e => Err(e)?,
						}
					},
					p => Some(p?),
				}
			}// closing, but still accepting input
			OpenState::Closed => {
				return Ok(())
			}, // actually should never happen, as outer tool terminates on "Closed"
		};
		if let Some(pdu) = pdu {
			trace!("next pdu => {:?}", pdu);
			self.next_pdu(sink, pdu).await?;
		}
		Ok(())
	}
	async fn next_pdu(&mut self, sink: &Sender<Payload>, pdu:Pdu) -> Result<()>
	{
		match pdu{
			Pdu::PData { data } => self.dispatch(sink,data).await?,
			Pdu::ReleaseRQ => {
				debug!("{} asked to release the connection, closing ..", self.ass.peer_address());
				self.ass.send_pdu(&Pdu::ReleaseRP).await?;
				self.state = OpenState::Closed;
			}
			Pdu::ReleaseRP => {
				self.state=OpenState::Closed;
				debug!("{} confirmed release. Bye...", self.ass.peer_address());
			},
			Pdu::AbortRQ { source } => {
				warn!("Aborted connection from: {:?}", source);
				self.state=OpenState::Closed;
			}
			Pdu::AssociationRQ(_)|Pdu::AssociationAC(_)|Pdu::AssociationRJ(_)|Pdu::Unknown { .. } =>
				warn!("Ignoring unexpected {} from {}",pdu.short_description(), self.ass.peer_address()),
		};
		Ok(())
	}

	async fn dispatch(&mut self, to_dispatch: &Sender<Payload>, data:Vec<PDataValue>) -> Result<()>
	{
		if let Some(first) = data.first()
		{
			match first.value_type {
				PDataValueType::Command => {
					match Message::read_from_data(data, &self.ass.context)?
					{
						// new request, start new task
						MessageType::Request(mut req) => {
							req.read_attachment(&mut self.ass).await?;
							debug!("Starting task to handle request {req}");
							let req_id = req.id;
							let task = Task::spawn(
								self.ass.peer.clone(), self.ass.local.clone(),
								to_dispatch.clone(),
								self.ass.context.clone(), self.file_access.clone(),
								req,
								&mut self.join_set
							);
							self.tasks.insert(req_id,task);
						}
						// response, find the task, and notify it
						MessageType::Response(resp) => {
							let (id,task) = self.tasks.iter()
								.find(|(_,t)|t.is_relevant(resp.id))
								.ok_or(UnexpectedResponse {name:resp.to_string()})?;
							trace!("Forwarding {resp} to task {id}");
							task.data_sink.send(resp).await?;
						}
					}
				},
				PDataValueType::Data => {
					Err(DimseError::UnexpectedMessageType {expected:"Message".into(), actual: "Data".into() })?
				}
			}
		}
		else
		{
			error!("Unexpectedly got empty pdu");
		}
		Ok(())
	}
	pub(crate) async fn run(mut self) -> Result<()>
	{
		let (sink, mut from_tasks) = tokio::sync::mpsc::channel::<Payload>(32);
		loop {
			tokio::select! {
					n = self.next(&sink) => {n?},
					Some(payload) = from_tasks.recv() => {
						self.ass.send_payload(payload).await?;
					}
				}
			if let OpenState::Closed = self.state {break}
		}
		info!("association {} => {} is closed", self.ass.peer_address(), self.ass.local_address());
		Ok(())
	}

}
