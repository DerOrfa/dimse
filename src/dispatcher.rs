use std::collections::HashMap;
use std::default::Default;
use std::future::poll_fn;
use std::net::SocketAddr;
use dicom_ul::{FullAeAddr, Pdu};
use dicom_ul::pdu::{PDataValue, PDataValueType};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::{JoinSet};
use tracing::{debug, error, info, trace, warn};
use crate::association::{Release, SendPayload};
use crate::error::DimseError::UnexpectedResponse;
use crate::io::FileAccess;
use crate::lazy_attachment_to_string;
use crate::msg::{Message, MessageType};
use crate::payload::Payload;
use crate::server::{Server, ServerAssoc};
use crate::task::{Task, TaskHandle};
use super::error::{DimseError, Result};

enum OpenState{
	Open(tokio::sync::watch::Sender<()>),
	Closing,
	Closed
}
pub(crate) struct Dispatcher<B>  where B:FileAccess + 'static
{
	ass:ServerAssoc,
	state:OpenState,
	tasks:HashMap<u16,TaskHandle>,
	join_set:JoinSet<(u16, Result<()>)>,
	file_access:B
}

impl<B> Dispatcher<B> where B:FileAccess
{
	pub(crate) async fn new(ae_title:impl AsRef<str>, scu_stream: TcpStream, peers:impl IntoIterator<Item=FullAeAddr<SocketAddr>>, watcher:tokio::sync::watch::Sender<()>, file_access:B)  -> Result<Self>
	{
		let ass = Server::new(ae_title.as_ref(),scu_stream, peers).await?;
		Ok(Self{
			ass, file_access,
			state: OpenState::Open(watcher),
			tasks: Default::default(),
			join_set: Default::default(),
		})
	}

	async fn check_tasks(set: &mut JoinSet<(u16,Result<()>)>) -> Result<Option<u16>>
	{
		match poll_fn(move |cx|set.poll_join_next(cx)).await.transpose()?
		{
			None => Ok(None),
			Some((id,Ok(_))) => {
				trace!("Task {id} successfully completed");
				Ok(Some(id))
			},
			Some((id,Err(f))) => {
				trace!("Task {id} failed with {f}");
				Err(f)
			},
		}
	}
	async fn next(&mut self, sink: &Sender<Payload>) -> Result<()> where <B as FileAccess>::Item: Sync{
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
					id = Self::check_tasks(&mut self.join_set), if !self.join_set.is_empty() => {
						if let Ok(Some(id)) = id{self.tasks.remove(&id);}
						None
					},
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
				for (id, t) in &self.tasks{
					warn!("Killing stuck task {id}");
					t.handle.abort();
				}
				return Ok(())
			},// actually should never happen, as outer tool terminates on "Closed"
		};
		if let Some(pdu) = pdu {
			trace!("next pdu => {:?}", pdu);
			self.next_pdu(sink, pdu).await?;
		}
		Ok(())
	}
	async fn next_pdu(&mut self, sink: &Sender<Payload>, pdu:Pdu) -> Result<()> where <B as FileAccess>::Item: Sync
	{
		match pdu{
			Pdu::PData { data } => self.dispatch(sink,data).await?,
			Pdu::ReleaseRQ => {
				debug!("{} asked to release the connection, closing ..", self.ass.peer_address());
				self.state = OpenState::Closed;
				self.ass.send_pdu(&Pdu::ReleaseRP).await?;
			}
			Pdu::ReleaseRP => {
				self.state=OpenState::Closed;
				debug!("{} confirmed release. Bye...", self.ass.peer_address());
			},
			Pdu::AbortRQ { source } => {
				self.state=OpenState::Closed;
				warn!("Aborted connection from: {:?}", source);
			}
			Pdu::AssociationRQ(_)|Pdu::AssociationAC(_)|Pdu::AssociationRJ(_)|Pdu::Unknown { .. } =>
				warn!("Ignoring unexpected {} from {}",pdu.short_description(), self.ass.peer_address()),
		};
		Ok(())
	}

	async fn dispatch(&mut self, to_dispatch: &Sender<Payload>, data:Vec<PDataValue>)
		-> Result<()> where <B as FileAccess>::Item: Sync
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
							debug!("Starting task to handle {req}");
							lazy_attachment_to_string(req.attachment()).map(|s|trace!("{s}"));
							let req_id = req.id;
							let task = Task::spawn(
								self.ass.local.clone(),
								self.ass.peer.clone(),
								self.ass.peers.iter().map(|(ae,socket)|FullAeAddr::new(ae,socket.clone())),
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
							debug!("Forwarding {resp} to task {id}");
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
	pub(crate) async fn run(mut self) -> Result<()> where <B as FileAccess>::Item: Sync
	{
		let (sink, mut from_tasks) = channel::<Payload>(32);
		loop {
			tokio::select! {
				n = self.next(&sink) => {n?},
				Some(payload) = from_tasks.recv() => {
					self.ass.send_payload(payload).await?;
				}
			}
			if let OpenState::Closed = self.state {
				break
			}
		}
		info!("association {} => {} is closed", self.ass.peer_address(), self.ass.local_address());
		Ok(())
	}

}
