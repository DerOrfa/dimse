use std::pin::Pin;
use std::task::{ready, Context, Poll};
use dicom::object::InMemDicomObject;
use crate::association::{Association, ReceiveMessage};
use crate::error::{DimseError, Result};
use futures::{FutureExt, Stream};
use futures::stream::FusedStream;
use snafu::ResultExt;
use tracing::{error, trace, warn};
use crate::definitions::{RequestCode, StatusOk};
use crate::error::DimseError::UnexpectedMessage;
use crate::msg::{MessageType, Response};
use crate::payload::Attachment;

#[derive(Default)]
enum PendingState<'a,A>
{
	#[default]
	None,
	Free(&'a mut Association<A>),
	Pending(Pin<Box<dyn Future<Output=Result<MessageType>>+'a>>)
}
pub struct PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin {
	request:RequestCode,
	pub succeeded:Option<u16>,
	pub warn:Option<u16>,
	pub failed:Option<u16>,
	pub to_do:Option<u16>,
	terminated:bool,
	state:PendingState<'a,A>
}

impl<'a,A> PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin
{
	pub fn new(ass: &'a mut Association<A>, request: RequestCode) -> Self
	{
		Self{request, succeeded: None, warn: None, failed: None, to_do: None, terminated: false, state:PendingState::Free(ass)}
	}
	fn poll_message(&mut self, cx: &mut Context<'_>) -> Poll<Result<MessageType>> {
		let mut p = match std::mem::take(&mut self.state) {
			PendingState::Free(ass) => {
				Box::pin(ass.receive_message())
			}
			PendingState::Pending(p) => p,
			_ => {panic!("message pending in invalid state")}
		};
		let poll = p.poll_unpin(cx);
		self.state = PendingState::Pending(p);
		poll
	}
	fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<Result<Response>> {
		match ready!(self.poll_message(cx)) {
			Ok(MessageType::Response(r)) if r.command==self.request => Poll::Ready(Ok(r)),
			Ok(r) => Err(UnexpectedMessage { expected: format!("response to {}",self.request), actual: r })?,
			Err(e) => Err(e)?,
		}
	}
}

impl<'a,A> Stream for PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin + 'a
{
	type Item = Result<InMemDicomObject>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>
	{
		let this = self.get_mut();
		match ready!(this.poll_response(cx)) {
			Ok(mut r) => {
				let a = match r.status.clone() {
					Ok(StatusOk::Success) => {
						trace!("Got end signal, closing stream..");
						this.terminated=true;return Poll::Ready(None)
					},
					Ok(StatusOk::Pending{ succeeded, warn, fail, to_do }) => {
						this.succeeded = succeeded;
						this.warn = warn;
						this.failed = fail;
						this.to_do = to_do;
						r.take_attachment()
					},
					Ok(StatusOk::Warning{ code, offending_element:_, comment }) => {
						match comment {
							None => warn!("received warning code {code}"),
							Some(c) => warn!("received warning {code} ({c})"),
						}
						r.take_attachment()
					}
					Err(e) => {
						this.terminated = true;
						return Err(e).with_whatever_context::<_, _, DimseError>(|e|
							format!("Failure reading find results: {e}"))?
					}
				};
				match a
				{
					Attachment::Obj(o) => Poll::Ready(Some(Ok(o))),
					_ => Poll::Ready(Some(Ok(InMemDicomObject::new_empty()))),
				}
			}
			Err(e) => {
				this.terminated = true;
				error!("{}", e);
				Poll::Ready(Some(Err(e)))
			}
		}
	}
	fn size_hint(&self) -> (usize, Option<usize>){
		(0, self.to_do.map(|d| d as usize))
	}
}

impl<A> FusedStream for PendingReceive<'_, A>  where Association<A>:ReceiveMessage + Unpin
{
	fn is_terminated(&self) -> bool{self.terminated}
}

impl<'a,A> Drop for PendingReceive<'_, A>  where Association<A>:ReceiveMessage + Unpin
{
	fn drop(&mut self) {
		if !self.terminated { // clear the pipe
			error!("dropping pending receive, there is probably some data left in the pipeline");
		}
	}
}