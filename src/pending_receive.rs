use std::pin::Pin;
use std::task::{Context, Poll};
use dicom::object::InMemDicomObject;
use crate::association::{Association, ReceiveMessage};
use crate::error::{DimseError, Result};
use futures::Stream;
use futures::stream::FusedStream;
use snafu::ResultExt;
use tracing::{error, trace, warn};
use crate::definitions::{RequestCode, StatusOk};
use crate::payload::Attachment;

pub struct PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin {
	ass:&'a mut Association<A>,
	request:RequestCode,
	pub succeeded:Option<u16>,
	pub warn:Option<u16>,
	pub failed:Option<u16>,
	pub to_do:Option<u16>,
	terminated:bool,
}

impl<'a,A> PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin
{
	pub fn new(ass: &'a mut Association<A>, request: RequestCode) -> Self
	{
		Self{ass, request, succeeded: None, warn: None, failed: None, to_do: None, terminated: false}
	}
}

impl<A> Stream for PendingReceive<'_,A> where Association<A>:ReceiveMessage + Unpin
{
	type Item = Result<InMemDicomObject>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>
	{
		let this = self.get_mut();
		let polled = std::pin::pin!(this.ass.receive_response(this.request.clone())).poll(cx);
		match polled {
			Poll::Ready(Ok(r)) => {
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
						r.into_attachment()
					},
					Ok(StatusOk::Warning{ code, offending_element:_, comment }) => {
						match comment {
							None => warn!("received warning from {}: {code}", this.ass.peer),
							Some(c) => warn!("received warning from {}: {code} ({c})", this.ass.peer),
						}
						r.into_attachment()
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
			Poll::Ready(Err(e)) => {
				this.terminated = true;
				error!("{}", e);
				Poll::Ready(Some(Err(e)))
			}
			Poll::Pending => Poll::Pending,
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