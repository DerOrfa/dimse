use std::pin::Pin;
use std::task::{ready, Context, Poll};
use dicom::object::{FileDicomObject, InMemDicomObject};
use dicom_dictionary_std::tags;
use crate::association::{Association, ReceiveMessage, SendPayload};
use crate::error::{DimseError, Result};
use futures::{FutureExt, Stream};
use futures::stream::FusedStream;
use snafu::ResultExt;
use tracing::{debug, error, trace, warn};
use crate::definitions::{RequestCode, StatusOk};
use crate::error::DimseError::UnexpectedMessage;
use crate::msg::{MessageType, Response};
use crate::payload::{Attachment, Payload};
use crate::{io, Taker};

#[derive(Default)]
enum PendingState<'a,A>
{
	#[default]
	None,
	Free(&'a mut Association<A>),
	PendingReceive(Pin<Box<dyn Future<Output=(Result<MessageType>, &'a mut Association<A>)>+'a>>),
	PendingSend(Pin<Box<dyn Future<Output=(Result<()>, &'a mut Association<A>)>+'a>>)
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

pub struct PendingStore<'a,A> where Association<A>:ReceiveMessage + Unpin
{
	inner:PendingReceive<'a,A>,
	store:Option<FileDicomObject<InMemDicomObject>>
}

impl<'a, A> PendingStore<'a, A> where Association<A>: ReceiveMessage + Unpin
{
	pub fn new(ass: &'a mut Association<A>, request_code: RequestCode) -> Self {
		let inner = PendingReceive::new(ass,request_code);
		Self{inner,	store: None}
	}
}

impl<'a,A> PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin
{
	pub fn new(ass: &'a mut Association<A>, request: RequestCode) -> Self
	{
		Self{request, succeeded: None, warn: None, failed: None, to_do: None, terminated: false, state:PendingState::Free(ass)}
	}
	fn poll_message(&mut self, cx: &mut Context<'_>) -> Poll<Result<MessageType>> {
		// take out state to make a poll or repoll
		let mut p = match std::mem::take(&mut self.state) {
			PendingState::Free(ass) => {
				Box::pin(async { // take association and wrap it in async block receiving the message
					let msg = ass.receive_message().await;
					(msg,ass)
				})
			}
			PendingState::PendingReceive(p) => p,
			_ => {panic!("message pending in invalid state")}
		};
		// (re)poll and put it back
		match p.poll_unpin(cx){
			Poll::Pending => {
				self.state = PendingState::PendingReceive(p);
				Poll::Pending
			},
			Poll::Ready((r,a)) => { // we got our result, go back to Free
				self.state = PendingState::Free(a);
				Poll::Ready(r)
			}
		}
	}
	fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<Result<Response>> {
		match ready!(self.poll_message(cx)?) {
			MessageType::Response(r) if r.command==self.request => Poll::Ready(Ok(r)),
			r => Err(UnexpectedMessage { expected: format!("response to {}",self.request), actual: r })?,
		}
	}
	fn get_association(&self) -> &Association<A>{
		match self.state {
			PendingState::Free(ref a) => a,
			_ => panic!("invalid state"),
		}
	}
	fn process_response(&mut self, mut response: Response) -> Result<Option<InMemDicomObject>>
	{
		let peer = &self.get_association().peer;
		let a = match response.status.clone() {
			Ok(StatusOk::Success) => {
				trace!("Got end signal, closing stream..");
				self.terminated=true;return Ok(None)
			},
			Ok(StatusOk::Pending{ succeeded, warn, fail, to_do }) => {
				self.succeeded = succeeded;
				self.warn = warn;
				self.failed = fail;
				self.to_do = to_do;
				response.take_attachment()
			},
			Ok(StatusOk::Warning{ code, offending_element:_, comment }) => {
				match comment {
					None => warn!("received warning from {peer}: {code}"),
					Some(c) => warn!("received warning from {peer}: {code} ({c})"),
				}
				response.take_attachment()
			}
			Err(e) => {
				self.terminated = true;
				return Err(e).with_whatever_context::<_, _, DimseError>(|e|
					format!("Failure reading response for {}: {e}", self.request))
			}
		};
		Ok(Some(match a
		{
			Attachment::Obj(o) => o,
			_ => InMemDicomObject::new_empty(),
		}))
	}

}

impl<'a,A> Stream for PendingReceive<'a,A> where Association<A>:ReceiveMessage + Unpin + 'a
{
	type Item = Result<InMemDicomObject>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>
	{
		let this = self.get_mut();
		let ready = ready!(this.poll_response(cx))
			.and_then(|r| this.process_response(r)).transpose();
		Poll::Ready(ready)
	}
	fn size_hint(&self) -> (usize, Option<usize>){
		(0, self.to_do.map(|d| d as usize))
	}
}

impl<'a,A> PendingStore<'a,A> where Association<A>:ReceiveMessage + SendPayload + Unpin + 'a {
	fn poll_send(&mut self, payload: Option<Payload>, cx: &mut Context<'_>) -> Poll<Result<()>> {
		// take out state to make a poll or repoll
		let mut p = match std::mem::take(&mut self.inner.state) {
			PendingState::Free(ass) => {
				Box::pin(async { // take association and wrap it in async block receiving the message
					let msg = ass.send_payload(payload.expect("initialized send without payload")).await;
					(msg,ass)
				})
			}
			PendingState::PendingSend(p) => p,
			_ => {panic!("message pending in invalid state")}
		};
		// (re)poll and put it back
		match p.poll_unpin(cx){
			Poll::Pending => {
				self.inner.state = PendingState::PendingSend(p);
				Poll::Pending
			},
			Poll::Ready((r,a)) => { // we got our result, go back to Free
				self.inner.state = PendingState::Free(a);
				Poll::Ready(r)
			}
		}
	}

}

impl<'a, A> Stream for PendingStore<'a, A> where Association<A>:ReceiveMessage + SendPayload + Unpin + 'a {
	type Item = Result<FileDicomObject<InMemDicomObject>>;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>
	{
		let this = self.get_mut();
		loop {
			// try sending pending store confirmations
			if let PendingState::PendingSend(_) = this.inner.state{
				ready!(this.poll_send(None,cx))?;
			}
			// check, if we already have a stored object
			if let Some(o) = this.store.take(){
				// we do, return this as ready
				return Poll::Ready(Some(Ok(o)))
			}
			// we should get a C-STORE request and/or a status response
			match ready!(this.inner.poll_message(cx))?
			{
				MessageType::Request(mut req) if req.command == RequestCode::C_STORE => {
					debug!("StoreStream got {req}");
					// ready above bails if we're pending, so this should be safe
					let context = &this.inner.get_association().context;
					if let Attachment::Obj(o) = std::mem::take(&mut req.attachment)
					{
						let ts: Option<String> = Taker::get_opt(&o, tags::TRANSFER_SYNTAX_UID)?;
						let ts = ts.as_deref().unwrap_or(context.select_ts(req.pc_id)?.uid());
						assert!(this.store.is_none());
						// store the file object and let peer know about the result
						let response = io::make_file(o, ts).map(|o|{
							this.store.replace(o);
							StatusOk::Success
						});
						let response = req.make_response(response);
						return this.poll_send(Some(response.try_into()?),cx)?
							.map(|_|Ok(this.store.take()).transpose()) // if confirmation already done send Ready right away
					} else { panic!("invalid attachment type") }
				}
				// we got a status response let the underlying stream handle it and actually return pending unless we're done
				MessageType::Response(res)  => {
					// error bails here or above, so this is always a non-error case and all processing is done by inner
					debug!("StoreStream got {}", res);
					this.inner.process_response(res)?;
					if this.inner.terminated{return Poll::Ready(None)}
				}
				r => Err(UnexpectedMessage { expected: format!("response to {}",this.inner.request), actual: r })?,
			}
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
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

