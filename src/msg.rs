use std::fmt::{Debug, Display, Formatter};
use std::io::Cursor;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use crate::association::{Association, ReceivePdu};
use crate::definitions::{get_status, match_status, RequestCode, Status, StatusFailure, StatusOk};
use crate::error::DimseError;
use crate::payload::{Attachment, Payload};
use crate::{io, lazy_dump_to_string, Taker};
use dicom::core::{DataElement, PrimitiveValue, VR};
use dicom::core::smallvec::SmallVec;
use dicom::dicom_value;
use dicom::object::{open_file, InMemDicomObject};
use dicom::object::mem::InMemElement;
use dicom::transfer_syntax::entries::IMPLICIT_VR_LITTLE_ENDIAN;
use dicom_dictionary_std::tags;
use dicom_ul::pdu::{PDataValue, PDataValueType, PresentationContextNegotiated};
use snafu::ResultExt;
use tracing::debug;
use crate::context::Context;
use crate::error::DimseError::{IncompleteMsg, UnexpectedEmptyMsg};

/// Unified message class
/// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-1
/// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-2
/// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-3
/// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html#table_9.1-4
/// https://dicom.nema.org/dicom/2013/output/chtml/part07/chapter_E.html
#[derive(Debug, Clone)]
pub struct Message
{
	/// COMMAND_FIELD
	pub(crate) command: RequestCode,
	/// presentation context id
	pub pc_id: u8,
	/// MESSAGE_ID_BEING_RESPONDED_TO /	MESSAGE_ID
	pub id:u16,
	pub obj: InMemDicomObject,
	pub attachment: Attachment,
}

#[derive(Clone,Debug)]
pub struct Request
{
	/// AFFECTED_SOP_INSTANCE_UID
	sop_class:String,
	/// PRIORITY
	pub priority:u16,
	inner: Message,
}

#[derive(Clone,Debug)]
pub struct Response
{
	inner: Message,
	pub status: Status,
}

#[derive(Debug)]
pub enum MessageType
{
	Request(Request),
	Response(Response),
}

impl Deref for Request
{
	type Target = Message;
	fn deref(&self) -> &Self::Target{&self.inner}
}
impl DerefMut for Request
{
	fn deref_mut(&mut self) -> &mut Self::Target { &mut self.inner }
}
impl Deref for Response
{
	type Target = Message;
	fn deref(&self) -> &Self::Target{&self.inner}
}
impl DerefMut for Response
{
	fn deref_mut(&mut self) -> &mut Self::Target { &mut self.inner }
}

impl Display for Message{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		f.write_fmt(format_args!("{} ID:{}",self.command.to_string(),self.id))
	}
}
impl Display for Response
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		let s= match &self.status {
			Ok(o) => o.as_ref().to_string(),
			Err(e) => e.to_string(),
		};
		f.write_fmt(format_args!(r#"response to {} with status "{}""#,self.inner,s))
	}
}
impl Display for Request
{
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		Display::fmt(&self.inner, f)
	}
}
impl TryInto<Payload> for Request
{
	type Error = DimseError;

	fn try_into(mut self) -> Result<Payload, Self::Error> {
		let code = dicom_value!(U16, self.inner.command.clone().into());
		self.inner.obj.put(DataElement::new(tags::COMMAND_FIELD, VR::US, code));
		self.inner.obj.put(DataElement::new(tags::MESSAGE_ID, VR::US,dicom_value!(self.inner.id)));
		self.inner.obj.put(DataElement::new(tags::AFFECTED_SOP_CLASS_UID,VR::UI, self.sop_class));
		self.inner.obj.put(DataElement::new(tags::PRIORITY,VR::US, dicom_value!(self.priority)));
		self.inner.into_payload()
	}
}
impl TryInto<Payload> for Response
{
	type Error = DimseError;
	fn try_into(mut self) -> Result<Payload, Self::Error>
	{
		let code:u16 = self.inner.command.clone().into();
		let status_code = get_status(self.status.clone());
		self.inner.obj.put(DataElement::new(tags::COMMAND_FIELD, VR::US, dicom_value!(U16, code | 0x8000)));
		self.inner.obj.put(DataElement::new(tags::MESSAGE_ID_BEING_RESPONDED_TO, VR::US, dicom_value!(self.inner.id)));
		self.inner.obj.put(DataElement::new(tags::STATUS, VR::US, dicom_value!(U16, status_code)));
		// https://dicom.nema.org/dicom/2013/output/chtml/part07/chapter_C.html
		match self.status
		{
			Ok(StatusOk::Success) => {}
			Ok(StatusOk::Warning{ offending_element, comment, .. })
			| Err(StatusFailure{offending_element, comment, ..})
			=> {
				offending_element.map(|e|
				{
					let e = SmallVec::<[_;2]>::from_vec(e);
					self.inner.obj.put(InMemElement::new(tags::OFFENDING_ELEMENT,VR::AT,PrimitiveValue::Tags(e)));
				});
				comment.map(|c|self.inner.obj.put(InMemElement::new(tags::ERROR_COMMENT,VR::AT, c)));
			}
			Ok(StatusOk::Pending { succeeded, warn, fail, to_do }) => {
				let attr = [
					(tags::NUMBER_OF_COMPLETED_SUBOPERATIONS, &succeeded),
					(tags::NUMBER_OF_FAILED_SUBOPERATIONS,    &fail),
					(tags::NUMBER_OF_WARNING_SUBOPERATIONS,   &warn),
					(tags::NUMBER_OF_REMAINING_SUBOPERATIONS, &to_do)
				].into_iter();
				for (a,v) in attr.filter(|(_,a)| a.is_some()) {
					self.inner.obj.put(DataElement::new(a, VR::US, dicom_value!(v.unwrap())));
				}
			}
		}
		self.inner.into_payload()
	}
}

impl Message
{
	pub fn read_from_data(data:Vec<PDataValue>, context: &Context) -> crate::Result<MessageType>
	{
		if data.is_empty(){return Err(UnexpectedEmptyMsg)}
		if !data.last().unwrap().is_last{return Err(IncompleteMsg)}

		// figure out transfer syntax
		let pc_id= context.select_pc(data[0].presentation_context_id).id;
		// flatten input vector
		let input = data.into_iter().fold(vec![],|mut i,mut d|{
			i.append(&mut d.data);
			i
		});
		// wrap the data into Cursor and feed that into the DICOM parser
		// The encoding of the Command Set shall be Little Endian Byte Ordering as defined in PS3.5
		// This is **regardless** of the transfer syntax negotiated
		// https://dicom.nema.org/dicom/news/March2024/docs/cpack126/cp2365.pdf
		let mut obj = InMemDicomObject::read_dataset_with_ts(Cursor::new(input),&IMPLICIT_VR_LITTLE_ENDIAN.erased())?;

		let command_code:u16 = Taker::take(&mut obj, tags::COMMAND_FIELD)?;
		let is_request = (command_code & 0x8000) == 0;
		let command = (command_code & !0x8000).try_into()
			.with_whatever_context::<_,_,DimseError>(|e|
				format!("Invalid command field code {:04X}H ({e})", command_code)
			)?;

		let ret:MessageType = if is_request
		{
			MessageType::Request(Request{
				sop_class: Taker::take(&mut obj,tags::AFFECTED_SOP_CLASS_UID)?,
				priority: Taker::take_opt(&mut obj, tags::PRIORITY)?.unwrap_or(0),
				inner: Message {
					command, pc_id,
					id: Taker::take(&mut obj, tags::MESSAGE_ID)?,
					obj, attachment:Attachment::None,
				},
			})
		} else { // it's a response
			let mut status = match_status(Taker::take(&mut obj, tags::STATUS)?);

			match &mut status {
				Ok(StatusOk::Success) => {}
				Ok(StatusOk::Pending { succeeded, warn, fail, to_do }) => {
					*to_do = Taker::take_opt(&mut obj, tags::NUMBER_OF_REMAINING_SUBOPERATIONS)?;
					*succeeded = Taker::take_opt(&mut obj, tags::NUMBER_OF_COMPLETED_SUBOPERATIONS)?;
					*fail = Taker::take_opt(&mut obj, tags::NUMBER_OF_FAILED_SUBOPERATIONS)?;
					*warn = Taker::take_opt(&mut obj, tags::NUMBER_OF_WARNING_SUBOPERATIONS)?;
				}
				Ok(StatusOk::Warning { offending_element, comment,.. })
				| Err(StatusFailure{ offending_element, comment, .. }) =>
					{
						*offending_element = Taker::take_opt(&mut obj, tags::OFFENDING_ELEMENT)?;
						*comment = Taker::take_opt(&mut obj, tags::ERROR_COMMENT)?;
					}
			}
			MessageType::Response(Response{
				inner: Message {
					command, pc_id,
					id: Taker::take(&mut obj, tags::MESSAGE_ID_BEING_RESPONDED_TO)?,
					obj, attachment:Attachment::None,
				},
				status
			})
		};
		Ok(ret)
	}
	pub(crate) async fn read_from<A>(ass:&mut Association<A>) -> crate::Result<MessageType> where A: ReceivePdu
	{
		// get a complete dataset
		let data = ass.read_pdata().await?;
		// read message
		let mut msg = Self::read_from_data(data, &ass.context)?;
		// read attachment, if existing
		match &mut msg {
			MessageType::Request(req) => &mut req.inner,
			MessageType::Response(res) => &mut res.inner
		}.read_attachment(ass).await?;
		Ok(msg)
	}
	pub(crate) async fn read_attachment<A>(&mut self,ass:&mut Association<A>) -> crate::Result<()> where A: ReceivePdu
	{
		let dataset_type:u16 = Taker::take(&mut self.obj, tags::COMMAND_DATA_SET_TYPE)?;
		if dataset_type != 0x0101
		{
			self.attachment = Attachment::Obj(ass.fetch_obj().await?)
		};
		Ok(())
	}
	pub fn attach(&mut self,ident:impl IntoIterator<Item = InMemElement>)
	{
		self.attachment= Attachment::Obj(InMemDicomObject::from_element_iter(ident));
	}
	pub fn attach_file(&mut self, filename:impl AsRef<Path>, context: &Context) -> crate::Result<()>
	{
		let filename = filename.as_ref();
		// read metadata header
		let (meta,offset) = io::get_filemeta(filename)
			.with_whatever_context::<_,_,DimseError>(|_|"Failed getting header of {} for attachment")?;
		// use that to fill reply
		self.obj.put(DataElement::new(tags::AFFECTED_SOP_INSTANCE_UID,VR::UI,meta.media_storage_sop_instance_uid()));

		// see if we can send in the original transfer syntax
		self.attachment = match context.match_file(&meta) {
			Some(pc) => {
				debug!("directly attaching file {} to the message {} using presentation context index {}",filename.display(), self.id, pc.id);
				Attachment::File { path: filename.to_path_buf(), offset, pc_id:pc.id}
			}
			None => {
				debug!("Attaching file {} as object to the message {}",filename.display(), self.id);
				let o = open_file(filename)
					.with_whatever_context::<_,_,DimseError>(|e|
						format!("Failed reading {} for attachment ({e})",filename.display()))?;
				Attachment::Obj(o.into_inner())
			}
		};
		Ok(())
	}
	pub fn abstract_syntax<A>(&self,ass: &Association<A>) -> String
	{
		ass.context.select_pc(self.pc_id).abstract_syntax.clone()
	}
	pub fn into_payload(mut self) -> crate::Result<Payload>{
		let mut data = Vec::new();
		let att_code = match self.attachment {
			Attachment::None => 0x0101,
			_ => 0x0000,
		};
		let title = match Taker::get_opt(&self.obj, tags::COMMAND_FIELD)? as Option<u16> {
			Some(e) if (e & 0x8000) == 0 => "request",
			Some(_) => "response to",
			None => "message",
		};
		self.obj.put(DataElement::new(tags::COMMAND_DATA_SET_TYPE,VR::US,dicom_value!(U16,att_code)));
		debug!("Preparing {title} {self} to be sent");
		let obj = InMemDicomObject::command_from_element_iter(self.obj.into_iter());
		lazy_dump_to_string(&obj).and_then(|s|Some(debug!("{s}")));
		obj.write_dataset_with_ts(&mut data, &IMPLICIT_VR_LITTLE_ENDIAN.erased())
			.with_whatever_context::<_,_,DimseError>(|e|
				format!("Preparation of message for sending failed ({e})"))?;

		let command = PDataValue {
			presentation_context_id:self.pc_id,
			value_type: PDataValueType::Command,
			is_last: true, data
		};
		Ok(Payload {command,attachment:self.attachment})
	}
}

impl Request {
	pub fn new(command: RequestCode, ctx:&PresentationContextNegotiated, attr: impl IntoIterator<Item=InMemElement>) -> Request
	{
		Request{
			sop_class:ctx.abstract_syntax.clone(),
			priority: 0,
			inner: Message {
				command, pc_id:ctx.id,
				id: rand::random::<u16>(),
				obj: InMemDicomObject::command_from_element_iter(attr),
				attachment: Attachment::None,
			},
		}
	}
	pub fn with_identifier(mut self,ident:impl IntoIterator<Item = InMemElement>) -> Self
	{
		self.attach(ident);
		self
	}
	pub fn with_file(mut self, filename:&Path, context: &Context) -> crate::Result<Self>
	{
		self.attach_file(filename,context)?;
		Ok(self)
	}
	pub fn make_response(&self, status: impl Into<Status>) -> Response
	{
		Response
		{
			inner:self.inner.clone(),
			status:status.into()
		}
	}
}

impl Response
{
	pub(crate) fn into_attachment(self) -> Attachment
	{
		self.inner.attachment
	}

}