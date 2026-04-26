mod payload;
pub mod io;
pub mod error;
pub mod definitions;
pub mod client;
pub mod association;
mod msg;
mod task;
mod context;
pub(crate) mod dispatcher;
mod server;
pub mod pending_receive;
use std::io::{Cursor, Read};
use dicom::core::{DataDictionary, PrimitiveValue, Tag};
use dicom::core::header::Header;
use dicom::core::value::{ConvertValueError, DicomValueType, Value};
use dicom::dump::DumpOptions;
use dicom::object::{InMemDicomObject};
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::StandardDataDictionary;
use dicom_ul::{FullAeAddr};
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio::sync::watch::Sender;
use tracing::{error, trace};
use crate::error::Result;
use crate::dispatcher::Dispatcher;
use crate::io::FileAccess;

pub trait Taker
{
	fn transform(e:&InMemElement) -> Result<Self> where Self: Sized;
	fn tag_name(tag:Tag) -> String
	{
		StandardDataDictionary::default().by_tag(tag)
			.map(|e|format!("{}{}",e.alias,tag.to_string()))
			.unwrap_or(format!("{tag}"))
	}
	fn take_opt(obj:&mut InMemDicomObject,tag:Tag) -> Result<Option<Self>> where Self: Sized
	{
		obj.take(tag).map(|e|Taker::transform(&e)).transpose()
	}
	fn take(obj:&mut InMemDicomObject,tag:Tag) -> Result<Self> where Self: Sized
	{
		Taker::transform(&obj.take_element(tag)?)
	}
	fn get_opt(obj:&InMemDicomObject,tag:Tag) -> Result<Option<Self>> where Self: Sized
	{
		obj.get(tag).map(Taker::transform).transpose()
	}
	fn get(obj:&InMemDicomObject,tag:Tag) -> Result<Self> where Self: Sized
	{
		Taker::transform(obj.element(tag)?)
	}
}

impl Taker for u16
{
	fn transform(e: &InMemElement) -> Result<Self> where	Self: Sized
	{
		e.uint16().with_whatever_context(|err|
				format!("Failed taking {}:{} from object ({err})",Self::tag_name(e.tag()),e.vr()))
	}
}
impl Taker for String
{
	fn transform(e: &InMemElement) -> Result<Self> where	Self: Sized
	{
		e.to_str().map(|s|s.to_string())
			.with_whatever_context(|err|
				format!("Failed taking {}:{} from object ({err})",Self::tag_name(e.tag()),e.vr()))
	}
}

impl Taker for Vec<Tag>
{
	fn transform(e: &InMemElement) -> Result<Self> where Self: Sized
	{
		match e.value()
		{
			Value::Primitive(PrimitiveValue::Tags(tags)) => Ok(tags.to_vec()),
			_ => Err(ConvertValueError {
				requested: "tags",
				original: e.value().value_type(),
				cause: None,
			}).with_whatever_context(|err|
					format!("Failed taking {}:{} from object ({err})",Self::tag_name(e.tag()),e.vr()))
		}
	}
}

impl Taker for Vec<String>
{
	fn transform(e: &InMemElement) -> Result<Self> where	Self: Sized
	{
		e.to_multi_str().map(|s|s.into_owned())
			.with_whatever_context(|err|
			format!("Failed taking {}:{} from object ({err})",Self::tag_name(e.tag()),e.vr()))
	}
}
impl Taker for InMemElement
{
	fn transform(e: &InMemElement) -> Result<Self> where	Self: Sized
	{
		Ok(e.clone())
	}
}

pub fn lazy_dump_to_string<D>(obj:&InMemDicomObject<D>) -> Option<String> where D:DataDictionary
{
	let mut obj_str:Cursor<Vec<u8>> = Default::default();
	match DumpOptions::new().dump_object_to(&mut obj_str, obj) {
		Ok(_) => {
			let mut ret = String::new();
			obj_str.set_position(0);
			obj_str.read_to_string(&mut ret).map(|_|ret.trim_end().to_string())
				.ok()
		},
		Err(_) => None
	}
}
// Specs
// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/sect_9.3.html

pub async fn serve<A>(listener:impl Into<FullAeAddr<TcpListener>>, watcher: Sender<()>, accessor:A)
	-> Result<()> where A:FileAccess + 'static
{
	let listener = listener.into();
	tracing::info!("SCP \"{}\" listening on: tcp://{}",
		listener.ae_title(), listener.socket_addr().local_addr()?);

	loop {
		let (stream,addr) = tokio::select! {
			conn = listener.socket_addr().accept() => conn?,
			_ = watcher.closed() => {
				trace!("signal received, not accepting new connections");
				break;
			}
		};
		trace!("Accepted connection from {}", stream.peer_addr()?);
		let title = FullAeAddr::new(listener.ae_title(),addr);
		let watcher = watcher.clone();
		let accessor = accessor.clone();
		tokio::spawn(async move {
			match Dispatcher::new(title.ae_title(),stream, watcher,accessor).await
			{
				Ok(d) => d.run().await
					.unwrap_or_else(|e|error!("{title} aborted with error: \"{}\"", e)),
				Err(e) => error!("Dispatcher error: {e}")
			};
		});
	}
	Ok(())
}
