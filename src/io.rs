use std::backtrace::Backtrace;
use std::fs::File;
use super::error::DimseError::TransferSyntaxNotFound;
use super::error::{DimseError, Result};
use dicom::encoding::TransferSyntaxIndex;
use dicom::object::{FileDicomObject, FileMetaTable, FileMetaTableBuilder, InMemDicomObject, ReadError};
use dicom::transfer_syntax::TransferSyntaxRegistry;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use snafu::{GenerateImplicitData, ResultExt};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use dicom_dictionary_std::tags;
use tokio::sync::mpsc::Receiver;
use tracing::error;
use crate::definitions::{FailureCode, Status, StatusFailure};
use crate::Taker;

pub struct PipeDataReader<'a>{
	source: &'a mut Receiver<PDataValue>,
	last: PDataValue,
}
impl<'a> PipeDataReader<'a> {
	pub(crate) fn new(source: &'a mut Receiver<PDataValue>) -> Self {
		PipeDataReader{source,last:PDataValue{
			presentation_context_id:0,
			value_type:PDataValueType::Data,
			is_last: false,
			data: vec![],
		}}
	}
}

impl<'a> Read for PipeDataReader<'a> {
	fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
		if self.last.data.is_empty(){
			if self.last.is_last{return Ok(0)}
			match self.source.blocking_recv() {
				Some(v) => self.last = v,
				None => return Ok(0)
			}
		}
		let written = buf.write(&self.last.data)?;
		self.last.data.drain(.. written);
		Ok(written)
	}
}

pub trait FileAccess: Clone + Send + Sync
{
	type Item:Send + Sync;
	fn get_path(&self, uid:&Self::Item) -> impl Future<Output = std::result::Result<PathBuf, StatusFailure>> + Send;
	fn store_file(&mut self, file:FileDicomObject<InMemDicomObject>) -> impl Future<Output = Status> + Send;
	fn lookup(&self, ident: InMemDicomObject) -> impl Future<Output = std::result::Result<Vec<Self::Item>, StatusFailure>> + Send;
}

pub async fn read_pipe(source: &mut Option<Receiver<PDataValue>>, prelude: Vec<u8>, ts:String)
                       -> Result<InMemDicomObject>
{
	let ts = TransferSyntaxRegistry.get(ts.as_str()).ok_or(TransferSyntaxNotFound{ts})?;
	let prelude = Cursor::new(prelude);

	// take the receiver and move it into a sync block
	let mut s =  source.take().expect("There must be a receiver");
	let (r,s) = tokio::task::spawn_blocking(move || {
		// chain the prelude and the pipe-reader and read that as dicom object
		let buffer = prelude.chain(PipeDataReader::new(&mut s));
		InMemDicomObject::read_dataset_with_ts(buffer, &ts).map(|e|(e,s))
	}).await.expect("Reading thread panicked")
		.whatever_context::<_,DimseError>("asynchronous reading of stream failed")?;

	// got our receiver back, put it back in place
	source.replace(s);
	Ok(r)
}

pub async fn make_file(obj: InMemDicomObject, ts:&str) -> std::result::Result<FileDicomObject<InMemDicomObject>,StatusFailure>
{
	let file_meta = FileMetaTableBuilder::new().transfer_syntax(ts);
	let uid = Taker::get(&obj,tags::SOP_INSTANCE_UID).ok().unwrap_or("file".to_string());
	match obj.with_meta(file_meta){
		Ok(obj) => Ok(obj),
		Err(e) => {
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_b.2.3.html
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html
			error!("Error {e} when preparing received data for storage");
			Err(StatusFailure{
				code:FailureCode::ProcessingFailure,
				offending_element: None,
				comment: Some(format!("Failed storing {uid}:({e})")),
			})
		}
	}
}

pub fn get_filemeta(path:impl AsRef<Path>) -> std::result::Result<(FileMetaTable,u64), ReadError>
{
	let path = path.as_ref();
	let mut file =	File::open(path).map_err(|e|ReadError::OpenFile {
		filename: path.to_path_buf(),
		backtrace: Backtrace::generate_with_source(&e),
		source: e
	})?;
	file.seek(SeekFrom::Start(128)).map_err(|e|ReadError::ReadFile {
		filename: path.to_path_buf(),
		backtrace: Backtrace::generate_with_source(&e),
		source: e
	})?;

	// read metadata header
	let meta = FileMetaTable::from_reader(&mut file).map_err(|e|ReadError::ParseMetaDataSet { source: e })?;
	file.stream_position().map(|p|(meta,p)).map_err(|e|ReadError::ReadFile {
		filename: path.to_path_buf(),
		backtrace: Backtrace::generate_with_source(&e),
		source: e
	})
}