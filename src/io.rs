use std::backtrace::Backtrace;
use std::fs::File;
use super::error::DimseError::TransferSyntaxNotFound;
use dicom::encoding::TransferSyntaxIndex;
use dicom::object::{FileDicomObject, FileMetaTable, FileMetaTableBuilder, InMemDicomObject, OpenFileOptions, ReadError};
use dicom::transfer_syntax::TransferSyntaxRegistry;
use dicom_ul::pdu::{PDataValue, PDataValueType};
use snafu::{GenerateImplicitData, ResultExt};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use dicom_dictionary_std::tags;
use futures::{stream, Stream, StreamExt};
use futures::stream::BoxStream;
use tokio::sync::mpsc::Receiver;
use tokio::task::spawn_blocking;
use tracing::error;
use crate::definitions::FailureCode;
use crate::error::DimseError;
use crate::status::{failure, Comment, Status, StatusFailure};
use crate::Taker;
use crate::identifier::Identifier;

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

pub type ItemResult<I> = Result<I,StatusFailure>;

pub trait FileAccess: Clone + Send + Sync
{
	type Item:Send + Sync;
	fn get_uid(&self, uid:&Self::Item) -> impl Future<Output = Result<String, StatusFailure>> + Send;
	fn get_path(&self, uid:&Self::Item) -> impl Future<Output = Result<PathBuf, StatusFailure>> + Send;
	fn store_file(&mut self, file:FileDicomObject<InMemDicomObject>) -> impl Future<Output = Status> + Send;
	fn lookup<'a>(&self, ident: impl Into<Identifier> + Send)
		-> impl Future<Output = Result<BoxStream<'a,Self::Item>, StatusFailure>> + Send;
	fn find<'a>(&self, ident: impl Into<Identifier> + Send)
		-> impl Future<Output = Result<BoxStream<'a,ItemResult<InMemDicomObject>>, StatusFailure>> + Send;
}

pub async fn read_pipe(source: &mut Option<Receiver<PDataValue>>, prelude: Vec<u8>, ts:String)
                       -> crate::error::Result<InMemDicomObject>
{
	let ts = TransferSyntaxRegistry.get(ts.as_str()).ok_or(TransferSyntaxNotFound{ts})?;
	let prelude = Cursor::new(prelude);

	// take the receiver and move it into a sync block
	let mut s =  source.take().expect("There must be a receiver");
	let (r,s) = spawn_blocking(move || {
		// chain the prelude and the pipe-reader and read that as dicom object
		let buffer = prelude.chain(PipeDataReader::new(&mut s));
		InMemDicomObject::read_dataset_with_ts(buffer, &ts).map(|e|(e,s))
	}).await.expect("Reading thread panicked")
		.whatever_context::<_,DimseError>("asynchronous reading of stream failed")?;

	// got our receiver back, put it back in place
	source.replace(s);
	Ok(r)
}

pub fn make_file(obj: InMemDicomObject, ts:&str) -> Result<FileDicomObject<InMemDicomObject>,StatusFailure>
{
	let file_meta = FileMetaTableBuilder::new().transfer_syntax(ts);
	let uid = Taker::get(&obj,tags::SOP_INSTANCE_UID).ok().unwrap_or("file".to_string());
	match obj.with_meta(file_meta){
		Ok(obj) => Ok(obj),
		Err(e) => {
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part04/sect_b.2.3.html
			// https://dicom.nema.org/medical/dicom/current/output/chtml/part07/chapter_9.html
			error!("Error {e} when preparing received data for storage");
			Err(failure(FailureCode::ProcessingFailure).comment(format!("Failed storing {uid}:({e})"))
			)
		}
	}
}

pub fn get_filemeta(path:impl AsRef<Path>) -> Result<(FileMetaTable,u64), ReadError>
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

pub fn stream_files<T>(files:T) -> impl Stream<Item=ItemResult<InMemDicomObject>> + Send
	where T:IntoIterator<Item=PathBuf> + Send, <T as IntoIterator>::IntoIter:Send
{
	stream::iter(files).then(move|p|{
		async {
			spawn_blocking(
				move || OpenFileOptions::new().read_until(tags::PIXEL_DATA).open_file(p)
					.map_err(|e| failure(FailureCode::Failure).comment(e))
					.map(|o|o.into_inner())
			).await.unwrap()
		}
	})

}