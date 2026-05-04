use std::backtrace::Backtrace;
use std::fs::File;
use dicom::object::{FileDicomObject, FileMetaTable, FileMetaTableBuilder, InMemDicomObject, OpenFileOptions, ReadError};
use snafu::{GenerateImplicitData};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use dicom_dictionary_std::tags;
use futures::{stream, Stream, StreamExt};
use futures::stream::BoxStream;
use tokio::task::spawn_blocking;
use tracing::error;
use crate::definitions::FailureCode;
use crate::status::{failure, Comment, Status, StatusFailure};
use crate::Taker;
use crate::identifier::Identifier;

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

pub(crate) fn make_file(obj: InMemDicomObject, ts:&str) -> Result<FileDicomObject<InMemDicomObject>,StatusFailure>
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

pub(crate) fn get_filemeta(path:impl AsRef<Path>) -> Result<(FileMetaTable,u64), ReadError>
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