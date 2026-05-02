#![allow(dead_code)]
pub mod dcm;

use dicom::object::{FileDicomObject, InMemDicomObject, OpenFileOptions};
use dicom_dictionary_std::tags;
use dimse;
use dimse::error::DimseError;
use dimse::io::{FileAccess, ItemResult};
use dimse::{stream_files, RetrieveLevel, Taker};
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use futures::{Stream, stream, StreamExt, FutureExt};
use futures::stream::BoxStream;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::sync::watch::Receiver;
use tokio::task::{spawn_blocking, JoinHandle};
use tracing::error;
use dimse::definitions::FailureCode;
use dimse::identifier::Identifier;
use dimse::status::{failure, success, Comment, Offending, Status, StatusFailure};

pub async fn init_server<F: FileAccess + 'static>(accessor:F)
                                                  -> Result<(SocketAddr,Receiver<()>,JoinHandle<Result<(),DimseError>>), DimseError>
{
	let dicom_listener = TcpListener::bind("localhost:0").await
		.with_whatever_context::<_,_,DimseError>(|e|format!("Binding to localhost failed: {e}"))?;
	let addr = dicom_listener.local_addr()?;
	let (signal_tx, signal_rx) = tokio::sync::watch::channel(());

	let handle = spawn(dimse::serve(("TEST_SCP".to_string(),dicom_listener),signal_tx, accessor));
	Ok((addr, signal_rx, handle))
}

#[derive(Clone)]
pub struct NullAccess;

impl FileAccess for NullAccess {
	type Item = String;

	async fn get_path(&self, _: &Self::Item) -> Result<PathBuf, StatusFailure> {
		unimplemented!()
	}

	async fn store_file(&mut self, _: FileDicomObject<InMemDicomObject>) -> Status {
		unimplemented!()
	}

	async fn lookup<'a>(&self, _: impl Into<Identifier>) -> Result<BoxStream<'a,ItemResult<Self::Item>>, StatusFailure> {
		unimplemented!()
	}

	async fn find<'a>(&self, _: impl Into<Identifier>) -> Result<BoxStream<'a,ItemResult<InMemDicomObject>>, StatusFailure> {
		unimplemented!()
	}
}

#[derive(Default,Clone)]
pub struct MultiFileAccess{
	instances:Arc<RwLock<HashMap<String,PathBuf>>>,
	series:Arc<RwLock<HashMap<String,Vec<String>>>>,
	studies:Arc<RwLock<HashMap<String,Vec<String>>>>,
}

impl MultiFileAccess {
	pub async fn new(objs:impl IntoIterator<Item=FileDicomObject<InMemDicomObject>>) -> Result<Self, StatusFailure> {
		let mut ret = Self::default();
		for o in objs{
			ret.store_file(o).await?;
		}
		Ok(ret)
	}
}

impl Drop for MultiFileAccess {
	fn drop(&mut self) {
		if Arc::strong_count(&self.instances) <=1
		{
			let map = self.instances.blocking_read();
			for (_,file) in map.iter() {
				if let Err(e) = std::fs::remove_file(file){
					error!("Failed to remove file: {} ({e})", file.display());
				}
			}
		};
	}
}
impl FileAccess for MultiFileAccess {
	type Item = String;

	async fn get_path(&self, uid: &Self::Item) -> Result<PathBuf, StatusFailure> {
		Ok(std::env::temp_dir().join(format!("Mr.{uid}.dcm")))
	}

	async fn store_file(&mut self, obj: FileDicomObject<InMemDicomObject>) -> dimse::status::Status
	{
		let instance = Taker::get(&obj,tags::SOP_INSTANCE_UID)?;
		let series = Taker::get(&obj,tags::SERIES_INSTANCE_UID)?;
		let study = Taker::get(&obj,tags::STUDY_INSTANCE_UID)?;
		let path= self.get_path(&instance).await?;
		let file = std::fs::File::create(path.clone())
			.map_err(|e|
				failure(FailureCode::ProcessingFailure).comment(format!("creating {} failed ({e})", path.display()))	
			)?;
		obj.write_all(file)
			.map_err(|e|
				failure(FailureCode::ProcessingFailure)
					.comment(format!("writing {} failed ({e})", path.display()))
			)?;
		self.instances.write().await.insert(instance.clone(),path.clone());
		self.series.write().await.entry(series).or_default().push(instance.clone());
		self.studies.write().await.entry(study).or_default().push(instance);
		Ok(success().into())
	}

	async fn lookup<'a>(&self, ident: impl Into<Identifier> + Send) -> Result<BoxStream<'a,ItemResult<Self::Item>>, StatusFailure> {
		let ident = ident.into();
		match ident.level {
			Some(RetrieveLevel::IMAGE) => {
				if let Some(sel_instance) = ident.contains(tags::SOP_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					Ok(stream::iter(vec![Ok(sel_instance.to_string())]).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::SOP_INSTANCE_UID]))}
			}
			Some(RetrieveLevel::SERIES) => {
				if let Some(sel_series) = ident.contains(tags::SERIES_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					let series = self.series.read().await;
					let series:Vec<_> = series.get(&sel_series.to_string()).into_iter().flatten().map(|s| Ok(s.clone())).collect();
					Ok(stream::iter(series).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::SERIES_INSTANCE_UID]))}
			}
			Some(RetrieveLevel::STUDY) => {
				if let Some(sel_study) = ident.contains(tags::STUDY_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					let study = self.series.read().await;
					let study:Vec<_> = study.get(&sel_study.to_string()).into_iter().flatten().map(|s| Ok(s.clone())).collect();
					Ok(stream::iter(study).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::STUDY_INSTANCE_UID]))}
			}
			_ => Err(failure(FailureCode::CannotUnderstand).offending([tags::QUERY_RETRIEVE_LEVEL]))
		}
	}

	async fn find<'a>(&self, ident: impl Into<Identifier> + Send) -> Result<BoxStream<'a,ItemResult<InMemDicomObject>>, StatusFailure> {
		let ident = ident.into();
		match ident.level {
			Some(RetrieveLevel::IMAGE) => {}
			Some(_) => return Err(failure(FailureCode::InvalidArgument).comment("Can only do IMAGE level")),
			_ => {}
		}
		let instances = self.instances.read().await.clone();
		let instances = stream::iter(instances).filter_map(move |(_,p)| {
			let ident = ident.clone();
			async move {
				spawn_blocking(
					move || OpenFileOptions::new().read_until(tags::PIXEL_DATA).open_file(p.clone())
						.map_err(|e| failure(FailureCode::Failure).comment(e))
				).await.unwrap()
				.map(|o| ident.matches_all(&o).then_some(o.into_inner())).transpose()
			}
		});
		Ok(instances.boxed())
	}
}
