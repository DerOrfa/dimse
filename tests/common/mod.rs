#![allow(dead_code)]
pub mod dcm;

use dicom::object::{FileDicomObject, InMemDicomObject};
use dicom_dictionary_std::tags;
use dimse;
use dimse::error::DimseError;
use dimse::io::{FileAccess, ItemResult};
use dimse::Taker;
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use futures::{Stream, stream, StreamExt};
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::sync::watch::Receiver;
use tokio::task::{spawn_blocking, JoinHandle};
use tracing::error;
use dimse::definitions::FailureCode;
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
		Err(failure(FailureCode::NoSuchSOPInstance))
	}

	async fn store_file(&mut self, _: FileDicomObject<InMemDicomObject>) -> Status {
		Err(failure(FailureCode::NoSuchSOPInstance))
	}

	fn lookup<'a>(&self, _: &InMemDicomObject) -> Result<impl Stream<Item=ItemResult<Self::Item>> + Send + 'a, StatusFailure> {
		Ok(stream::iter(vec![]))

	}

	fn find<'a>(&self, _: &InMemDicomObject) -> Result<impl Stream<Item=ItemResult<Self::Item>> + Send + 'a, StatusFailure> {
		Ok(stream::iter(vec![]))
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

	fn lookup<'a>(&self, ident: &InMemDicomObject) -> Result<impl Stream<Item=ItemResult<Self::Item>> + Send + 'a, StatusFailure> {

		let sel_instance = Taker::get_opt(&ident,tags::SOP_INSTANCE_UID)?;
		let sel_series = Taker::get_opt(&ident,tags::SERIES_INSTANCE_UID)?;
		let sel_study = Taker::get_opt(&ident,tags::STUDY_INSTANCE_UID)?;
		let series = self.series.clone();
		let studies = self.studies.clone();
		let stream = stream::once(async move {
			if let Some(instance) = sel_instance
			{
				stream::iter(vec![Ok(instance)])
			} else if let Some(sel_series) = sel_series	{
				let series:Vec<_> = series.read().await.get::<String>(&sel_series).unwrap().clone()
					.into_iter().map(Ok::<_,StatusFailure>).collect();
				stream::iter(series)
			} else if let Some(sel_study) = sel_study {
				let studies:Vec<_> = studies.read().await.get::<String>(&sel_study).unwrap().clone()
					.into_iter().map(Ok::<_,StatusFailure>).collect();
				stream::iter(studies)
			}
			else { panic!("invalid ident") }
		});
		Ok(stream.flatten())
	}

	fn find<'a>(&self, ident: &InMemDicomObject) -> Result<impl Stream<Item=ItemResult<Self::Item>> + Send + 'a, StatusFailure> {
		let name_glob = Taker::get_opt(&ident,tags::PATIENT_NAME)?.ok_or(failure(FailureCode::NoSuchAttribute).offending([tags::PATIENT_NAME]))?;
		let instances = self.instances.clone();
		let instances_stream = stream::once(async move {
			let v:Vec<_> = instances.read().await.clone().into_iter().map(|(uid,p)|(uid,p, String::clone(&name_glob)))
				.collect();
			stream::iter(v)
		})
		.flatten()
		.filter_map( async move|(uid,p,name_glob)|{
				let obj = spawn_blocking(move || dicom::object::open_file(p.clone())
						.map_err(|e| failure(FailureCode::Failure).comment(e))).await.unwrap();
				obj.map(|o|{
					let name: String = Taker::get(&o, tags::PATIENT_NAME).unwrap();
					glob_match::glob_match(name_glob.as_str(), &name).then(|| uid)
				}).transpose()
			});
		Ok(instances_stream)
	}
}
