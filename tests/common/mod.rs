#![allow(dead_code)]
pub mod dcm;

use dicom::object::{FileDicomObject, InMemDicomObject};
use dicom_dictionary_std::tags;
use dimse;
use dimse::definitions::FailureCode::{Failure, NoSuchAttribute, NoSuchSOPInstance};
use dimse::definitions::{FailureCode, StatusFailure, StatusOk};
use dimse::error::DimseError;
use dimse::io::FileAccess;
use dimse::Taker;
use snafu::ResultExt;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tracing::error;

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
		Err(StatusFailure{code: NoSuchSOPInstance, offending_element: None, comment: None})
	}

	async fn store_file(&mut self, _: FileDicomObject<InMemDicomObject>) -> dimse::definitions::Status
	{
		Err(StatusFailure{code: Failure, offending_element: None, comment: None})
	}

	async fn lookup(&self, _: InMemDicomObject) -> Result<Vec<Self::Item>, StatusFailure> {
		Err(StatusFailure{code: Failure, offending_element: None, comment: None})
	}
}

#[derive(Clone)]
pub struct MultiFileAccess(Arc<Mutex<HashMap<String,PathBuf>>>);

impl MultiFileAccess {
	pub async fn new(objs:impl IntoIterator<Item=FileDicomObject<InMemDicomObject>>) -> Result<Self, StatusFailure> {
		let mut ret = MultiFileAccess(Default::default());
		for o in objs{
			ret.store_file(o).await?;
		}
		Ok(ret)
	}
}

impl Drop for MultiFileAccess {
	fn drop(&mut self) {
		if Arc::strong_count(&self.0) <=1
		{
			let map = self.0.lock().unwrap();
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

	async fn store_file(&mut self, obj: FileDicomObject<InMemDicomObject>) -> dimse::definitions::Status
	{
		let uid = Taker::get(&obj,tags::SOP_INSTANCE_UID)?;
		let path= self.get_path(&uid).await?;
		let file = std::fs::File::create(path.clone())
			.map_err(|e|StatusFailure::new(
				FailureCode::ProcessingFailure,
				Some(format!("creating {} failed ({e})", path.display()))
			)
		)?;
		obj.write_all(file)
			.map_err(|e|StatusFailure::new(
				FailureCode::ProcessingFailure,
				Some(format!("writing {} failed ({e})", path.display()))
			))?;
		self.0.lock().unwrap().insert(uid,path);
		StatusOk::Success.into()
	}

	async fn lookup(&self, ident: InMemDicomObject) -> Result<Vec<Self::Item>, StatusFailure> {
		Taker::get(&ident,tags::SOP_INSTANCE_UID)
			.map(|s|vec![s])
			.map_err(|_| StatusFailure{
				code: NoSuchAttribute,
				offending_element: Some(vec![tags::SOP_INSTANCE_UID]),
				comment: None
			})
	}
}
