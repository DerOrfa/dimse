use futures::StreamExt;
use clap::builder::PossibleValue;
use clap::{Parser, ValueEnum};
use dicom::object::{FileDicomObject, InMemDicomObject, OpenFileOptions};
use dicom_dictionary_std::tags;
use dicom_ul::AeAddr;
use dimse::definitions::FailureCode;
use dimse::error::DimseError;
use dimse::io::{FileAccess, ItemResult};
use dimse::status::{failure, Comment, Offending, Status, StatusFailure};
use dimse::Taker;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use snafu::prelude::*;
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use futures::{stream, Stream};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::{spawn_blocking, JoinSet};
use tokio::{signal, spawn};
use tracing::{info, Level};

#[derive(Clone)]
struct LogLevel(Level);
impl ValueEnum for LogLevel
{
	fn value_variants<'a>() -> &'a [Self]
	{
		&[
			LogLevel(Level::TRACE),
			LogLevel(Level::DEBUG),
			LogLevel(Level::INFO),
			LogLevel(Level::WARN),
			LogLevel(Level::ERROR)
		]
	}

	fn to_possible_value(&self) -> Option<PossibleValue> {
		let alias= self.0.to_string().to_lowercase();
		Some(PossibleValue::new(self.0.as_str()).alias(alias))
	}
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
	/// logging level
	#[arg(long, default_value = Level::INFO.as_str())]
	log_level:LogLevel,
	/// address for the PACS to bind to
	#[arg(value_parser = socketaddr_value_parser)]
	addr:AeAddr<SocketAddr>,
	/// a (globbing) path for files to serve
	path:String,
}


pub(crate) fn socketaddr_value_parser(value: &str) -> Result<AeAddr<SocketAddr>, String>
{
	let addr = AeAddr::<String>::from_str(value).map_err(|e|e.to_string())?;
	let aet = addr.ae_title();
	let addr = addr.socket_addr().to_socket_addrs().map_err(|e|e.to_string())?
		.filter(|a|a.is_ipv4())
		.next().ok_or(String::from("Invalid address"))?
		;
	Ok(match aet
	{
		None => AeAddr::from(addr),
		Some(ae) => AeAddr::from(addr).with_ae_title(ae).into()
	})
}

#[derive(Clone)]
struct Accessor
{
	files:Arc<RwLock<HashMap<String,PathBuf>>>,
	series:Arc<RwLock<HashMap<String,Vec<String>>>>,
	studies:Arc<RwLock<HashMap<String,Vec<String>>>>
}

impl Accessor {
	pub async fn new(glb:&str) -> dimse::error::Result<Self>{
		let mut readers=JoinSet::new();
		let sp = ProgressBar::new_spinner();
		sp.enable_steady_tick(Duration::from_millis(500));
		sp.set_message(format!("Scanning {glb}"));
		for p in glob(glb).unwrap().filter_map(Result::ok){
			readers.spawn(async move {
				(p.clone(),OpenFileOptions::new()
					.read_until(tags::PIXEL_DATA)
					.open_file(p))
			});
		}
		sp.finish_and_clear();
		let progress=ProgressBar::new(readers.len() as u64);
		progress.set_style(ProgressStyle::with_template("[Reading {len} files] {wide_bar} {eta}").unwrap());
		let mut instances = HashMap::default();
		let mut series: HashMap<_, Vec<_>> = HashMap::default();
		let mut studies:HashMap<_, Vec<_>> = HashMap::default();
		while let Some((p,result)) = readers.join_next().await.transpose().unwrap() {
			if let Ok(o) = result
			{
				let instance = Taker::get(&o,tags::SOP_INSTANCE_UID)?;
				let ser = Taker::get(&o,tags::SERIES_INSTANCE_UID)?;
				let stdy = Taker::get(&o,tags::STUDY_INSTANCE_UID)?;
				instances.insert(String::clone(&instance),p);
				series.entry(ser).or_default().push(String::clone(&instance));
				studies.entry(stdy).or_default().push(String::clone(&instance));
			}
			progress.inc(1);
		}
		progress.finish_and_clear();
		info!("Registered {} instances", instances.len());
		Ok(Self{
			files: Arc::new(RwLock::new(instances)),
			series: Arc::new(RwLock::new(series)),
			studies: Arc::new(RwLock::new(studies)),
		})
	}
}

impl FileAccess for Accessor
{
	type Item = String;

	async fn get_path(&self, uid: &Self::Item) -> Result<PathBuf, StatusFailure>{
		self.files.read().await.get(uid).cloned().ok_or(failure(FailureCode::NoSuchSOPInstance))
	}

	async fn store_file(&mut self, _: FileDicomObject<InMemDicomObject>) -> Status {
		todo!()
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

	fn find<'a>(&self, ident: &InMemDicomObject) -> Result<impl Stream<Item=ItemResult<Self::Item>> + Send + 'a, StatusFailure>{
		let name_glob = Taker::get_opt(&ident,tags::PATIENT_NAME)?.ok_or(failure(FailureCode::NoSuchAttribute).offending([tags::PATIENT_NAME]))?;
		let instances = self.files.clone();
		let instances_stream = stream::once(async move {
			let v:Vec<_> = instances.read().await.clone().into_iter().map(|(uid,p)|(uid,p, String::clone(&name_glob)))
				.collect();
			stream::iter(v)
		})
			.flatten()
			.filter_map( async move|(uid,p,name_glob)|{
				let obj = spawn_blocking(
					move || OpenFileOptions::new().read_until(tags::PIXEL_DATA).open_file(p.clone())
						.map_err(|e| failure(FailureCode::Failure).comment(e))).await.unwrap();
				obj.map(|o|{
					let name: String = Taker::get(&o, tags::PATIENT_NAME).unwrap();
					glob_match::glob_match(name_glob.as_str(), &name).then(|| uid)
				}).transpose()
			});
		Ok(instances_stream)
	}
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
#[snafu::report]
async fn main() -> Result<(),DimseError>
{
	let cli=Cli::parse();

	tracing_subscriber::fmt().with_max_level(cli.log_level.0).init();

	let dicom_listener = TcpListener::bind(cli.addr.socket_addr()).await
		.with_whatever_context::<_,_,DimseError>(|e|format!("Binding to {} failed: {e}", cli.addr))?;
	let (signal_tx, signal_rx) = tokio::sync::watch::channel(());

	spawn(async move {
		signal::ctrl_c()
			.await
			.expect("failed to install Ctrl+C handler");
		eprintln!("Got CTRL+C trying graceful shutdown");
		drop(signal_rx);
	});

	let a= Accessor::new(cli.path.as_str()).await?;
	let address = (cli.addr.ae_title().unwrap_or("DIMSE").to_string(),dicom_listener);
	dimse::serve(address,signal_tx, a).await
}