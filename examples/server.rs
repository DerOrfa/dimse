use futures::{FutureExt, StreamExt};
use clap::builder::PossibleValue;
use clap::{Parser, ValueEnum};
use dicom::object::{FileDicomObject, InMemDicomObject, OpenFileOptions};
use dicom_dictionary_std::tags;
use dicom_ul::AeAddr;
use dimse::definitions::FailureCode;
use dimse::error::DimseError;
use dimse::io::{FileAccess, ItemResult};
use dimse::status::{failure, Comment, Offending, Status, StatusFailure};
use dimse::{io, RetrieveLevel, Taker};
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
use futures::stream::BoxStream;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::{spawn_blocking, JoinSet};
use tokio::{signal, spawn};
use tracing::{info, Level};
use dimse::definitions::FailureCode::MissingAttribute;
use dimse::identifier::Identifier;

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
	pub fn stream_files<'a>(&self,direct_select:Option<String>) -> impl Stream<Item=ItemResult<(String,InMemDicomObject)>> + Send +'a
	{
		let reader = self.files.clone().read_owned();
		async move {
			let i = reader.await;
			let instances:Vec<_> = match direct_select
			{
				Some(instance) => i.get_key_value(&instance)
					.map_or(vec![], |(uid,v)|vec![(uid.clone(),v.clone())]),
				_ => i.clone().into_iter().collect(),
			};
			stream::iter(instances)
		}.into_stream().flatten()
		.then( async move|(uid,p)|{
			spawn_blocking(
				move || OpenFileOptions::new().read_until(tags::PIXEL_DATA).open_file(p.clone())
					.map_err(|e| failure(FailureCode::Failure).comment(e))
			).await.unwrap().map(|o|(uid,o.into_inner()))
		})
	}
}

impl FileAccess for Accessor
{
	type Item = String;

	async fn get_uid(&self, uid: &Self::Item) -> Result<String, StatusFailure> {
		Ok(uid.to_string())
	}

	async fn get_path(&self, uid: &Self::Item) -> Result<PathBuf, StatusFailure>{
		self.files.read().await.get(uid).cloned().ok_or(failure(FailureCode::NoSuchSOPInstance))
	}

	async fn store_file(&mut self, _: FileDicomObject<InMemDicomObject>) -> Status {
		todo!()
	}

	async fn lookup<'a>(&self, ident: impl Into<Identifier> + Send) -> Result<BoxStream<'a,Self::Item>, StatusFailure> {
		let ident = ident.into();
		match ident.level {
			Some(RetrieveLevel::IMAGE) => {
				if let Some(sel_instance) = ident.contains(tags::SOP_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					Ok(stream::iter(vec![sel_instance.to_string()]).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::SOP_INSTANCE_UID]))}
			}
			Some(RetrieveLevel::SERIES) => {
				if let Some(sel_series) = ident.contains(tags::SERIES_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					let series = self.series.read().await;
					let series:Vec<_> = series.get(&sel_series.to_string()).into_iter().flatten().cloned().collect();
					Ok(stream::iter(series).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::SERIES_INSTANCE_UID]))}
			}
			Some(RetrieveLevel::STUDY) => {
				if let Some(sel_study) = ident.contains(tags::STUDY_INSTANCE_UID).and_then(|e|e.to_str().ok()){
					let study = self.series.read().await;
					let study:Vec<_> = study.get(&sel_study.to_string()).into_iter().flatten().cloned().collect();
					Ok(stream::iter(study).boxed())
				} else {Err(failure(FailureCode::InvalidAttributeValue).offending([tags::STUDY_INSTANCE_UID]))}
			}
			_ => Err(failure(FailureCode::CannotUnderstand).offending([tags::QUERY_RETRIEVE_LEVEL]))
		}
	}

	async fn find<'a>(&self, ident: impl Into<Identifier> + Send) -> Result<BoxStream<'a,ItemResult<InMemDicomObject>>, StatusFailure>
	{
		let ident = ident.into();
		let first_in = match ident.level{
			Some(RetrieveLevel::IMAGE) => {
				let stream = self.stream_files(None).filter_map(move|r|{
					let ident = ident.clone();
					async move {
						r.map(|(_,o)|ident.matches_all(&o).then_some(o)).transpose()
					}
				});
				return Ok(stream.boxed());
			}
			Some(RetrieveLevel::SERIES) => {
				let files = self.files.read().await;
				self.series.read().await.iter()
					.filter_map(|(_,v)|v.first())
					.map(|s|files.get(s).cloned().unwrap()).collect::<Vec<_>>()
			}
			Some(RetrieveLevel::STUDY) => {
				let files = self.files.read().await;
				self.studies.read().await.iter()
					.filter_map(|(_,v)|v.first())
					.map(|s|files.get(s).cloned().unwrap()).collect::<Vec<_>>()
			}
			_ => return Err(failure(MissingAttribute).offending([tags::QUERY_RETRIEVE_LEVEL]))
		};
		Ok(io::stream_files(first_in).filter_map(move|r|{
			let ident = ident.clone();
			async move {
				r.map(|o|ident.matches_all(&o).then_some(o)).transpose()
			}
		}).boxed())
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
	dimse::serve(address, [], signal_tx, a).await
}