use clap::builder::PossibleValue;
use clap::{Parser, ValueEnum};
use dicom::core::{PrimitiveValue, VR};
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::tags;
use dicom_ul::AeAddr;
use dimse::RetrieveLevel;
use dimse::client::{Client, ClientAssociation};
use futures::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, Level};
use dimse::Taker;

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


pub fn socketaddr_value_parser(value: &str) -> Result<AeAddr<SocketAddr>, String>
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

#[derive(Parser)]
#[command(author, version, about="A client tool (SCU) to synchronize data between SPCs", long_about = None)]
struct Cli {
	/// logging level
	#[arg(long, default_value = Level::INFO.as_str())]
	log_level:LogLevel,
	/// Address of the source PACS (AET@IP:PORT).
	#[arg(long, short, value_parser = socketaddr_value_parser)]
	source:AeAddr<SocketAddr>,
	/// Address of the destination PACS (AET@IP:PORT).
	#[arg(long, short, value_parser = socketaddr_value_parser)]
	dest:AeAddr<SocketAddr>,
	/// The calling AE title, default is ANY-SCU.
	#[arg(long)]
	calling_ae:Option<String>,
	/// UID of the study to synchronize
	study:String,

}

async fn connect(addr:AeAddr<SocketAddr>, calling_ae:&Option<String>) -> dimse::error::Result<(ClientAssociation,ProgressBar)>
{
	let sp = ProgressBar::new_spinner();
	sp.enable_steady_tick(Duration::from_millis(500));
	sp.set_message(format!("Connecting to {}", addr));
	Client::new(addr, calling_ae.clone(), vec![]).await
		.map(|c|(c,sp))
}

async fn get_instances(study:&str, client:&mut ClientAssociation, sp:ProgressBar)
	-> dimse::error::Result<BTreeMap<String,String>> {
	sp.set_message(format!(r#"Looking up "{}""#, study));
	let mut instances = BTreeMap::<String,String>::default();
	let mut found =client.find(RetrieveLevel::IMAGE,[
		InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, study),
		InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
	]).await?;
	sp.finish_and_clear();

	let progress = ProgressBar::no_length();
	progress.set_style(ProgressStyle::with_template("[Scanning study] {wide_bar} {pos}/{len}").unwrap());
	while let Some(mut obj) = found.next().await.transpose()? {
		if let Some(s) = found.size_hint().1 {
			progress.set_length(progress.position() + s as u64)
		}
		progress.inc(1);
		instances.insert(
			Taker::take(&mut obj, tags::SOP_INSTANCE_UID)?,
			Taker::take(&mut obj, tags::SOP_CLASS_UID)?,
		);
	}
	Ok(instances)
}

#[tokio::main]
async fn main() -> dimse::error::Result<()>
{
	let cli=Cli::parse();
	tracing_subscriber::fmt().with_max_level(cli.log_level.0).init();

	let d_name = cli.dest.to_string();
	let (mut source, source_progress) = connect(cli.source, &cli.calling_ae).await?;
	let (mut dest, dest_progress) = connect(cli.dest, &cli.calling_ae).await?;

	let in_source = get_instances(cli.study.as_str(),&mut source, source_progress).await?;
	let in_dest= get_instances(cli.study.as_str(),&mut dest, dest_progress).await?;
	let remain = in_source.into_iter()
		.filter(|(k,_)|in_dest.contains_key(k))
		.collect::<Vec<_>>();

	let sp = ProgressBar::new(remain.len() as u64);
	sp.set_style(ProgressStyle::with_template(["[Copying {len} files to ",&d_name,"] {wide_bar} {eta}"].concat().as_str()).unwrap());

	for (uid,class) in remain {
		let got = source.get_file(uid,class).await?;
		dest.store_fileobj(got,[]).await?;
		sp.inc(1);
	}

	Ok(())
}