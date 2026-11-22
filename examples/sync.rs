use clap::builder::PossibleValue;
use clap::{Parser, ValueEnum};
use dicom::core::{PrimitiveValue, VR};
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::{tags, StandardSopClassDictionary};
use dicom_ul::AeAddr;
use dimse::RetrieveLevel;
use dimse::Taker;
use dimse::client::{Client, ClientAssociation};
use futures::{Stream, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;
use comfy_table::{Cell, ContentArrangement, Table};
use dicom::core::dictionary::{UidDictionary, UidDictionaryEntry};
use dicom::object::InMemDicomObject;
use tokio::{try_join};
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
	#[arg(long)]
	/// only show what would be copied
	probe:bool
}

async fn connect(addr:AeAddr<SocketAddr>, calling_ae:&Option<String>, multi_progress: &MultiProgress)
	-> dimse::error::Result<(ClientAssociation,ProgressBar)>
{
	let sp = multi_progress.add(ProgressBar::new_spinner());
	sp.enable_steady_tick(Duration::from_millis(500));
	sp.set_message(format!("Connecting to {}", addr));
	Client::new(addr, calling_ae.clone(), vec![]).await
		.map(|c|(c,sp))
}

async fn get_instances(study:&str, client:&mut ClientAssociation, sp:ProgressBar)
	-> dimse::error::Result<BTreeMap<String,InMemDicomObject>> {
	sp.set_message(format!("Scanning {}", client.peer_address().to_string()));
	let mut instances = BTreeMap::<_,_>::default();
	let mut found =client.find(RetrieveLevel::IMAGE,[
		InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, study),
		InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SERIES_DESCRIPTION, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SERIES_NUMBER, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
	]).await?;

	if let Some(first) = found.next().await.transpose()? {
		if let Some(hint) = found.size_hint().1{
			sp.set_style(ProgressStyle::with_template("[{msg}] {wide_bar} {pos}/{len}").unwrap());
			sp.set_length(hint as u64);
		} else {
			sp.set_style(ProgressStyle::with_template("{spinner} {msg} {pos} images found").unwrap())
		}
		while let Some(mut obj) = found.next().await.transpose()? {
			sp.inc(1);
			instances.insert(
				Taker::take(&mut obj, tags::SOP_INSTANCE_UID)?,
				obj,
			);
		}

	}
	sp.finish();
	Ok(instances)
}

#[tokio::main]
async fn main() -> dimse::error::Result<()>
{
	let cli=Cli::parse();
	tracing_subscriber::fmt().with_max_level(cli.log_level.0).init();
	let m = MultiProgress::new();

	let d_name = cli.dest.to_string();
	let (mut source, source_progress) = connect(cli.source, &cli.calling_ae, &m).await?;
	let (mut dest, dest_progress) = connect(cli.dest, &cli.calling_ae, &m).await?;

	let (in_source, in_dest) = try_join!(
		get_instances(cli.study.as_str(), &mut source, source_progress),
		get_instances(cli.study.as_str(),&mut dest, dest_progress)
	)?;

	let remain = in_source.into_iter()
		.filter(|(k,_)|!in_dest.contains_key(k))
		.collect::<Vec<_>>();

	if remain.is_empty(){
		info!("Nothing to copy");
		return Ok(());
	}

	if cli.probe {
		drop(m);
		let mut series = BTreeMap::new();
		for (k, mut v) in remain {
			let s_id:String = Taker::take(&mut v,tags::SERIES_INSTANCE_UID)?;
			series.entry(s_id).or_insert_with(Vec::new).push(v);
		}

		let mut table = Table::new();
		let reg = StandardSopClassDictionary::default();
		table
			.set_header(vec!["uid", "number", "description", "images", "abstract syntax"])
			.set_content_arrangement(ContentArrangement::Dynamic);

		for (s_uid, images) in series
		{
			let first = images.first().unwrap();
			let as_name = Taker::get_opt(first,tags::SOP_CLASS_UID)?
				.and_then(|e:String|reg.by_uid(e.as_str()))
				.map(|t|t.name())
				.unwrap_or("<UNKNOWN>");
			let row = [
				Cell::new::<String>(s_uid),
				Cell::new::<String>(Taker::get(first,tags::SERIES_NUMBER)?),
				Cell::new::<String>(Taker::get(first,tags::SERIES_DESCRIPTION)?),
				Cell::new(images.len()),
				Cell::new(as_name),
			];
			table.add_row(row);
		}
		println!("{}", table);

	} else {
		let sp = m.add(ProgressBar::new(remain.len() as u64));
		sp.set_style(ProgressStyle::with_template(["[{msg}] {wide_bar} {eta}"].concat().as_str()).unwrap());
		sp.set_message(format!("Copying {} images to {d_name}", remain.len()));

		for (uid, mut obj) in remain {
			let class: String = Taker::take(&mut obj, tags::SOP_CLASS_UID)?;
			let got = source.get_file(uid, class).await?;
			dest.store_fileobj(got, []).await?;
			sp.inc(1);
		}
		sp.finish();
	}

	Ok(())
}