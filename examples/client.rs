use futures::Stream;
use std::collections::HashSet;
use std::fs::File;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, error, Level};
use dimse::client::{Client, ClientAssociation};
use clap::builder::{PossibleValue};
use clap::{Parser, Subcommand, ValueEnum};
use dicom::core::{PrimitiveValue, VR};
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::{tags, uids, StandardSopClassDictionary};
use comfy_table::{Cell, ContentArrangement, Table};
use comfy_table::Attribute::Dim;
use dicom::core::dictionary::{UidDictionary, UidDictionaryEntry};
use dicom::transfer_syntax::{TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_ul::AeAddr;
use futures::{StreamExt, TryStreamExt};
use glob::glob;
use dimse::error::DimseError;
use snafu::prelude::*;
use dimse::{RetrieveLevel, Taker};
use indicatif::{ProgressBar, ProgressStyle};
use dimse::association::{Release, SendPayload};
use dimse::definitions::RequestCode::C_GET;
use dimse::pending_receive::PendingStore;

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
#[command(author, version, about="A client tool (SCU) for basic operations on PACS servers (SCP)", long_about = None)]
struct Cli {
	/// logging level
	#[arg(long, default_value = Level::INFO.as_str())]
	log_level:LogLevel,
	/// Address of the PACS to connect to (AET@IP:PORT).
	#[arg(value_parser = socketaddr_value_parser)]
	addr:AeAddr<SocketAddr>,
	/// The calling AE title, default is ANY-SCU.
	#[arg(long)]
	calling_ae:Option<String>,
	#[command(subcommand)]
	command: Commands,
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

#[derive(Subcommand)]
pub enum Commands
{
	/// Find a measurement on the PACS.
	FindStudy {
		/// subject name to search for ("*" will list all studies)
		#[arg(default_value = "*")]
		name:String,
		/// date as string YYYYMMDD or *
		#[arg(default_value = "*")]
		date:String
	},
	/// Move a measurement to another PACS.
	Move {
		/// UID of the study
		uid:String,
		/// AET of the PACS to send to (must be known by the peer)
		aet:String,
	},
	/// List all instances in a measurement.
	ListInstances {
		/// UID of the study
		uid:String
	},
	/// Store one or more DICOM files.
	Store {
		/// glob or single file dicom file to upload
		pattern:Vec<String>
	},
	/// Download all instances of a measurement.
	GetStudy {
		/// UID of the measurement (use find-study to get that)
		uid:String,
		/// Output path to store the files (must exist)
		out:PathBuf
	},
	/// Probing which abstract syntaxes the server supports
	Probe {}
}

async fn connect(addr:AeAddr<SocketAddr>, calling_ae:Option<String>) -> dimse::error::Result<(ClientAssociation,ProgressBar)>
{
	let sp = ProgressBar::new_spinner();
	sp.enable_steady_tick(Duration::from_millis(500));
	sp.set_message(format!("Connecting to {}", addr));
	Client::new(addr, calling_ae, vec![]).await
		.map(|c|(c,sp))
}

#[tokio::main]
#[snafu::report]
async fn main() -> dimse::error::Result<()>
{
	let cli=Cli::parse();

	tracing_subscriber::fmt().with_max_level(cli.log_level.0).init();
	match cli.command {
		Commands::FindStudy { name, date } => {
			let (mut client, sp) = connect(cli.addr, cli.calling_ae).await?;
			sp.set_message(format!(r#"Looking up "{name}" from {date}"#));
			let found:Vec<_> =client.find(RetrieveLevel::STUDY, [
				InMemElement::new(tags::PATIENT_NAME, VR::PN, name),
				InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::STUDY_DATE, VR::DA, date),
				InMemElement::new(tags::STUDY_TIME, VR::TM, PrimitiveValue::Empty),
			]).await?.try_collect().await?;

			sp.finish_and_clear();

			let mut table = Table::new();
			table
				.set_header(vec!["Name", "Date", "Time", "UID", "Abstract Syntaxes", "Instances"])
				.set_content_arrangement(ContentArrangement::Dynamic);

			let reg = StandardSopClassDictionary::default();
			let progress = ProgressBar::new(found.len() as u64);
			progress.set_style(ProgressStyle::with_template("[Scanning studies] {wide_bar} {pos}/{len}").unwrap());
			for mut study in found
			{
				let instances:Vec<_> = client.collect_abstract_syntax([
						InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
						InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
						study.element(tags::STUDY_INSTANCE_UID)?.to_owned(),
					]).await?
					.map_ok(|a|reg.by_uid(a.as_str()).map_or("<unknown>",|a|a.name))
					.try_collect().await?;
				let len = instances.len();
				let syntaxes:Vec<_> = instances.into_iter().collect::<HashSet<_>>().into_iter().collect();

				let row = [tags::PATIENT_NAME, tags::STUDY_DATE, tags::STUDY_TIME, tags::STUDY_INSTANCE_UID].into_iter()
					.map(|t| study.take(t).map(|t|t.to_str().as_deref().unwrap_or("<INVALID ELEMENT>").to_string()))
					.map(|t|t.unwrap_or("<EMPTY>".into()))
					.map(Cell::new);
				let row = row.chain([Cell::new(syntaxes.join("\n")), Cell::new(len)]);
				table.add_row(row);
				progress.inc(1);
			}
			progress.finish_and_clear();
			if table.row_count() > 0 {
				println!("{}", table);
			}
		}
		Commands::ListInstances { uid } => {
			let (mut client, sp) = connect(cli.addr, cli.calling_ae).await?;
			sp.set_message(format!(r#"Looking up "{}""#, uid));
			let mut instances = vec![];
			let mut found =client.find(RetrieveLevel::IMAGE,[
				InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, uid),
				InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::CONTENT_TIME,VR::TM, PrimitiveValue::Empty),
			]).await?;
			sp.finish_and_clear();

			let progress = ProgressBar::no_length();
			progress.set_style(ProgressStyle::with_template("[Scanning studies] {wide_bar} {pos}/{len}").unwrap());
			while let Some(obj) = found.next().await.transpose()? {
				if let Some(s) = found.size_hint().1 {
					progress.set_length(progress.position() + s as u64)
				}
				progress.inc(1);
				instances.push(obj);
			}
			progress.finish_and_clear();
			instances.sort_by_key::<String,_>(|o|Taker::get(o,tags::CONTENT_TIME).unwrap());

			let mut table = Table::new();
			let reg = StandardSopClassDictionary::default();
			table
				.set_header(vec!["content time", "uid", "abstract syntax"])
				.set_content_arrangement(ContentArrangement::Dynamic);

			for mut obj in instances
			{
				let as_name= Taker::take_opt(&mut obj,tags::SOP_CLASS_UID)?
					.and_then(|e:String|reg.by_uid(e.as_str()))
					.map(|t|t.name())
					.unwrap_or("<UNKNOWN>");
				let row = [
					Cell::new::<String>(Taker::take(&mut obj,tags::CONTENT_TIME)?),
					Cell::new::<String>(Taker::take(&mut obj,tags::SOP_INSTANCE_UID)?),
					Cell::new(as_name),
				];
				table.add_row(row);
			}
			if table.row_count() > 0 {
				println!("{}", table);
			}
		}
		Commands::Move {uid,aet} => {
			let (mut client, sp) = connect(cli.addr, cli.calling_ae).await?;
			sp.finish_and_clear();

			let progress = ProgressBar::no_length();
			progress.set_style(ProgressStyle::with_template("[Initiating move] {wide_bar} {pos}/{len}").unwrap());
			let mut mover = client.move_study(uid, aet.clone()).await?;
			progress.set_style(ProgressStyle::with_template(["[Moving {len} files to ",&aet,"] {wide_bar} {eta}"].concat().as_str()).unwrap());
			while let Some(_) = mover.next().await.transpose()?{
				progress.inc(1);
				if let Some(s) = mover.to_do {
					progress.set_length(progress.position() + s as u64)
				}
			}
			drop(mover);
			progress.finish();
			client.release().await?;
		}
		Commands::Store { pattern } => {
			let (mut client, sp) = connect(cli.addr, cli.calling_ae).await?;
			sp.set_message(format!(r#"Storing "{}""#, pattern.join(",")));

			for p in pattern{
				let files = glob(p.as_ref()).map_err(|e|e.to_string())
					.with_whatever_context::<_,_,DimseError>(|e|format!("Globbing error {e}"))?.filter_map(|r|
				{
					match r {
						Ok(p) => if p.is_file() {Some(p)} else {None},
						Err(e) => {error!("Failed glob {e}");None}
					}
				});
				for file in files{
					client.store_file(&file,[]).await?;
					info!("Stored file {}", file.display());
				}
			}
			client.release().await?;
		}
		Commands::GetStudy { uid, out } => {
			if !out.is_dir(){
				eprintln!("{} is not a directory", out.display());
				exit(1);
			}
			let (mut client, sp) = connect(cli.addr, cli.calling_ae).await?;
			sp.set_message(format!(r#"Getting study "{}""#, uid));
			let mut sp = Some(sp);
			let mut response= client.get(uid,RetrieveLevel::STUDY).await?;

			let mut progress = None;
			while let Some(e) = response.next().await.transpose()?{
				if let Some(len) = response.size_hint().1{
					sp.take().map(|sp|sp.finish_and_clear());
					progress.get_or_insert({
						let progress = ProgressBar::new(len as u64);
						progress.set_style(ProgressStyle::with_template(["[Downloading {pos}/{len} files to ",out.to_string_lossy().as_ref(),"] {wide_bar} {eta}"].concat().as_str()).unwrap());
						progress
					}).inc(1);
				}
				let uid = e.element(tags::SOP_INSTANCE_UID)?.to_str().unwrap();
				let modality = e.element(tags::MODALITY)?.to_str().unwrap();
				e.write_all(File::create(out.join(format!("{modality}.{uid}.dcm")))?)?;
			}
			drop(response);
			client.release().await?;
		}
		Commands::Probe { } => {
			let mut table = Table::new();
			table
				.set_header(vec!["ID", "Abstract Syntax UID", "Abstract Syntax Name", "transfer syntax", "transfer syntax name"])
				.set_content_arrangement(ContentArrangement::Dynamic);

			let sp = ProgressBar::new_spinner();
			sp.enable_steady_tick(Duration::from_millis(500));
			sp.set_message(format!("Probing {}", cli.addr));
			for c in Client::probe(cli.addr,cli.calling_ae).await?
			{
				let entry = StandardSopClassDictionary::default().by_uid(&c.abstract_syntax)
					.map_or((false,"<unknown>"),|a|(a.retired,a.name));
				let syntax = TransferSyntaxRegistry::default().get(&c.transfer_syntax)
					.map_or("<unknown>",|s|s.name());

				let r = vec![Cell::new(c.id), Cell::new(c.abstract_syntax), Cell::new(entry.1), Cell::new(c.transfer_syntax), Cell::new(syntax)].into_iter();
				if entry.0 {
					table.add_row(r.map(|r|r.add_attribute(Dim)));
				} else {
					table.add_row(r);
				}
			}
			println!("{}", table);
		}
	}
	Ok(())
}
