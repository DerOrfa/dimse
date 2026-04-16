use std::collections::HashSet;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{info, debug, error, Level};
use dimse::client::Client;
use clap::builder::{PossibleValue};
use clap::{Parser, Subcommand, ValueEnum};
use dicom::core::{PrimitiveValue, VR};
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::{tags, StandardSopClassDictionary};
use comfy_table::{Cell, ContentArrangement, Table};
use comfy_table::Attribute::Dim;
use dicom::core::dictionary::{UidDictionary, UidDictionaryEntry};
use dicom::transfer_syntax::{TransferSyntaxIndex, TransferSyntaxRegistry};
use dicom_ul::AeAddr;
use futures::TryStreamExt;
use glob::glob;
use dimse::error::DimseError;
use snafu::prelude::*;
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

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
	/// logging level
	#[arg(long, default_value = Level::INFO.as_str())]
	log_level:LogLevel,
	/// address of the PACS to connect to
	addr:AeAddr<SocketAddr>,
	/// the calling AE title, default is ANY-SCU
	#[arg(long)]
	calling_ae:Option<String>,
	#[command(subcommand)]
	command: Commands,

}

#[derive(Subcommand)]
pub enum Commands
{
	/// finding an instance on the server
	FindStudy {
		/// subject name to search for ("*" will list all studies)
		name:String
	},
	Move {
		/// UID of the study
		uid:String,
		aet:String,
	},
	ListInstances {
		/// UID of the study
		uid:String
	},
	/// store one or more DICOM files
	Store {
		/// glob or single file dicom file to upload
		pattern:Vec<String>
	},
	Get {
		/// glob or single file dicom file to upload
		uid:String,
		out:PathBuf
	},
	/// Probing which abstract syntaxes the server supports
	Probe {}
}

#[tokio::main]
#[snafu::report]
async fn main() -> dimse::error::Result<()>
{
	let cli=Cli::parse();

	tracing_subscriber::fmt().with_max_level(cli.log_level.0).init();
	match cli.command {
		Commands::FindStudy { name } => {
			let mut client = Client::new(cli.addr, cli.calling_ae, vec![])?;

			let found:Vec<_> =client.find(dimse::client::RetrieveLevel::STUDY, [
				InMemElement::new(tags::PATIENT_NAME, VR::PN, name),
				InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::STUDY_DATE, VR::DA, PrimitiveValue::Empty),
				InMemElement::new(tags::STUDY_TIME, VR::TM, PrimitiveValue::Empty),
			]).await?.try_collect().await?;

			let mut table = Table::new();
			table
				.set_header(vec!["Name", "Date", "Time", "UID", "Abstract Syntaxes"])
				.set_content_arrangement(ContentArrangement::Dynamic);

			let reg = StandardSopClassDictionary::default();
			for mut obj in found
			{
				let abstract_syntax:Vec<_> = client.collect_abstract_syntax([
						obj.element(tags::STUDY_INSTANCE_UID)?.to_owned(),
					]).await?
					.map_ok(|a|reg.by_uid(a.as_str()).map_or("<unknown>",|a|a.name))
					.try_collect::<HashSet<_>>().await?//make entries unique
					.into_iter().collect();

				let row = [tags::PATIENT_NAME, tags::STUDY_DATE, tags::STUDY_TIME, tags::STUDY_INSTANCE_UID].into_iter()
					.map(|t|obj.take(t).map(|t|t.to_str().as_deref().unwrap_or("<INVALID ELEMENT>").to_string()))
					.map(|t|t.unwrap_or("<EMPTY>".into()))
					.map(Cell::new);
				let row = row.chain([Cell::new(abstract_syntax.join("\n"))]);
				table.add_row(row);
			}
			if table.row_count() > 0 {
				println!("{}", table);
			}
		}
		Commands::ListInstances { uid } => {
			let mut client = Client::new(cli.addr, cli.calling_ae,vec![])?;

			let mut found:Vec<_> =client.find(dimse::client::RetrieveLevel::IMAGE,[
				InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, uid),
				InMemElement::new(tags::SOP_CLASS_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
				InMemElement::new(tags::CONTENT_TIME,VR::TM, PrimitiveValue::Empty),
			]).await?.try_collect().await?;
			found.sort_by_key::<String,_>(|o|Taker::get(o,tags::CONTENT_TIME).unwrap());


			let mut table = Table::new();
			let reg = StandardSopClassDictionary::default();
			table
				.set_header(vec!["content time", "uid", "abstract syntax"])
				.set_content_arrangement(ContentArrangement::Dynamic);

			for mut obj in found.into_iter()
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
			let mut client = Client::new(cli.addr, cli.calling_ae, vec![])?;
			client.move_study(uid,aet).await?;
		}
		Commands::Store { pattern } => {
			let mut client = Client::new(cli.addr, cli.calling_ae, vec![])?;

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
					client.store_file(&file).await?;
					info!("Stored file {}", file.display());
				}
			}

		}
		Commands::Get { uid, out } => {
			let mut client = Client::new(cli.addr, cli.calling_ae, vec![])?;
			client.get_file(uid).await?.write_all(File::create(out)?)?;
		}
		Commands::Probe { } => {
			let mut table = Table::new();
			table
				.set_header(vec!["ID", "Abstract Syntax UID", "Abstract Syntax Name", "transfer syntax", "transfer syntax name"])
				.set_content_arrangement(ContentArrangement::Dynamic);
			for c in Client::probe(cli.addr,cli.calling_ae)?
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
