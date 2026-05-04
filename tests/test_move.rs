use crate::common::dcm;
use dicom::core::VR;
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::{tags, uids};
use dicom_ul::FullAeAddr;
use dimse::association::SendPayload;
use dimse::client::Client;
use dimse::definitions::RequestCode::C_MOVE;
use dimse::error::Result;
use dimse::pending_receive::PendingReceive;
use dimse::{RetrieveLevel, Taker};
use futures::TryStreamExt;
use std::collections::HashMap;

mod common;

#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_move() -> Result<()>
{
	tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();

	let series = dcm::synthesize_series(&dcm::UidSynthesizer::default(),1,1,10);
	let series_uid:String = Taker::get(series.first().expect("Series must not be empty"), tags::SERIES_INSTANCE_UID)?;

	let filled = common::MultiFileAccess::new(series.clone()).await?;
	let empty = common::MultiFileAccess::new([]).await?;
	let series= series
		.into_iter().map(|o|Taker::get(&o,tags::SOP_INSTANCE_UID).map(|s|(s,o)))
		.collect::<Result<HashMap<String,_>>>()?;

	let (empty_srv, watcher1,_) = common::init_server("EMPTY",[] , empty.clone()).await?;
	let (filled_srv, watcher2,_) = common::init_server("FILLED",[FullAeAddr::new("EMPTY",empty_srv)] , filled.clone()).await?;

	let mut client = Client::new(filled_srv.into(),Some("TESTSCU"), [uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE]).await?;

	let req= client.new_request(C_MOVE, uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_MOVE ,vec![
		InMemElement::new(tags::MOVE_DESTINATION, VR::AE, "EMPTY")
	])?
		.with_identifier([
			InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,RetrieveLevel::SERIES.as_ref()),
			InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, series_uid),
		]);
	client.send_request(req).await?;
	let response:Vec<_> = PendingReceive::new(&mut client, C_MOVE).try_collect().await?;

	dbg!(response);
	drop(watcher1);
	drop(watcher2);
	Ok(())
}


