use std::collections::HashMap;
use dicom::core::header::Header;
use dicom::core::VR;
use dicom::object::mem::InMemElement;
use crate::common::dcm;
use dicom_dictionary_std::{tags, uids};
use dicom_dictionary_std::uids::{ENHANCED_MR_IMAGE_STORAGE};
use futures::TryStreamExt;
use tracing::error;
use dimse::association::SendPayload;
use dimse::client::{Client, RetrieveLevel};
use dimse::definitions::RequestCode::C_GET;
use dimse::Taker;
use dimse::error::Result;
use crate::common::dcm::{diff, tagname};
use dimse::pending_receive::PendingReceive;

mod common;
#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_get() -> Result<()>
{
	// tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();

	let obj = dcm::synthesize_dicom_obj(&dcm::UidSynthesizer::default(),1,1,1);
	let uid:String = Taker::get(&obj,tags::SOP_INSTANCE_UID)?;
	let access = common::MultiFileAccess::new([obj.clone()]).await?;

	let (addr, watcher,_) = common::init_server(access.clone()).await?;

	let mut client = Client::new(addr.into(),Some("TESTSCU"), [ENHANCED_MR_IMAGE_STORAGE])?;

	let req = client.get_file(uid).await?;

	let d = diff(obj.into_inner(),req.into_inner());
	for d in d.iter() {
		match d
		{
			(Some(e1),Some(e2)) =>
				error!("Sent {} differs from the reply: {:?}//{:?}",tagname(e1.tag()),e1.value(),e2.value()),
			(Some(e1),None) =>
				error!("Sent {} is missing in the reply", tagname(e1.tag())),
			_ =>
				error!("Unexpected {:?}//{:?}",d.0,d.1),
		}
	}
	assert!(d.is_empty());

	drop(watcher);
	Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_get_many() -> Result<()>
{
	// tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();

	let series = dcm::synthesize_series(&dcm::UidSynthesizer::default(),1,1,10);
	let series_uid:String = Taker::get(series.first().expect("Series must not be empty"), tags::SERIES_INSTANCE_UID)?;

	let access = common::MultiFileAccess::new(series.clone()).await?;
	let series= series
		.into_iter().map(|o|Taker::get(&o,tags::SOP_INSTANCE_UID).map(|s|(s,o)))
		.collect::<Result<HashMap<String,_>>>()?;

	let (addr, watcher,_) = common::init_server(access.clone()).await?;

	let mut client = Client::new(addr.into(),Some("TESTSCU"), [ENHANCED_MR_IMAGE_STORAGE])?;

	let req= client.new_request(C_GET, uids::ENHANCED_MR_IMAGE_STORAGE ,vec![])?
		.with_identifier([
			InMemElement::new(tags::QUERY_RETRIEVE_LEVEL,VR::CS,RetrieveLevel::SERIES.as_ref()),
			InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, series_uid),
		]);
	client.send_request(req).await?;
	let response:Vec<_> = PendingReceive::new(&mut client, C_GET).try_collect().await?;
	let response = response.into_iter().map(|o|Taker::get(&o,tags::SOP_INSTANCE_UID).map(|s|(s,o)))
			.collect::<Result<HashMap<String,_>>>()?;

	assert_eq!(response.keys().len(), series.keys().len());

	drop(watcher);
	Ok(())
}


