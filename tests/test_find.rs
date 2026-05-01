use std::collections::HashMap;
use crate::common::dcm;
use dicom::core::{PrimitiveValue, VR};
use dicom::core::header::Header;
use dicom::object::mem::InMemElement;
use dicom_dictionary_std::{tags, uids};
use dimse::client::{Client, RetrieveLevel};
use dimse::error::Result;
use futures::TryStreamExt;
use tracing::error;
use crate::common::dcm::{diff, tagname};

mod common;

#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_find() -> Result<()>
{
	//tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();

	let mut series_a = dcm::synthesize_series(&dcm::UidSynthesizer::default(), 1, 1, 10);
	let series_b = dcm::synthesize_series(&dcm::UidSynthesizer::default(),2,1,10);

	for instance in &mut series_a {
		instance.update_value(tags::PATIENT_NAME,|v|*v="Finder, John".into());
	}


	let access = common::MultiFileAccess::new(series_a.iter().map(Clone::clone).chain(series_b.into_iter())).await?;

	let (addr, watcher,_) = common::init_server(access.clone()).await?;

	let mut client = Client::new(addr.into(),Some("TESTSCU"), [uids::STUDY_ROOT_QUERY_RETRIEVE_INFORMATION_MODEL_FIND])?;

	let found:Vec<_> = client.find(RetrieveLevel::IMAGE,[
		InMemElement::new(tags::PATIENT_NAME, VR::PN, "Finder*"),
		InMemElement::new(tags::SOP_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::SERIES_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::STUDY_INSTANCE_UID, VR::UI, PrimitiveValue::Empty),
		InMemElement::new(tags::STUDY_DATE, VR::DA, PrimitiveValue::Empty),
		InMemElement::new(tags::STUDY_TIME, VR::TM, PrimitiveValue::Empty),
	]).await?.try_collect().await?;
	assert_eq!(found.len(), 10);
	let mut found:HashMap<_,_> = found.into_iter()
		.map(|o|(o.element(tags::SOP_INSTANCE_UID).unwrap().to_str().unwrap().into_owned(),o)).collect();
	for instance in series_a {
		let uid = instance.element(tags::SOP_INSTANCE_UID).unwrap().to_str().unwrap().into_owned();
		for d in diff(found.remove(&uid).expect("Missing object in find result"),instance.into_inner()){
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

	}
	assert!(found.is_empty());
	drop(watcher);
	Ok(())
}


