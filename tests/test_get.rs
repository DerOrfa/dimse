use dicom::core::header::Header;
use crate::common::dcm;
use dicom_dictionary_std::tags;
use dicom_dictionary_std::uids::{ENHANCED_MR_IMAGE_STORAGE};
use tracing::error;
use dimse::client::Client;
use dimse::Taker;
use dimse::error::Result;
use crate::common::dcm::{diff, tagname};

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



