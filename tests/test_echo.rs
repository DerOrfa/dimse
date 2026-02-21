use dicom_dictionary_std::uids::VERIFICATION;
use dimse::association::SendPayload;
use dimse::definitions::RequestCode::C_ECHO;
use dimse::error::Result;

mod common;
#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_echo() -> Result<()>
{
	// tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
	let (addr, watcher,handle) = common::init_server(common::NullAccess).await?;

	assert!(!handle.is_finished()); // should not be finished

	let mut client = dimse::client::Client::new(addr.into(),Some("TEST_CLIENT"), [VERIFICATION])?;

	let req = client.new_request(C_ECHO,VERIFICATION, vec![])?;
	client.send_request(req).await?;
	client.receive_response(C_ECHO).await?;

	drop(watcher);
	handle.await.unwrap()?;
	Ok(())
}



