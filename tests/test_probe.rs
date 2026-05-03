mod common;

use dimse::error::Result;

#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_server_probe() -> Result<()>
{
//	tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
	let (addr, watcher,handle) = common::init_server("TEST_SCP",[] , common::NullAccess).await?;

	assert!(!handle.is_finished()); // should not be finished

	let probes = dimse::client::Client::probe(addr.into(),Some("TEST_CLIENT"))?;

	assert!(!probes.is_empty());
	drop(watcher);
	Ok(())
}



