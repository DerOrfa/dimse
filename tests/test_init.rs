use dicom_dictionary_std::uids::VERIFICATION;
use dicom_ul::ClientAssociationOptions;
use dimse::error::Result;

mod common;

#[tokio::test(flavor = "multi_thread")]
#[snafu::report]
async fn test_server_init() -> Result<()>
{
//    tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).init();
    let (addr, watcher,handle) = common::init_server(common::NullAccess).await?;

    assert!(!handle.is_finished());// as long as watcher is open, server should be running

    let association = ClientAssociationOptions::new()
        .called_ae_title("TEST")
        .with_abstract_syntax(VERIFICATION)
        .establish(addr)?;
    association.release()?;

    drop(watcher);
    handle.await.unwrap()?; // watcher is dropped, so server should close
    Ok(())
}

