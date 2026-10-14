//! Explicit opt-in crash points used by the real-Engine recovery tests.
#[inline]
pub(crate) fn checkpoint(_name: &str) {
    #[cfg(feature = "subscription-fault-injection")]
    if std::env::var("DS_SUBSCRIPTION_FAULT").as_deref() == Ok(_name) {
        if let Ok(marker) = std::env::var("DS_SUBSCRIPTION_FAULT_MARKER") {
            std::fs::write(marker, _name).expect("write recovery test checkpoint");
        }
        std::process::abort();
    }
}

/// A bounded barrier for races while an Engine request is outside the actor lock.
#[inline]
pub(crate) async fn pause(_name: &str) {
    #[cfg(feature = "subscription-fault-injection")]
    if std::env::var("DS_SUBSCRIPTION_PAUSE_POINT").as_deref() == Ok(_name)
        && let Ok(marker) = std::env::var("DS_SUBSCRIPTION_PAUSE_MARKER")
    {
        std::fs::write(&marker, _name).expect("write recovery test barrier");
        let release = format!("{marker}.release");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !std::path::Path::new(&release).exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
    }
}
