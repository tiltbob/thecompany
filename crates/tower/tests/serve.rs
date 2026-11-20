#![cfg(test)]
//! A production tower started from a config file, as `tower serve --config <PATH>` starts it:
//! through `config::load` and `Tower::spawn`, in-process (no `tower` process is started).
//!
//! Everything binds to 127.0.0.1 with OS-chosen ports, the certificate is made at run time, and
//! every id is generated at run time.

mod common;

use common::{DENY_REASON, Station, bounded, connected, denial, online};
use iroh::SecretKey;
use iroh::tls::CaTlsConfig;
use numbers_tower::{Tower, config};

#[tokio::test]
async fn a_tower_from_a_config_file_admits_the_listed_station_and_denies_the_other() {
    bounded(async {
        let dir = tempfile::tempdir().unwrap();
        let generated = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).unwrap();
        std::fs::create_dir(dir.path().join("tls")).unwrap();
        std::fs::write(dir.path().join("tls/cert.pem"), generated.cert.pem()).unwrap();
        std::fs::write(
            dir.path().join("tls/key.pem"),
            generated.signing_key.serialize_pem(),
        )
        .unwrap();
        let (listed_key, unlisted_key) = (SecretKey::generate(), SecretKey::generate());
        std::fs::write(
            dir.path().join("allow.txt"),
            format!("# the one listed station\n{}\n", listed_key.public()),
        )
        .unwrap();
        let config_path = dir.path().join("tower.toml");
        std::fs::write(
            &config_path,
            "hostname = \"127.0.0.1\"\n\
             http_bind = \"127.0.0.1:0\"\n\
             https_bind = \"127.0.0.1:0\"\n\
             qad_bind = \"127.0.0.1:0\"\n\
             allow_file = \"allow.txt\"\n\
             [tls]\ncert = \"tls/cert.pem\"\nkey = \"tls/key.pem\"\n\
             [rate]\nbytes_per_second = 1048576\n",
        )
        .unwrap();

        let tower = Tower::spawn(config::load(&config_path).unwrap())
            .await
            .unwrap_or_else(|error| panic!("start tower: {error}"));
        let url = tower
            .url()
            .parse()
            .unwrap_or_else(|_| panic!("parse tower URL"));
        assert!(tower.qad_port().is_some());
        let trusting = || Station {
            roots: Some(CaTlsConfig::custom_roots([generated.cert.der().clone()])),
            ..Station::relay_only(&url)
        };
        let listed = trusting().bind(listed_key).await;
        let unlisted = trusting().bind(unlisted_key).await;

        let (reason, ()) = tokio::join!(denial(&unlisted), online(&listed));
        assert_eq!(reason, DENY_REASON);
        assert!(connected(&listed) && !connected(&unlisted));
        assert!(tower.stats().denied >= 1);

        listed.close().await;
        unlisted.close().await;
        assert!(tower.shutdown().await.is_ok());
    })
    .await;
}
