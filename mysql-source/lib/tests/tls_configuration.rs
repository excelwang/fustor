// ASSERTION INTENT (Black-box tests — public traits and APIs only). Do not introduce white-box testing logic or internal workarounds.
// Installed trust is configuration, not registration-time provider readiness.
use std::collections::HashMap;

use capanix_app_sdk::runtime::ConfigValue;
use mysql_source::{MysqlSourceRuntimeConfig, MysqlTlsConfig};

fn string(value: &str) -> ConfigValue {
    ConfigValue::String(value.to_owned())
}

fn map(values: &[(&str, ConfigValue)]) -> ConfigValue {
    ConfigValue::Map(
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect(),
    )
}

fn configuration(tls: Option<ConfigValue>, legacy: bool) -> HashMap<String, ConfigValue> {
    let mut endpoint = HashMap::from([
        ("object_ref".into(), string("secure-mysql")),
        (
            "endpoint_uri".into(),
            string(if legacy {
                "mysql://old-db:3306/archive?ssl=true"
            } else {
                "mysql://old-db:3306/archive"
            }),
        ),
    ]);
    if let Some(tls) = tls {
        endpoint.insert("tls".into(), tls);
    }
    HashMap::from([
        (
            "endpoints".into(),
            ConfigValue::Array(vec![ConfigValue::Map(endpoint)]),
        ),
        (
            "__cnx_runtime".into(),
            map(&[(
                "resource_grants",
                ConfigValue::Array(vec![map(&[
                    ("resource_kind", string("mysql")),
                    ("object_ref", string("secure-mysql")),
                    ("endpoint_uri", string("mysql://new-db:3306/archive")),
                    ("grant_state", string("active")),
                    ("grant_epoch", ConfigValue::Int(7)),
                    ("interfaces", ConfigValue::Array(vec![string("read")])),
                ])]),
            )]),
        ),
    ])
}

// @verify_spec("CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION", mode="system")
#[test]
fn required_tls_and_installed_ca_survive_native_endpoint_rebinding_without_io() {
    let tls = map(&[("ca_file", string("/nonexistent-installed-ca.pem"))]);
    let config =
        MysqlSourceRuntimeConfig::from_manifest_config(&configuration(Some(tls), false)).unwrap();
    let resolved = config.endpoint_for("secure-mysql", None, None).unwrap();
    assert_eq!(resolved.endpoint_uri, "mysql://new-db:3306/archive");
    assert_eq!(
        resolved.tls,
        Some(MysqlTlsConfig {
            ca_file: Some("/nonexistent-installed-ca.pem".into())
        })
    );
    assert_eq!(resolved.grant_epoch, Some(7));
}

// @verify_spec("CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION", mode="system")
#[test]
fn legacy_tls_cannot_be_downgraded_by_an_endpoint_override() {
    let config =
        MysqlSourceRuntimeConfig::from_manifest_config(&configuration(None, true)).unwrap();
    assert_eq!(
        config.endpoint_for("secure-mysql", None, None).unwrap().tls,
        Some(MysqlTlsConfig::default())
    );
    let default =
        MysqlSourceRuntimeConfig::from_manifest_config(&configuration(None, false)).unwrap();
    assert_eq!(
        default
            .endpoint_for("secure-mysql", None, None)
            .unwrap()
            .tls,
        None
    );
    let required =
        MysqlSourceRuntimeConfig::from_manifest_config(&configuration(Some(map(&[])), false))
            .unwrap();
    assert_eq!(
        required
            .endpoint_for("secure-mysql", None, None)
            .unwrap()
            .tls,
        Some(MysqlTlsConfig::default())
    );
}

// @verify_spec("CONTRACTS.SOURCE.SOURCE_CREDENTIAL_AND_SCOPE_ISOLATION", mode="system")
#[test]
fn malformed_trust_paths_and_insecure_overrides_are_rejected_without_io() {
    let invalid = [
        ConfigValue::Bool(false),
        string("disabled"),
        map(&[("ca_file", string(""))]),
        map(&[("ca_file", string("relative.pem"))]),
        map(&[("ca_file", string("/private/../other.pem"))]),
        map(&[("ca_file", string("/private/invalid\n.pem"))]),
        map(&[("ca_file", ConfigValue::Int(1))]),
        map(&[("accept_invalid_certs", ConfigValue::Bool(true))]),
        map(&[("skip_domain_validation", ConfigValue::Bool(true))]),
    ];
    for tls in invalid {
        assert!(
            MysqlSourceRuntimeConfig::from_manifest_config(&configuration(Some(tls), false))
                .is_err()
        );
    }
}
