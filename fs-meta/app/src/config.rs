use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use capanix_app_sdk::Result;
use capanix_app_sdk::runtime::ConfigValue;
use fs_meta::FSMetaConfig as ProductFSMetaConfig;

use crate::api::config::{ApiConfig, ApiListenerResource};
use crate::source::config::{GrantedMountRoot, SourceConfig};

#[derive(Clone, Debug, Default)]
pub struct FSMetaRuntimeInputs {
    pub host_object_grants: Vec<GrantedMountRoot>,
    pub local_listener_resources: Vec<ApiListenerResource>,
}

#[derive(Clone, Debug)]
pub struct FSMetaConfig {
    pub source: SourceConfig,
    pub api: ApiConfig,
}

impl Default for FSMetaConfig {
    fn default() -> Self {
        Self::from_product_config(
            ProductFSMetaConfig::default(),
            FSMetaRuntimeInputs::default(),
        )
    }
}

impl From<ProductFSMetaConfig> for FSMetaConfig {
    fn from(value: ProductFSMetaConfig) -> Self {
        Self::from_product_config(value, FSMetaRuntimeInputs::default())
    }
}

impl FSMetaRuntimeInputs {
    pub fn from_runtime_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Self {
        fn get_str<'a>(row: &'a HashMap<String, ConfigValue>, key: &str) -> Option<&'a str> {
            match row.get(key) {
                Some(ConfigValue::String(s)) => Some(s.as_str()),
                _ => None,
            }
        }

        fn get_map<'a>(
            row: &'a HashMap<String, ConfigValue>,
            key: &str,
        ) -> Option<&'a HashMap<String, ConfigValue>> {
            match row.get(key) {
                Some(ConfigValue::Map(map)) => Some(map),
                _ => None,
            }
        }

        fn get_string_array(row: &HashMap<String, ConfigValue>, key: &str) -> Vec<String> {
            match row.get(key) {
                Some(ConfigValue::Array(items)) => items
                    .iter()
                    .filter_map(|value| match value {
                        ConfigValue::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            }
        }

        fn descriptor_attr<'a>(
            object_descriptors: &'a HashMap<String, ConfigValue>,
            key: &str,
        ) -> Option<&'a str> {
            get_map(object_descriptors, "attributes").and_then(|attrs| get_str(attrs, key))
        }

        fn descriptor_str<'a>(
            object_descriptors: &'a HashMap<String, ConfigValue>,
            legacy_key: &str,
            attr_key: &str,
        ) -> Option<&'a str> {
            get_str(object_descriptors, legacy_key)
                .or_else(|| descriptor_attr(object_descriptors, attr_key))
        }

        fn descriptor_mount_options(
            object_descriptors: &HashMap<String, ConfigValue>,
        ) -> Vec<String> {
            let legacy = get_string_array(object_descriptors, "mount_options");
            if !legacy.is_empty() {
                return legacy;
            }
            descriptor_attr(object_descriptors, "fs.mount_options_json")
                .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
                .unwrap_or_default()
        }

        fn local_announced_ingress_resources(
            cfg: &HashMap<String, ConfigValue>,
        ) -> Vec<ApiListenerResource> {
            let Some(runtime) = get_map(cfg, "__cnx_runtime") else {
                return Vec::new();
            };
            let Some(local_host_ref) = get_str(runtime, "local_host_ref") else {
                return Vec::new();
            };
            let local_host_ref = local_host_ref.trim();
            if local_host_ref.is_empty() {
                return Vec::new();
            }
            let Some(ConfigValue::Array(resources)) = runtime.get("announced_resources") else {
                return Vec::new();
            };
            let mut local = Vec::new();
            for resource in resources {
                let ConfigValue::Map(row) = resource else {
                    continue;
                };
                let Some(resource_kind) = get_str(row, "resource_kind") else {
                    continue;
                };
                if resource_kind.trim() != "tcp_listener" {
                    continue;
                }
                let Some(resource_id) = get_str(row, "resource_id") else {
                    continue;
                };
                let Some(node_id) = get_str(row, "node_id") else {
                    continue;
                };
                if node_id.trim() != local_host_ref {
                    continue;
                }
                let Some(bind_addr) = get_str(row, "bind_addr") else {
                    continue;
                };
                if resource_id.trim().is_empty() || bind_addr.trim().is_empty() {
                    continue;
                }
                local.push(ApiListenerResource {
                    resource_id: resource_id.to_string(),
                    bind_addr: bind_addr.to_string(),
                });
            }
            local
        }

        fn runtime_host_object_grants(cfg: &HashMap<String, ConfigValue>) -> Vec<GrantedMountRoot> {
            let Some(runtime) = get_map(cfg, "__cnx_runtime") else {
                return Vec::new();
            };
            let Some(ConfigValue::Array(grants)) = runtime.get("host_object_grants") else {
                return Vec::new();
            };
            let mut out = Vec::new();
            for item in grants {
                let ConfigValue::Map(row) = item else {
                    continue;
                };
                let Some(object_ref) = get_str(row, "object_ref") else {
                    continue;
                };
                let Some(host_ref) = get_str(row, "host_ref") else {
                    continue;
                };
                let Some(object_descriptors) = get_map(row, "object_descriptors") else {
                    continue;
                };
                let Some(host_descriptors) = get_map(row, "host_descriptors") else {
                    continue;
                };
                let Some(mount_point) =
                    descriptor_str(object_descriptors, "mount_point", "fs.mount_point")
                else {
                    continue;
                };
                let Some(fs_source) = descriptor_str(object_descriptors, "fs_source", "fs.source")
                else {
                    continue;
                };
                let Some(fs_type) = descriptor_str(object_descriptors, "fs_type", "fs.type")
                    .or_else(|| get_str(object_descriptors, "descriptor_class"))
                else {
                    continue;
                };
                let Some(host_ip) = get_str(host_descriptors, "host_ip") else {
                    continue;
                };
                out.push(GrantedMountRoot {
                    object_ref: object_ref.to_string(),
                    interfaces: match row.get("interfaces") {
                        Some(ConfigValue::Array(items)) => items
                            .iter()
                            .filter_map(|value| match value {
                                ConfigValue::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    },
                    host_ref: host_ref.to_string(),
                    host_ip: host_ip.to_string(),
                    host_name: get_str(host_descriptors, "host_name").map(str::to_string),
                    site: get_str(host_descriptors, "site").map(str::to_string),
                    zone: get_str(host_descriptors, "zone").map(str::to_string),
                    host_labels: match host_descriptors.get("host_labels") {
                        Some(ConfigValue::Map(labels)) => labels
                            .iter()
                            .filter_map(|(k, v)| match v {
                                ConfigValue::String(s) => Some((k.clone(), s.clone())),
                                _ => None,
                            })
                            .collect::<BTreeMap<_, _>>(),
                        _ => BTreeMap::new(),
                    },
                    mount_point: PathBuf::from(mount_point),
                    fs_source: fs_source.to_string(),
                    fs_type: fs_type.to_string(),
                    mount_options: descriptor_mount_options(object_descriptors),
                    active: match row.get("grant_state").or_else(|| row.get("state")) {
                        Some(ConfigValue::String(v)) => v.trim() == "active",
                        _ => true,
                    },
                });
            }
            out
        }

        Self {
            host_object_grants: runtime_host_object_grants(cfg),
            local_listener_resources: local_announced_ingress_resources(cfg),
        }
    }
}

impl FSMetaConfig {
    pub fn from_product_config(product: ProductFSMetaConfig, runtime: FSMetaRuntimeInputs) -> Self {
        Self {
            source: SourceConfig::from_product_config(product.source, runtime.host_object_grants),
            api: ApiConfig::from_product_config(product.api, runtime.local_listener_resources),
        }
    }

    pub fn from_runtime_manifest_config(cfg: &HashMap<String, ConfigValue>) -> Result<Self> {
        let product = ProductFSMetaConfig::from_product_manifest_config(cfg)?;
        let runtime = FSMetaRuntimeInputs::from_runtime_manifest_config(cfg);
        Ok(Self::from_product_config(product, runtime))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string(value: &str) -> ConfigValue {
        ConfigValue::String(value.to_string())
    }

    #[test]
    fn runtime_inputs_parse_neutral_host_object_descriptor_attributes() {
        let cfg = HashMap::from([(
            "__cnx_runtime".to_string(),
            ConfigValue::Map(HashMap::from([(
                "host_object_grants".to_string(),
                ConfigValue::Array(vec![ConfigValue::Map(HashMap::from([
                    ("object_ref".to_string(), string("node-a::nfs1")),
                    (
                        "interfaces".to_string(),
                        ConfigValue::Array(vec![string("host-object-access")]),
                    ),
                    ("host_ref".to_string(), string("node-a")),
                    (
                        "host_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([
                            ("host_ip".to_string(), string("10.0.0.11")),
                            ("host_name".to_string(), string("node-a")),
                        ])),
                    ),
                    (
                        "object_descriptors".to_string(),
                        ConfigValue::Map(HashMap::from([
                            ("descriptor_class".to_string(), string("fs")),
                            (
                                "attributes".to_string(),
                                ConfigValue::Map(HashMap::from([
                                    ("fs.mount_point".to_string(), string("/mnt/nfs1")),
                                    ("fs.source".to_string(), string("server:/export/nfs1")),
                                    ("fs.type".to_string(), string("nfs")),
                                    (
                                        "fs.mount_options_json".to_string(),
                                        string("[\"ro\",\"vers=4\"]"),
                                    ),
                                ])),
                            ),
                        ])),
                    ),
                    ("grant_state".to_string(), string("active")),
                ]))]),
            )])),
        )]);

        let inputs = FSMetaRuntimeInputs::from_runtime_manifest_config(&cfg);

        assert_eq!(inputs.host_object_grants.len(), 1);
        let grant = &inputs.host_object_grants[0];
        assert_eq!(grant.object_ref, "node-a::nfs1");
        assert_eq!(grant.host_ref, "node-a");
        assert_eq!(grant.host_ip, "10.0.0.11");
        assert_eq!(grant.mount_point, PathBuf::from("/mnt/nfs1"));
        assert_eq!(grant.fs_source, "server:/export/nfs1");
        assert_eq!(grant.fs_type, "nfs");
        assert_eq!(grant.mount_options, vec!["ro", "vers=4"]);
        assert!(grant.active);
    }
}
