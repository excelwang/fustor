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
                let Some(mount_point) = get_str(object_descriptors, "mount_point") else {
                    continue;
                };
                let Some(fs_source) = get_str(object_descriptors, "fs_source") else {
                    continue;
                };
                let Some(fs_type) = get_str(object_descriptors, "fs_type") else {
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
                    mount_options: match object_descriptors.get("mount_options") {
                        Some(ConfigValue::Array(items)) => items
                            .iter()
                            .filter_map(|value| match value {
                                ConfigValue::String(s) => Some(s.clone()),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    },
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
