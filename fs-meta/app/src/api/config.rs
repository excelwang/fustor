pub use fs_meta::api::config::{
    ApiAuthConfig, ApiListenerResource, BootstrapAdminConfig, BootstrapManagementConfig,
};
use fs_meta::api::config::ApiConfig as ProductApiConfig;

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub enabled: bool,
    pub facade_resource_id: String,
    pub local_listener_resources: Vec<ApiListenerResource>,
    pub auth: ApiAuthConfig,
}

#[derive(Debug, Clone)]
pub struct ResolvedApiConfig {
    pub bind_addr: String,
    pub auth: ApiAuthConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self::from_product_config(ProductApiConfig::default(), Vec::new())
    }
}

impl From<ProductApiConfig> for ApiConfig {
    fn from(value: ProductApiConfig) -> Self {
        Self::from_product_config(value, Vec::new())
    }
}

impl ApiConfig {
    pub fn from_product_config(
        product: ProductApiConfig,
        local_listener_resources: Vec<ApiListenerResource>,
    ) -> Self {
        Self {
            enabled: product.enabled,
            facade_resource_id: product.facade_resource_id,
            local_listener_resources,
            auth: product.auth,
        }
    }

    pub(crate) fn product_config(&self) -> ProductApiConfig {
        ProductApiConfig {
            enabled: self.enabled,
            facade_resource_id: self.facade_resource_id.clone(),
            auth: self.auth.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.product_config().validate()
    }

    pub fn resolve_for_candidate_ids(
        &self,
        candidate_resource_ids: &[String],
    ) -> Option<ResolvedApiConfig> {
        let selected = candidate_resource_ids.iter().find_map(|resource_id| {
            self.local_listener_resources
                .iter()
                .find(|row| row.resource_id == *resource_id)
        })?;
        Some(ResolvedApiConfig {
            bind_addr: selected.bind_addr.clone(),
            auth: self.auth.clone(),
        })
    }
}
