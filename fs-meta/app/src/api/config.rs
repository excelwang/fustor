use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct BootstrapManagementConfig {
    pub username: String,
    pub password: String,
    pub uid: u32,
    pub gid: u32,
    pub home: String,
    pub shell: String,
}

impl BootstrapManagementConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.username.trim().is_empty() {
            return Err("api.auth.bootstrap_management.username must not be empty".into());
        }
        if self.password.is_empty() {
            return Err("api.auth.bootstrap_management.password must not be empty".into());
        }
        Ok(())
    }
}

pub type BootstrapAdminConfig = BootstrapManagementConfig;

#[derive(Debug, Clone)]
pub struct ApiAuthConfig {
    pub passwd_path: PathBuf,
    pub shadow_path: PathBuf,
    pub query_keys_path: PathBuf,
    pub session_ttl_secs: u64,
    pub management_group: String,
    pub bootstrap_management: Option<BootstrapManagementConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiListenerResource {
    pub resource_id: String,
    pub bind_addr: String,
}

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
        Self {
            enabled: true,
            facade_resource_id: "fs-meta-tcp-listener".to_string(),
            local_listener_resources: Vec::new(),
            auth: ApiAuthConfig::default(),
        }
    }
}

impl Default for ApiAuthConfig {
    fn default() -> Self {
        Self {
            passwd_path: PathBuf::from("./fs-meta.passwd"),
            shadow_path: PathBuf::from("./fs-meta.shadow"),
            query_keys_path: PathBuf::from("./fs-meta.query-keys.json"),
            session_ttl_secs: 3600,
            management_group: "fsmeta_management".to_string(),
            bootstrap_management: None,
        }
    }
}

impl ApiAuthConfig {
    pub fn ensure_materialized(&self) -> Result<(), String> {
        let needs_login_files = !self.passwd_path.exists() || !self.shadow_path.exists();
        let bootstrap = if needs_login_files {
            let Some(bootstrap) = &self.bootstrap_management else {
                return self.ensure_query_keys_materialized();
            };
            bootstrap.validate()?;
            Some(bootstrap)
        } else {
            None
        };
        if let Some(parent) = self.passwd_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create passwd directory {} failed: {e}", parent.display()))?;
        }
        if let Some(parent) = self.shadow_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("create shadow directory {} failed: {e}", parent.display()))?;
        }
        if let Some(parent) = self.query_keys_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "create query key directory {} failed: {e}",
                    parent.display()
                )
            })?;
        }
        if let Some(bootstrap) = bootstrap {
            let passwd = format!(
                "{}:{}:{}:{}:{}:{}:0\n",
                bootstrap.username,
                bootstrap.uid,
                bootstrap.gid,
                self.management_group,
                bootstrap.home,
                bootstrap.shell,
            );
            let shadow = format!("{}:plain${}:0\n", bootstrap.username, bootstrap.password);
            std::fs::write(&self.passwd_path, passwd).map_err(|e| {
                format!(
                    "write passwd file {} failed: {e}",
                    self.passwd_path.display()
                )
            })?;
            std::fs::write(&self.shadow_path, shadow).map_err(|e| {
                format!(
                    "write shadow file {} failed: {e}",
                    self.shadow_path.display()
                )
            })?;
        }
        self.ensure_query_keys_materialized()
    }

    fn ensure_query_keys_materialized(&self) -> Result<(), String> {
        if self.query_keys_path.exists() {
            return Ok(());
        }
        std::fs::write(&self.query_keys_path, "{\n  \"keys\": []\n}\n").map_err(|e| {
            format!(
                "write query keys file {} failed: {e}",
                self.query_keys_path.display()
            )
        })
    }
}

impl Default for BootstrapManagementConfig {
    fn default() -> Self {
        Self {
            username: "fsmeta-admin".to_string(),
            password: String::new(),
            uid: 1000,
            gid: 1000,
            home: "/home/fsmeta-admin".to_string(),
            shell: "/bin/bash".to_string(),
        }
    }
}

impl ApiConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Err(
                "api.enabled must be true; fs-meta management API boundary is mandatory".into(),
            );
        }
        if self.facade_resource_id.trim().is_empty() {
            return Err("api.facade_resource_id must not be empty when api is enabled".into());
        }
        if self.auth.session_ttl_secs == 0 {
            return Err("api.auth.session_ttl_secs must be > 0".into());
        }
        if self.auth.management_group.trim().is_empty() {
            return Err("api.auth.management_group must not be empty".into());
        }
        if self.auth.passwd_path.as_os_str().is_empty() {
            return Err("api.auth.passwd_path must not be empty".into());
        }
        if self.auth.shadow_path.as_os_str().is_empty() {
            return Err("api.auth.shadow_path must not be empty".into());
        }
        if self.auth.query_keys_path.as_os_str().is_empty() {
            return Err("api.auth.query_keys_path must not be empty".into());
        }
        if let Some(bootstrap) = &self.auth.bootstrap_management {
            bootstrap.validate()?;
        }
        Ok(())
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
