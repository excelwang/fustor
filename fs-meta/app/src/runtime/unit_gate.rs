use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

use capanix_app_sdk::{CnxError, Result};
use capanix_runtime_entry_sdk::control::RuntimeBoundScope;

#[derive(Debug, Clone, Default)]
struct RouteControlState {
    generation: u64,
    active: bool,
    bound_scopes: Vec<RuntimeBoundScope>,
}

#[derive(Debug, Clone, Default)]
struct UnitControlState {
    routes: HashMap<String, RouteControlState>,
}

#[derive(Default)]
struct UnitControlGate {
    units: HashMap<String, UnitControlState>,
}

impl UnitControlGate {
    fn apply_activate(
        &mut self,
        unit_id: &str,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> bool {
        let entry = self.units.entry(unit_id.to_string()).or_default();
        let route = entry.routes.entry(route_key.to_string()).or_default();
        if generation < route.generation {
            return false;
        }
        route.generation = generation;
        route.active = true;
        route.bound_scopes = bound_scopes.to_vec();
        true
    }

    fn apply_deactivate(&mut self, unit_id: &str, route_key: &str, generation: u64) -> bool {
        let entry = self.units.entry(unit_id.to_string()).or_default();
        let route = entry.routes.entry(route_key.to_string()).or_default();
        if generation < route.generation {
            return false;
        }
        route.generation = generation;
        route.active = false;
        route.bound_scopes.clear();
        true
    }

    fn accepts_tick(&self, unit_id: &str, route_key: &str, generation: u64) -> bool {
        match self.units.get(unit_id) {
            Some(state) => match state.routes.get(route_key) {
                Some(route) => generation >= route.generation && route.active,
                None => state.routes.is_empty(),
            },
            None => true,
        }
    }

    fn sync_active_scopes(&mut self, unit_id: &str, bound_scopes: &[RuntimeBoundScope]) {
        let Some(state) = self.units.get_mut(unit_id) else {
            return;
        };
        for route in state.routes.values_mut().filter(|route| route.active) {
            route.bound_scopes = bound_scopes.to_vec();
        }
    }
}

/// Shared runtime unit control gate for fs-meta modules.
///
/// It provides one deterministic implementation of:
/// - unit allowlist contract
/// - generation fencing behavior
/// - activate/deactivate/tick acceptance rules
pub(crate) struct RuntimeUnitGate {
    app_label: &'static str,
    supported_units: BTreeSet<String>,
    runtime_managed: bool,
    gate: Mutex<UnitControlGate>,
}

impl RuntimeUnitGate {
    pub(crate) fn new(app_label: &'static str, supported_units: &[&str]) -> Self {
        Self::new_with_mode(app_label, supported_units, false)
    }

    pub(crate) fn new_runtime_managed(app_label: &'static str, supported_units: &[&str]) -> Self {
        Self::new_with_mode(app_label, supported_units, true)
    }

    fn new_with_mode(
        app_label: &'static str,
        supported_units: &[&str],
        runtime_managed: bool,
    ) -> Self {
        let supported_units = supported_units
            .iter()
            .map(|unit| unit.trim())
            .filter(|unit| !unit.is_empty())
            .map(|unit| unit.to_string())
            .collect::<BTreeSet<_>>();
        Self {
            app_label,
            supported_units,
            runtime_managed,
            gate: Mutex::new(UnitControlGate::default()),
        }
    }

    fn validate_runtime_unit(&self, unit_id: &str) -> Result<()> {
        if self.supported_units.contains(unit_id) {
            Ok(())
        } else {
            Err(CnxError::NotSupported(format!(
                "{}: unsupported unit_id '{}' in control envelope",
                self.app_label, unit_id
            )))
        }
    }

    pub(crate) fn apply_activate(
        &self,
        unit_id: &str,
        route_key: &str,
        generation: u64,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<bool> {
        self.validate_runtime_unit(unit_id)?;
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| CnxError::Internal("RuntimeUnitGate lock poisoned".into()))?;
        Ok(gate.apply_activate(unit_id, route_key, generation, bound_scopes))
    }

    pub(crate) fn apply_deactivate(
        &self,
        unit_id: &str,
        route_key: &str,
        generation: u64,
    ) -> Result<bool> {
        self.validate_runtime_unit(unit_id)?;
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| CnxError::Internal("RuntimeUnitGate lock poisoned".into()))?;
        Ok(gate.apply_deactivate(unit_id, route_key, generation))
    }

    pub(crate) fn accept_tick(
        &self,
        unit_id: &str,
        route_key: &str,
        generation: u64,
    ) -> Result<bool> {
        self.validate_runtime_unit(unit_id)?;
        let gate = self
            .gate
            .lock()
            .map_err(|_| CnxError::Internal("RuntimeUnitGate lock poisoned".into()))?;
        Ok(gate.accepts_tick(unit_id, route_key, generation))
    }

    pub(crate) fn sync_active_scopes(
        &self,
        unit_id: &str,
        bound_scopes: &[RuntimeBoundScope],
    ) -> Result<()> {
        self.validate_runtime_unit(unit_id)?;
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| CnxError::Internal("RuntimeUnitGate lock poisoned".into()))?;
        gate.sync_active_scopes(unit_id, bound_scopes);
        Ok(())
    }

    pub(crate) fn has_runtime_state(&self) -> bool {
        self.gate
            .lock()
            .map(|gate| self.runtime_managed || !gate.units.is_empty())
            .unwrap_or(false)
    }

    pub(crate) fn unit_state(
        &self,
        unit_id: &str,
    ) -> Result<Option<(bool, Vec<RuntimeBoundScope>)>> {
        self.validate_runtime_unit(unit_id)?;
        let gate = self
            .gate
            .lock()
            .map_err(|_| CnxError::Internal("RuntimeUnitGate lock poisoned".into()))?;
        Ok(gate.units.get(unit_id).cloned().map(|row| {
            let mut merged = BTreeMap::<String, BTreeSet<String>>::new();
            for route in row.routes.values().filter(|route| route.active) {
                for scope in &route.bound_scopes {
                    let entry = merged.entry(scope.scope_id.clone()).or_default();
                    for resource_id in &scope.resource_ids {
                        entry.insert(resource_id.clone());
                    }
                }
            }
            let active = !merged.is_empty();
            let bound_scopes = merged
                .into_iter()
                .map(|(scope_id, resource_ids)| RuntimeBoundScope {
                    scope_id,
                    resource_ids: resource_ids.into_iter().collect(),
                })
                .collect();
            (active, bound_scopes)
        }))
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self, unit_id: &str) -> Option<(u64, bool)> {
        self.gate
            .lock()
            .ok()
            .and_then(|gate| gate.units.get(unit_id).cloned())
            .map(|row| {
                let generation = row
                    .routes
                    .values()
                    .map(|route| route.generation)
                    .max()
                    .unwrap_or(0);
                (generation, row.routes.values().any(|route| route.active))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bound_scope(scope_id: &str, resource_id: &str) -> RuntimeBoundScope {
        RuntimeBoundScope {
            scope_id: scope_id.to_string(),
            resource_ids: vec![resource_id.to_string()],
        }
    }

    #[test]
    fn route_generations_merge_active_scopes_for_same_unit() {
        let gate = RuntimeUnitGate::new_runtime_managed("test-gate", &["runtime.exec.sink"]);

        assert!(
            gate.apply_activate(
                "runtime.exec.sink",
                "find:v1.find",
                100,
                &[bound_scope("nfs1", "node-a::nfs1")]
            )
            .expect("activate query route")
        );
        assert!(
            gate.apply_activate(
                "runtime.exec.sink",
                "fs-meta.events:v1",
                101,
                &[bound_scope("nfs2", "node-c::nfs2")]
            )
            .expect("activate stream route")
        );

        let state = gate
            .unit_state("runtime.exec.sink")
            .expect("unit_state")
            .expect("unit should exist");
        assert!(state.0, "unit should remain active");
        let scope_ids = state
            .1
            .into_iter()
            .map(|scope| scope.scope_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            scope_ids,
            BTreeSet::from(["nfs1".to_string(), "nfs2".to_string()])
        );
    }

    #[test]
    fn deactivate_only_removes_target_route_state() {
        let gate = RuntimeUnitGate::new_runtime_managed("test-gate", &["runtime.exec.sink"]);

        gate.apply_activate(
            "runtime.exec.sink",
            "find:v1.find",
            100,
            &[bound_scope("nfs1", "node-a::nfs1")],
        )
        .expect("activate query route");
        gate.apply_activate(
            "runtime.exec.sink",
            "fs-meta.events:v1",
            101,
            &[bound_scope("nfs2", "node-c::nfs2")],
        )
        .expect("activate stream route");
        assert!(
            gate.apply_deactivate("runtime.exec.sink", "find:v1.find", 102)
                .expect("deactivate query route")
        );

        let state = gate
            .unit_state("runtime.exec.sink")
            .expect("unit_state")
            .expect("unit should exist");
        assert!(state.0, "stream route should keep the unit active");
        let scopes = state.1;
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].scope_id, "nfs2");
    }

    #[test]
    fn tick_acceptance_is_route_scoped_after_activation() {
        let gate = RuntimeUnitGate::new_runtime_managed("test-gate", &["runtime.exec.sink"]);

        gate.apply_activate(
            "runtime.exec.sink",
            "find:v1.find",
            100,
            &[bound_scope("nfs1", "node-a::nfs1")],
        )
        .expect("activate query route");

        assert!(
            gate.accept_tick("runtime.exec.sink", "find:v1.find", 100)
                .expect("tick on active route should be accepted")
        );
        assert!(
            !gate
                .accept_tick("runtime.exec.sink", "fs-meta.events:v1", 101)
                .expect("tick on unknown route should be rejected")
        );
    }
}
