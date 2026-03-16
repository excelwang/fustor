#![forbid(unsafe_code)]
//! Thin re-export surface for the extracted `fs-meta` workspace.
//!
//! The standalone repository still keeps the shared constitutional vocabulary
//! in `capanix-specs-vocabulary`; this root crate exists so repository-level
//! tests and scripts can depend on a stable workspace package.

pub mod evidence {
    pub use capanix_specs_vocabulary::evidence::*;
}

pub mod lifecycle {
    pub use capanix_specs_vocabulary::lifecycle::*;
}

pub mod relation {
    pub use capanix_specs_vocabulary::relation::*;
}

pub use evidence::{
    EvidenceField, ALL_CANONICAL_EVIDENCE_FIELDS, AUTHORITATIVE_REVISION, CUTOVER_AT,
    DRAIN_START_AT, ELIGIBLE_AT, GENERATION, LEASE_EPOCH, OBSERVATION_ELIGIBLE_AT,
    OBSERVATION_PLANE_EVIDENCE_FIELDS, OBSERVED_PROJECTION_REVISION, RELATION_KIND, RETIRE_AT,
    ROLLBACK_REASON, ROLLBACK_TARGET_GENERATION, UNIVERSAL_RELATION_EVIDENCE_FIELDS,
};
pub use lifecycle::{LifecyclePhase, SHARED_CHANGE_WINDOW_LIFECYCLE};
pub use relation::{RelationKind, UNIVERSAL_RELATION_KINDS};
