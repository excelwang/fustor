use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataViewContractError {
    InvalidContract(&'static str),
    AlreadyExists(&'static str),
    NotFound(&'static str),
    HistoricalReplayIsReadOnly,
    ExecutionContextMismatch,
    SourceAuthorityMismatch,
    FreshnessRequiresRevalidation,
    Expired,
    Paused,
    Revoked,
    RequiresRevalidation,
    PolicyContextMismatch,
    GrantEpochMismatch,
    TrackedResultUnavailable,
    ObservationPathMismatch,
    ProjectionEvidenceRequired,
    ProjectionEvidenceUnexpected,
    UnsupportedSchema,
    LimitExceeded(&'static str),
    SequenceGap,
    WatermarkGap,
    BatchConflict,
    IdGenerationFailed,
    SerializationFailed,
    PersistenceFailed,
}

impl fmt::Display for DataViewContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContract(field) => write!(formatter, "invalid contract field: {field}"),
            Self::AlreadyExists(kind) => write!(formatter, "{kind} already exists"),
            Self::NotFound(kind) => write!(formatter, "{kind} not found"),
            Self::HistoricalReplayIsReadOnly => {
                formatter.write_str("historical replay is read-only")
            }
            Self::ExecutionContextMismatch => formatter.write_str("execution context mismatch"),
            Self::SourceAuthorityMismatch => formatter.write_str("source authority mismatch"),
            Self::FreshnessRequiresRevalidation => {
                formatter.write_str("freshness requires revalidation")
            }
            Self::Expired => formatter.write_str("contract expired"),
            Self::Paused => formatter.write_str("subscription paused"),
            Self::Revoked => formatter.write_str("subscription revoked"),
            Self::RequiresRevalidation => formatter.write_str("revalidation required"),
            Self::PolicyContextMismatch => formatter.write_str("policy context mismatch"),
            Self::GrantEpochMismatch => formatter.write_str("grant epoch mismatch"),
            Self::TrackedResultUnavailable => formatter.write_str("tracked result unavailable"),
            Self::ObservationPathMismatch => formatter.write_str("observation path mismatch"),
            Self::ProjectionEvidenceRequired => formatter.write_str("projection evidence required"),
            Self::ProjectionEvidenceUnexpected => {
                formatter.write_str("projection evidence unexpected")
            }
            Self::UnsupportedSchema => formatter.write_str("unsupported schema"),
            Self::LimitExceeded(limit) => write!(formatter, "limit exceeded: {limit}"),
            Self::SequenceGap => formatter.write_str("stream sequence gap"),
            Self::WatermarkGap => formatter.write_str("stream watermark gap"),
            Self::BatchConflict => formatter.write_str("stream batch conflicts with prior batch"),
            Self::IdGenerationFailed => formatter.write_str("opaque id generation failed"),
            Self::SerializationFailed => formatter.write_str("contract serialization failed"),
            Self::PersistenceFailed => formatter.write_str("DataView persistence failed"),
        }
    }
}

impl std::error::Error for DataViewContractError {}
