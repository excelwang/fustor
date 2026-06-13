use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use capanix_app_sdk::CnxError;
use serde::Serialize;

use super::types::StatusRepairLaneEvidence;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    repair_lanes: Option<Vec<StatusRepairLaneEvidence>>,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    pub repair_lanes: Option<Vec<StatusRepairLaneEvidence>>,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
            repair_lanes: None,
        }
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: msg.into(),
            repair_lanes: None,
        }
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: msg.into(),
            repair_lanes: None,
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
            repair_lanes: None,
        }
    }

    pub fn service_unavailable(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: msg.into(),
            repair_lanes: None,
        }
    }

    pub fn service_unavailable_with_repair_lanes(
        msg: impl Into<String>,
        repair_lanes: Vec<StatusRepairLaneEvidence>,
    ) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: msg.into(),
            repair_lanes: Some(repair_lanes),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
                repair_lanes: self.repair_lanes,
            }),
        )
            .into_response()
    }
}

impl From<CnxError> for ApiError {
    fn from(value: CnxError) -> Self {
        match value {
            CnxError::InvalidInput(msg) => Self::bad_request(msg),
            CnxError::NotSupported(msg) => Self::bad_request(msg),
            CnxError::NotReady(msg) => Self::service_unavailable(msg),
            other => Self::internal(other.to_string()),
        }
    }
}
