mod bridge;
mod control;
mod error;
mod transport;
mod typed_client;

#[doc(hidden)]
pub mod __private {
    pub use capanix_app_sdk::{CnxError, Result};
}

pub use control::{decode_control_payload, encode_control_frame};
pub use typed_client::{TypedWorkerClient, TypedWorkerRpc};

#[macro_export]
macro_rules! define_typed_worker_rpc {
    (
        $vis:vis struct $name:ident {
            request: $request:ty,
            response: $response:ty,
            encode: $encode:path,
            decode: $decode:path,
            error: $error_variant:path,
            unavailable: $label:expr $(,)?
        }
    ) => {
        $vis struct $name;

        impl $crate::TypedWorkerRpc for $name {
            type Request = $request;
            type Response = $response;

            fn encode_request(request: &Self::Request) -> $crate::__private::Result<Vec<u8>> {
                $encode(request)
            }

            fn decode_response(payload: &[u8]) -> $crate::__private::Result<Self::Response> {
                $decode(payload)
            }

            fn into_result(response: Self::Response) -> $crate::__private::Result<Self::Response> {
                match response {
                    $error_variant(message) => Err($crate::__private::CnxError::PeerError(message)),
                    other => Ok(other),
                }
            }

            fn unavailable_label() -> &'static str {
                $label
            }
        }
    };
}
