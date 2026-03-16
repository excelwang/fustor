//! Additional traceability shells for refined fs-meta domain contracts.

#[test]
// @verify_spec("CONTRACTS.KERNEL_RELATION.THIN_RUNTIME_ABI_CONSUMPTION", mode="system")
fn thin_runtime_abi_consumption() {
    let l1 = include_str!("../../specs/L1-CONTRACTS.md");
    assert!(l1.contains("THIN_RUNTIME_ABI_CONSUMPTION"));
}

#[test]
// @verify_spec("CONTRACTS.KERNEL_RELATION.APP_OWNS_OPAQUE_PORT_MEANING", mode="system")
fn app_owns_opaque_port_meaning() {
    let l1 = include_str!("../../specs/L1-CONTRACTS.md");
    assert!(l1.contains("APP_OWNS_OPAQUE_PORT_MEANING"));
}

#[test]
// @verify_spec("CONTRACTS.API_BOUNDARY.RESOURCE_SCOPED_DOMAIN_HTTP_FACADE", mode="system")
fn ingress_scoped_domain_http_facade() {
    let l1 = include_str!("../../specs/L1-CONTRACTS.md");
    let api = include_str!("../../specs/L3-RUNTIME/API_HTTP.md");
    assert!(l1.contains("RESOURCE_SCOPED_DOMAIN_HTTP_FACADE"));
    assert!(
        api.contains("ingress-resource-driven one-cardinality domain facade")
            || api.contains("resource-scoped one-cardinality domain facade")
    );
}
