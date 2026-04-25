#[path = "common.rs"]
mod common;
#[path = "path_support.rs"]
mod path_support;

#[test]
fn empty_roots_online_roots_apply_distributed_force_find_e2e() {
    common::assert_empty_roots_online_roots_apply_distributed_force_find_e2e();
}
