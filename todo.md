
- [ ] **Architecture**: Finalize gRPC transport implementation.
- [ ] **DevOps**: Expand CI/CD to include performance regression benchmarks.

- [ ] 能否合并 source-fs的pre-snapshot和snapshot的逻辑
- 分页返回大树
- [ ] **Architecture**: Revisit Multi-View design. It currently acts as a pseudo-ViewDriver but fails at ingestion. It fundamentally differs from standard Views and shouldn't be a pipe target.

## Centralized Agent Management (Control Plane)
- [ ] **Architecture**: Research Control/Data Plane Separation (Ref: `under-review/2026-02-12-architecture-split.md`)
- [ ] Implement "Configuration Signature" based Hot Reload (Prerequisite).
- [ ] Design Fusion Management API (Config + State).
- [ ] Implement `RemoteConfigProvider` in Agent (polling Fusion).
- [ ] Implement `SelfUpdater` in Agent (upgrade strategy).