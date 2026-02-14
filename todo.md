
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
- [ ] **Architecture**: Generalize `on_session_created` pattern to Sender side for full negotiation decoupling.
- [ ] **Testing**: Add integration tests covering dynamic `source_uri` changes in ForestView.】

## source-fs
考虑到一个agent可能需要监听十几个fs source。如果每个source都能监控1000万个文件的inotify，cpu、内存开销会有多大？是否要改进。如设置监听年龄上限。
- [ ] **Architecture**: Revisit `FusionPipe.get_session_role` implementation. Currently uses `is_any_leader` logic (one-size-fits-all) for M:N mapping due to Agent's single-channel nature. Future work should consider per-view role negotiation if Agents need to support mixed Leader/Follower states across different views on the same pipe.
