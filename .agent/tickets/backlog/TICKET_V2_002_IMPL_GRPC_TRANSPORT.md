# TICKET_V2_002_IMPL_GRPC_TRANSPORT: Implement gRPC Transport layer

> **Status**: Backlog
> **Type**: Feature
> **Workstream**: refactor/architecture-v2

## 1. Context
`Phase 3` of the architecture refactoring (specs/01-ARCHITECTURE.md) calls for gRPC transport support. Currently, only HTTP is implemented.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Implement `fustor-sender-grpc` package.
- [ ] Implement `fustor-receiver-grpc` package.
- [ ] Define `.proto` files for event streaming.
- [ ] Support both Realtime and Snapshot over gRPC.

### B. Non-Functional (Should Have)
- [ ] Performance: Lower latency than HTTP for high-frequency events.
- [ ] Portability: Share `.proto` definitions across Agent and Fusion.

## 3. Implementation Plan

> **Strategy**: Spec-First

1.  **Phase 1: Definition**
    - Create `packages/fustor-schema-grpc` (or similar) for Protobuf definitions.
    - Setup package structures.
2.  **Phase 2: Receiver Implementation**
    - Implement `GRPCReceiver` in Fusion.
3.  **Phase 3: Sender Implementation**
    - Implement `GRPCSender` in Agent.

## 4. Acceptance Criteria (DoD)
- [ ] End-to-end integration test passes using gRPC driver.
