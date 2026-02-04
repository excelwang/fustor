# Ticket: Implement gRPC Transport

> **Status**: Backlog
> **Type**: Feature
> **Workstream**: feat/grpc-transport

## 1. Context
As per the System Architecture Spec (01-ARCHITECTURE.md), Phase 3 requires high-performance gRPC transport between Agent and Fusion to replace the HTTP protocol for improved throughput and bi-directional streaming.

## 2. Requirements

### A. Functional (Must Have)
- [ ] Implement `gRPC-Sender` in `packages/sender-grpc/`.
- [ ] Implement `gRPC-Receiver` in `packages/receiver-grpc/`.
- [ ] Define Protobuf schemas for `Event`, `Snapshot`, and `Heartbeat`.
- [ ] Update `fustor-agent` to support `grpc` in `senders-config.yaml`.

### B. Non-Functional (Should Have)
- [ ] Throughput: > 10,000 events/sec.
- [ ] Latency: < 50ms overhead.

## 3. Implementation Plan

> **Strategy**: Prototype

1.  **Phase 1: Proto Definition**
    - Create `proto/fustor.proto`.
    - Generate Python stubs.

2.  **Phase 2: Transport Layer**
    - Implement `GrpcSender` class inheriting from `SenderDriver`.
    - Implement `GrpcReceiver` handler for `GrpcServer`.

## 4. Acceptance Criteria (DoD)
- [ ] `fustor-benchmark` can run with `transport: grpc`.
- [ ] Integration test passes with gRPC config.
