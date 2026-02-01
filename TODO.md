# Fustor TODO List

## source fs
  - [ ] **通讯协议升级 (gRPC/Protobuf)**: 将 JSON Over HTTP 替换为二进制流式协议（如 gRPC），以支持流式推送、并发 Multiplexing，并降低路径名等重复字符串的序列化开销。