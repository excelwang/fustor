# FuAgent Elasticsearch Source Driver

This package provides the Elasticsearch source driver for FuAgent.

## Implementation Notes

- **Snapshot Phase (`get_snapshot_iterator`)**: The snapshot is implemented using the Elasticsearch Point in Time (PIT) API combined with `search_after` for deep, consistent pagination across the index. This is the most efficient way to perform a full scan of an index without being affected by ongoing writes.

- **Message Phase (`get_message_iterator`)**: Real-time events are fetched by periodically polling the index for new documents. It uses a timestamp field (e.g., `@timestamp`) to query for documents that have arrived since the last poll, using a `range` query with the `gt` (greater than) operator.
