# Known Pitfalls

## Materialized tree PIT entry pagination is not strict snapshot isolation

Scope: fs-meta materialized tree PIT queries, especially when a group has more
entries than the first PIT response can return and later pages use
`entry_after` to fetch additional entry windows.

Current behavior:

- Entries already stored in a `PitSession` remain stable for later pages.
- If an entry window was not fully captured, later `entry_after` requests fetch
  the missing window from the current sink materialized tree and merge it into
  the existing `PitSession`.
- The entry cursor carries `pit_id`, `group`, and `next_entry_offset`; it does
  not carry a stable tree revision, snapshot generation, or last-path anchor.

Known risk:

- If the sink materialized tree changes between pages, offset-based follow-up
  windows can repeat files from an earlier page or skip files that should have
  appeared in the PIT result.
- Inserts before the next offset can make later pages repeat items.
- Deletes before the next offset can make later pages skip items.
- Path-order changes can also cause duplicate or missing entries.

Existing mitigation:

- The merge path detects a repeated entry position and marks the entry window
  complete to avoid repeated refetch loops.
- This mitigation is not a full deduplication or snapshot-isolation guarantee.

Implication:

- The current PIT should be understood as a bounded query-session cache, not a
  strict frozen-tree historical snapshot.
- This pitfall is independent of capanix statecell revision retention. Setting
  sink snapshot statecell history to zero removes full-snapshot revision
  retention, but PIT paging correctness is governed by fustor's query-layer
  `PitSession` and entry cursor design.

Potential repair direction:

- Strict PIT requires binding entry pagination to stable tree state, such as a
  tree revision plus last-path anchor, or freezing the full entry list at PIT
  creation time within explicit TTL and byte limits.
