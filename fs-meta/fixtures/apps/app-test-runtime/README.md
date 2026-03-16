# `app-test-runtime` fixture

This directory is a test fixture only.

- It is not a workspace member.
- It exists to build a disposable app artifact for integration and spec scenarios.
- Platform docs, ownership, and module inventories must not treat it as a product crate.
- `app/` is the fixture's ordinary authoring crate.
- The top-level crate remains only the disposable artifact/runtime wrapper that owns the entry macro and cdylib output.
