# Step 04: Polish and Consistency Fixes [COMPLETED]

- [x] **Task 4.1: Remove Time Unit Heuristic** <!-- id: 4.1 -->
    - [x] Locate `MS_THRESHOLD` in `FSArbitrator` and remove it. <!-- id: 4.1.1 -->
    - [x] Ensure all drivers (SourceFS, etc.) output seconds. <!-- id: 4.1.2 -->
    - [x] Update tests to use seconds exclusively. <!-- id: 4.1.3 -->

- [x] **Task 4.2: Clean Debug Log Spam** <!-- id: 4.2 -->
    - [x] Remove `print()` statements in `FusionPipeline.py`. <!-- id: 4.2.1 -->
    - [x] Review other files for any leftover standard output usage. <!-- id: 4.2.2 -->

- [x] **Task 4.3: Optimize Suspect Stability Check** <!-- id: 4.3 -->
    - [x] Update `FSViewProvider.update_suspect` to renew TTL and update `recorded_mtime` when mtime changes. <!-- id: 4.3.1 -->
    - [x] Verify `cleanup_expired_suspects` correctly handles the updated state. <!-- id: 4.3.2 -->

- [x] **Task 4.4: Final Global Verification** <!-- id: 4.4 -->
    - [x] Run all runtime tests. <!-- id: 4.4.1 -->
    - [x] Update `refactor-global-TODO.md`. <!-- id: 4.4.2 -->
