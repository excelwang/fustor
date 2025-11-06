# FuAgent File System Source Driver

# FuAgent File System (fs) Source Driver

This package provides the File System (fs) source driver for FuAgent.

## Features

-   **Real-time Monitoring**: Uses the extended `watchdog` library to monitor file system events (create, delete, modify, move) in real-time.
-   **Snapshotting**: Can perform a full snapshot of the specified directory to ensure data consistency.
-   **Smart Dynamic Monitoring**: Implements an LRU (Least Recently Used) caching mechanism to manage watched directories. This allows it to monitor a large number of directories without hitting the system's `inotify` watch limit. Older, inactive directories are automatically unwatched to make room for new, active ones.

## Configuration

-   `driver`: `fs`
-   `uri`: The absolute path to the directory to be monitored (e.g., `/path/to/my/files`).
-   `driver_params`:
    -   `file_pattern`: A glob pattern to filter which files to include (e.g., `*.log`, `data-*.json`).
    -   `startup_mode`: Can be `snapshot` (default) or `message-only`.

## Core Implementation Strategy

This driver is designed to be a lightweight, robust, and resource-efficient solution for monitoring file system events. It incorporates several advanced strategies to handle large and active directory structures gracefully.

### 1. Smart Dynamic Monitoring & LRU Policy

To avoid exhausting the system's `inotify` watch limits (typically controlled by `fs.inotify.max_user_watches`), this driver adopts a dynamic, non-recursive watching strategy.

- **Directory-level Watches**: Instead of one recursive watch on the root path, the driver places an independent, non-recursive watch on each subdirectory it discovers.

- **LRU Eviction**: The driver maintains an internal LRU (Least Recently Used) cache of all active directory watches. If adding a new watch would exceed the configured limit, the driver automatically "prunes" the watch on the "oldest" directory to make space. The definition of "oldest" is based on the timestamp associated with each watch. **Note**: The cache's `get_oldest` operation has O(1) best-case performance but O(N) worst-case performance when heap cleaning is required.

- **Timestamp Model**: The timestamp stored for each watch in the LRU cache follows a consistent model throughout the driver lifecycle:
    1.  **Initial Assignment**: When a directory is scheduled for monitoring (during pre-scan or runtime), its timestamp is set to reflect either its historical content modification time (for initial pre-scan) or the current time (for runtime events).
    2.  **Runtime Updates**: During active monitoring, any file event (`create`, `delete`, `close`, etc.) that occurs within a monitored directory will trigger a `touch`, updating the directory's timestamp in the LRU cache to the **current system time** (`time.time()`). This makes the LRU policy effectively a "Least Recently Used (Active)" strategy during runtime.

- **Thread-Safe Operations**: The watch management system is fully thread-safe using `threading.RLock()` to protect shared resources during concurrent access from multiple threads (e.g., the main monitoring loop, file system event handlers, and the unscheduling worker).

- **Eviction & Cascading Unscheduling**: When a watch is evicted, the system not only unschedules that specific directory but also checks for any subdirectories that are currently being watched. All matching subdirectory watches are added to an asynchronous queue for proper cleanup in a dedicated worker thread. This ensures proper resource management when directories are removed from monitoring. The eviction process is thread-safe and involves two steps: first, the evicted watch object is immediately placed on the unschedule queue for the exact path being evicted, and then any matching subdirectories are processed for unscheduling. The actual `observer.unschedule()` calls are performed asynchronously in a separate worker thread to avoid blocking the main monitoring loop or causing deadlocks with watchdog's internal locks.

- **Eviction Timing**: The age shown in eviction logs reflects the time difference between when the directory was scheduled for monitoring and the current time, not necessarily the time since the directory's last actual modification. This provides insight into how long a directory remained in the monitoring cache before eviction.

### 2. Dynamic Watch Limit Adaptation

The driver can automatically adapt to the host system's capabilities. 

- **Error Detection**: If the driver attempts to schedule a new watch and receives an `OSError` with `errno 28` (the system's `inotify` limit has been hit), it catches this specific error.
- **Automatic Adjustment**: It then dynamically adjusts its internal `watch_limit` down to the number of watches that were active at the time of the error.
- **Recursive Retry**: After adjusting the limit, it immediately retries scheduling the new watch, which now triggers the LRU eviction logic with the new, correct limit. This ensures no events are lost while making the driver self-tuning.

### 3. Pre-scan and Initial Directory Setup

Before starting real-time monitoring, the driver performs a comprehensive pre-scan of the entire directory tree:

- **Capacity-aware Scan**: The driver walks the entire directory structure to identify the most active directories based on their content modification times.
- **Hierarchy-complete Watch Set**: It builds a hierarchy-complete set of watches (including all parent directories needed for a child directory to be properly watched).
- **Time Synchronization**: The driver calculates a client-server time delta to normalize server modification times to the client's time domain, ensuring consistent timestamp handling.
- **Progress Monitoring**: During the pre-scan, the driver periodically logs statistics every 1000 entries processed (including both directories and files), including the total entries scanned, error count, and the newest/oldest directories found so far (with their ages in days).

### 4. Event Generation Model

The driver aims to provide a clean and consistent event stream.

- **Stability Signal**: A file modification is only registered when an `on_closed` event occurs, ensuring that the driver only processes files that have been completely written.
- **Event Mapping**:
  - `UpdateEvent`: Generated for all pre-existing files during the snapshot, and for `closed` and `moved` (destination path) events during real-time monitoring.
  - `DeleteEvent`: Generated for `deleted` and `moved` (source path) events.
  - `modified` events are intentionally ignored to wait for the `closed` signal.

- **Timestamp Format**: To optimize for downstream consumers, all timestamps in the event payload (`modified_time`, `created_time`) are sent as raw **float Unix timestamps**.

### System Configuration: Increasing Inotify Watch Limit

For optimal performance and to minimize monitoring blind spots, especially when monitoring large directory structures, it is highly recommended to increase the system's `fs.inotify.max_user_watches` limit. This parameter controls the maximum number of files and directories that a user can watch.

**1. Check Current Limit:**
```bash
cat /proc/sys/fs/inotify/max_user_watches
```

**2. Permanently Change the Limit (e.g., to 10,000,000):**
Edit `/etc/sysctl.conf` and add or modify the following line:
```
fs.inotify.max_user_watches = 10000000
```
*Note: The maximum possible value for `fs.inotify.max_user_watches` is `2147483647` (2^31 - 1). A value of 10,000,000 is a common recommendation for systems monitoring many files. Adjust this value based on your system's resources and monitoring needs.*

**3. Apply Changes:**
```bash
sudo sysctl -p
```
This command reloads the system settings from `/etc/sysctl.conf`.

---

## Usage Notes

- **NFS Warning**: Every node mounting the same NFS must run this driver to ensure all changes are captured, as `inotify` events are not propagated across NFS clients.

- **Asynchronous Unscheduling Worker**: The driver uses a dedicated background thread to handle `observer.unschedule()` calls. This prevents blocking the main monitoring thread and avoids potential deadlocks with watchdog's internal synchronization mechanisms. The worker thread processes a queue of watch objects that need to be unscheduled.