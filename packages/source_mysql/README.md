# FuAgent MySQL Source Driver

This package provides the MySQL source driver for FuAgent.

## Implementation Notes

This driver serves as a reference implementation for the two-phase synchronization model required by FuAgent.

- **Snapshot Phase (`get_snapshot_iterator`)**:
  The snapshot is implemented using a transaction with `START TRANSACTION WITH CONSISTENT SNAPSHOT`. This ensures that the data read during the snapshot is consistent as of the moment the transaction began. Data is fetched in batches using a server-side cursor (`SSCursor`) via `cursor.fetchmany` to handle large tables efficiently without consuming excessive client-side memory.

- **Message Phase (`get_message_iterator`)**:
  Real-time change data capture (CDC) is implemented by listening to the MySQL binary log (Binlog). The `pymysql-replication` library is used to stream row-based replication events (`WriteRowsEvent`, `UpdateRowsEvent`, `DeleteRowsEvent`), which are then converted into FuAgent's standard `Event` format.

---

## Configuration Requirements

To ensure FuAgent can reliably capture data changes from MySQL, the server and user permissions must meet the following requirements.

### 1. Server Configuration (`my.cnf`)

The following parameters must be set correctly in MySQL's configuration file (`my.cnf` or `my.ini`):

- **`log_bin`**: Binary logging must be enabled.
  ```ini
  [mysqld]
  log_bin = mysql-bin
  ```
- **`binlog_format`**: The binary log format must be set to `ROW`. FuAgent relies on the complete before-and-after data images provided by this format.
  ```ini
  [mysqld]
  binlog_format = ROW
  ```
- **`server_id`**: The server must have a globally unique ID, as required by the MySQL replication protocol.
  ```ini
  [mysqld]
  server_id = 1
  ```

### 2. Storage Engine

- **InnoDB is Mandatory**: All tables to be synchronized **must** use the `InnoDB` storage engine. FuAgent's consistent snapshot mechanism relies on InnoDB's transactional and MVCC features. Non-transactional engines like `MyISAM` are **not supported** as they cannot guarantee data consistency.

### 3. User Permissions

FuAgent's setup wizard requires two types of credentials:

- **Admin User**: Used only during the configuration phase to automatically create and grant privileges to the agent user. This user requires `CREATE USER` and `GRANT OPTION` privileges.
- **Agent User**: This is the user FuAgent uses for its daily operations. It requires the following minimum set of privileges:
  - `REPLICATION SLAVE`: Allows the user to connect to the master and request the binlog.
  - `REPLICATION CLIENT`: Allows the user to execute commands like `SHOW MASTER STATUS` to get binlog position information.
  - `SELECT`: Allows the user to perform `SELECT` queries on all tables to be synchronized for the initial data snapshot.

**Example Grant SQL:**

```sql
-- Create the agent user
CREATE USER 'fuagent_user'@'%' IDENTIFIED BY 'your_secure_password';
-- Grant necessary privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'fuagent_user'@'%';
-- Flush privileges
FLUSH PRIVILEGES;
```
