# Redis Counter Exporter

## Usage

```
Utilities for transferring counter data from Redis to PostgreSQL.

Usage: redis-counter-exporter [OPTIONS] --psql-host <STRING> --psql-port <NUMBER> --psql-user <STRING> --psql-password <STRING> --psql-db <STRING> --psql-table <STRING>

Options:
      --redis-host <STRING>
          The server hostname
          
          [default: 127.0.0.1]

      --redis-port <NUMBER>
          The server port
          
          [default: 6379]

      --redis-db <NUMBER>
          The database to `SELECT` after connecting

      --redis-username <STRING>
          The username to provide when authenticating
          
          [env: REDIS_USERNAME=]

      --redis-password <STRING>
          The password to provide after connection
          
          [env: REDIS_PASSWORD=]

      --redis-cluster
          Whether to discover other nodes in a Redis cluster

      --redis-replicas
          Whether to scan replicas rather than primary nodes. This also implies `--cluster`

      --redis-reconnect <NUMBER>
          An optional reconnection delay. If not provided the client will stop scanning after any disconnection

      --redis-tls
          Whether to use TLS when connecting to Redis

      --tls-key <PATH>
          A file path to the private key for a x509 identity used by the client

      --tls-cert <PATH>
          A file path to the certificate for a x509 identity used by the client

      --tls-ca-cert <PATH>
          A file path to a trusted certificate bundle

      --pattern <STRING>
          The glob pattern to provide in each `SCAN` command
          
          [default: *]

      --page-size <PAGE_SIZE>
          The number of results to request in each `SCAN` command
          
          [default: 100]

      --redis-delay <REDIS_DELAY>
          A delay, in milliseconds, to wait between `SCAN` commands
          
          [default: 0]

  -f, --filter <REGEX>
          A regular expression used to filter keys while scanning. Keys that do not match will be skipped before any subsequent operations are performed

  -s, --reject <REGEX>
          A regular expression used to reject or skip keys while scanning. Keys that match will be skipped before any subsequent operations are performed

      --filter-missing-groups
          Whether to skip keys that do not capture anything from the `--extractors` regular expressions

      --initial-index-size <NUMBER>
          The number of records to index in memory while scanning. This should be just larger than the max expected cardinality of the extractors
          
          [default: 1024]

  -e, --extractor [<<COLUMN>[::INT|STRING]=<REGEX>,<COLUMN>[::INT|STRING]=<REGEX>>...]
          One or more extractors used to capture and map portions of the Redis key to a PostgreSQL column

      --extractor-delimiter <STRING>
          A delimiter used to `slice::join` multiple values from each extractor, if applicable
          
          [default: :]

      --reset
          Whether to reset counters while scanning

      --decr
          Whether to decrement counters by the most recent sample while scanning. Combined with `--expire` this is more cancellation-safe than `--reset` and allows concurrent scanners to work correctly without race conditions

      --expire <NUMBER>
          Set an expiration (milliseconds) on values after reading them

      --expire-gt
          Whether to send `GT` with the `PEXPIRE` command

      --min-refresh-delay <NUMBER>
          Set a minimum refresh delay between progress bar updates, in milliseconds

  -q, --quiet
          Whether to hide progress bars and messages before the final output

  -i, --ignore
          Ignore errors, if possible

      --dry-run
          Perform a dry run, scanning and indexing the values but not resetting, decrementing, expiring, or writing them to PostgreSQL

      --psql-host <STRING>
          The hostname of the PostgreSQL server
          
          [default: 127.0.0.1]

      --psql-port <NUMBER>
          The port on which the PostgreSQL server is listening
          
          [default: 5432]

      --psql-user <STRING>
          The PostgreSQL username
          
          [env: PSQL_USERNAME=foo]

      --psql-password <STRING>
          The PostgreSQL password
          
          [env: PSQL_PASSWORD=bar]

      --psql-db <STRING>
          The database to use after connecting

      --psql-table <STRING>
          The table into which results should be added

      --psql-tls
          Whether to use TLS when connecting to PostgreSQL. The same x509 identity provided by the other TLS argv will be used, if specified

      --psql-batch <NUMBER>
          The batch size to use when inserting records into PostgreSQL
          
          [default: 512]

      --psql-date-col <STRING>
          The name of the column used to store the created timestamp on new records
          
          [default: created]

      --psql-count-col <STRING>
          The name of the column used to store the value on new records
          
          [default: counter]

      --psql-delay <NUMBER>
          A delay to wait between each batch insert to PostgreSQL

      --psql-init <PATH>
          A file or folder path containing SQL statements that should run before scanning or exporting. This is typically used to conditionally create tables or indexes.
          
          If a folder path is provided the client will run the files according to their lexicographical sort. If a file contains multiple statements they will be split by ";" and sent as separate queries.

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

## Background

Redis/Valkey is often used as a storage layer for hot, write-heavy data such as counters or other metrics. However, it's
often useful to periodically aggregate and transfer this data to more durable storage, such as PostgreSQL. This tool is
designed to support this use case with hierarchical key structures.

For example, consider the following Redis key structure that stores counter data for users in an org:

```
counters:<operation>:<org>:<user>
```

with the associated PostgreSQL table:

```
CREATE TABLE IF NOT EXISTS counters (
    created timestamp with time zone not null default now(),
    counter int not null default 0,
    org_id int not null,
    user_id int not null,
    operation varchar(40) not null
);

CREATE INDEX ON counters (org_id);
CREATE INDEX ON counters (user_id);
CREATE INDEX ON counters (counter);
CREATE INDEX ON counters (operation);
CREATE INDEX ON counters (created);
```

This tool will transfer the data from Redis to PostgreSQL, mapping the components of each Redis key to the corresponding
PostgreSQL column. The `--extractor` argv control the mapping from key components to columns. For example:

```
RUST_LOG=trace cargo run --release -- --redis-host redis-cluster-1 --redis-port 30001 --redis-cluster --pattern "counters*" \
  -e "org_id::int=\w+:\w+:(\w+):\w+" -e "user_id::int=\w+:\w+:\w+:(\w+)" -e "operation::string=\w+:(\w+):\w+" \
  --psql-host psql --psql-port 5432 --psql-db redis-counter-exporter --psql-table counters
```

The `--psql-init` argv can be used to send queries before scanning, such as `CREATE TABLE IF NOT EXISTS ...`, etc.

The `--decr` and `--expire` argv can also be used to decrement counters by the sampled value while scanning, effectively
resetting the counters in a safe manner. Callers can also use `--reset` for this, but doing so can risk some data loss
if other INCR operations occur between the SCAN and GETSET operations.