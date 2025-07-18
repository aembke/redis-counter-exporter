use crate::progress::{set_min_refresh_delay, set_quiet_output};
use clap::Parser;
use log::debug;
use std::fs;

#[derive(Parser, Debug, Clone, Default)]
#[command(
  version,
  about,
  long_about = "Utilities for transferring counter data from Redis to PostgreSQL."
)]
#[command(propagate_version = true)]
pub struct Argv {
  /// The server hostname.
  #[arg(long = "redis-host", default_value = "127.0.0.1", value_name = "STRING")]
  pub redis_host: String,
  /// The server port.
  #[arg(long = "redis-port", default_value = "6379", value_name = "NUMBER")]
  pub redis_port: u16,
  /// The database to `SELECT` after connecting.
  #[arg(long = "redis-db", value_name = "NUMBER")]
  pub redis_db: Option<u8>,
  /// The username to provide when authenticating.
  #[arg(long = "redis-username", env = "REDIS_USERNAME", value_name = "STRING")]
  pub redis_username: Option<String>,
  /// The password to provide after connection.
  #[arg(long = "redis-password", env = "REDIS_PASSWORD", value_name = "STRING")]
  pub redis_password: Option<String>,
  /// Whether to discover other nodes in a Redis cluster.
  #[arg(long = "redis-cluster", default_value = "false")]
  pub redis_cluster: bool,
  /// Whether to scan replicas rather than primary nodes. This also implies `--cluster`.
  #[arg(
    long = "redis-replicas",
    default_value = "false",
    conflicts_with_all(["decr", "reset", "expire"])
  )]
  pub redis_replicas: bool,
  /// An optional reconnection delay. If not provided the client will stop scanning after any disconnection.
  #[arg(long = "redis-reconnect", value_name = "NUMBER")]
  pub redis_reconnect: Option<u32>,

  // TLS Arguments
  /// Whether to use TLS when connecting to Redis.
  #[arg(long = "redis-tls", default_value = "false")]
  pub redis_tls: bool,
  /// A file path to the private key for a x509 identity used by the client.
  #[arg(long = "tls-key", value_name = "PATH", hide = true)]
  pub tls_key: Option<String>,
  /// A file path to the certificate for a x509 identity used by the client.
  #[arg(long = "tls-cert", value_name = "PATH", hide = true)]
  pub tls_cert: Option<String>,
  /// A file path to a trusted certificate bundle.
  #[arg(long = "tls-ca-cert", value_name = "PATH", hide = true)]
  pub tls_ca_cert: Option<String>,

  /// A file path to the private key for a x509 identity used by the redis client.
  #[arg(long = "redis-tls-key", value_name = "PATH")]
  pub redis_tls_key: Option<String>,
  /// A file path to the certificate for a x509 identity used by the redis client.
  #[arg(long = "redis-tls-cert", value_name = "PATH")]
  pub redis_tls_cert: Option<String>,
  /// A file path to a trusted certificate bundle used by the redis client.
  #[arg(long = "redis-tls-ca-cert", value_name = "PATH")]
  pub redis_tls_ca_cert: Option<String>,

  /// A file path to the private key for a x509 identity used by the postgres client.
  #[arg(long = "psql-tls-key", value_name = "PATH")]
  pub psql_tls_key: Option<String>,
  /// A file path to the certificate for a x509 identity used by the postgres client.
  #[arg(long = "psql-tls-cert", value_name = "PATH")]
  pub psql_tls_cert: Option<String>,
  /// A file path to a trusted certificate bundle used by the postgres client.
  #[arg(long = "psql-tls-ca-cert", value_name = "PATH")]
  pub psql_tls_ca_cert: Option<String>,

  // Shared Scan Arguments
  /// The glob pattern to provide in each `SCAN` command.
  #[arg(long = "pattern", value_name = "STRING", default_value = "*")]
  pub pattern: String,
  /// The number of results to request in each `SCAN` command.
  #[arg(long = "page-size", default_value = "100", allow_negative_numbers = false)]
  pub page_size: u32,
  /// A delay, in milliseconds, to wait between `SCAN` commands.
  #[arg(long = "redis-delay", default_value = "0")]
  pub redis_delay: u64,
  /// A regular expression used to filter keys while scanning. Keys that do not match will be skipped before
  /// any subsequent operations are performed.
  #[arg(short = 'f', long = "filter", value_name = "REGEX")]
  pub filter: Option<String>,
  /// A regular expression used to reject or skip keys while scanning. Keys that match will be skipped before
  /// any subsequent operations are performed.
  #[arg(short = 's', long = "reject", value_name = "REGEX")]
  pub reject: Option<String>,
  /// Whether to skip keys that do not capture anything from the `--extractors` regular expressions.
  #[arg(long = "filter-missing-groups", default_value = "true")]
  pub filter_missing_groups: bool,
  /// The number of records to index in memory while scanning. This should be just larger than the max expected
  /// cardinality of the extractors.
  #[arg(
    long = "initial-index-size",
    allow_negative_numbers = false,
    value_name = "NUMBER",
    default_value = "1024"
  )]
  pub initial_index_size: usize,
  /// One or more extractors used to capture and map portions of the Redis key to a PostgreSQL column.
  #[arg(
    num_args(0..),
    value_delimiter = ',',
    short = 'e',
    long = "extractor",
    value_name = "<COLUMN>[::INT|STRING]=<REGEX>,<COLUMN>[::INT|STRING]=<REGEX>"
  )]
  pub extractors: Vec<String>,
  /// A delimiter used to `slice::join` multiple values from each extractor, if applicable.
  #[arg(long = "extractor-delimiter", value_name = "STRING", default_value = ":")]
  pub extractor_delimiter: String,
  /// Whether to reset counters while scanning.
  #[arg(long = "reset", default_value = "false")]
  pub reset: bool,
  /// Whether to decrement counters by the most recent sample while scanning. Combined with `--expire` this is more
  /// cancellation-safe than `--reset` and allows concurrent scanners to work correctly without race conditions.
  #[arg(long = "decr", default_value = "false", conflicts_with = "reset")]
  pub decr: bool,
  /// Set an expiration (milliseconds) on values after reading them.
  #[arg(long = "expire", value_name = "NUMBER")]
  pub expire: Option<i64>,
  /// Whether to send `GT` with the `PEXPIRE` command.
  #[arg(long = "expire-gt", default_value = "false")]
  pub expire_gt: bool,

  /// Set a minimum refresh delay between progress bar updates, in milliseconds.
  #[arg(long = "min-refresh-delay", value_name = "NUMBER")]
  pub refresh: Option<u64>,
  /// Whether to hide progress bars and messages before the final output.
  #[arg(short = 'q', long = "quiet", default_value = "false")]
  pub quiet: bool,
  /// Ignore errors, if possible.
  #[arg(short = 'i', long = "ignore", default_value = "true")]
  pub ignore: bool,
  /// Perform a dry run, scanning and indexing the values but not resetting, decrementing, expiring, or writing them
  /// to PostgreSQL.
  #[arg(long = "dry-run", default_value = "false")]
  pub dry_run: bool,

  /// The hostname of the PostgreSQL server.
  #[arg(
    long = "psql-host",
    default_value = "127.0.0.1",
    value_name = "STRING",
    required = true
  )]
  pub psql_host: String,
  /// The port on which the PostgreSQL server is listening.
  #[arg(long = "psql-port", default_value = "5432", value_name = "NUMBER", required = true)]
  pub psql_port: u16,
  /// The PostgreSQL username.
  #[arg(long = "psql-user", value_name = "STRING", required = true, env = "PSQL_USERNAME")]
  pub psql_user: String,
  /// The PostgreSQL password.
  #[arg(
    long = "psql-password",
    value_name = "STRING",
    required = true,
    env = "PSQL_PASSWORD"
  )]
  pub psql_password: String,
  /// The database to use after connecting.
  #[arg(long = "psql-db", value_name = "STRING", required = true)]
  pub psql_db: String,
  /// The table into which results should be added.
  #[arg(long = "psql-table", value_name = "STRING", required = true)]
  pub psql_table: String,
  /// Whether to use TLS when connecting to PostgreSQL. The same x509 identity provided by the other TLS argv will be
  /// used, if specified.
  #[arg(long = "psql-tls", default_value = "false")]
  pub psql_tls: bool,
  /// The batch size to use when inserting records into PostgreSQL.
  #[arg(long = "psql-batch", value_name = "NUMBER", default_value = "512")]
  pub psql_batch: u64,
  /// The name of the column used to store the created timestamp on new records.
  #[arg(long = "psql-date-col", value_name = "STRING", default_value = "created")]
  pub psql_date_col: String,
  /// The name of the column used to store the value on new records.
  #[arg(long = "psql-count-col", value_name = "STRING", default_value = "counter")]
  pub psql_count_col: String,
  /// A delay to wait between each batch insert to PostgreSQL.
  #[arg(long = "psql-delay", value_name = "NUMBER")]
  pub psql_delay: Option<u64>,
  /// A file or folder path containing SQL statements that should run before scanning or exporting. This is typically
  /// used to conditionally create tables or indexes.
  ///
  /// If a folder path is provided the client will run the files according to their lexicographical sort. If a file
  /// contains multiple statements they will be split by ";" and sent as separate queries.
  #[arg(long = "psql-init", value_name = "PATH")]
  pub psql_init: Option<String>,
  /// The maximum number of connections in the PostgreSQL connection pool.
  #[arg(long = "psql-pool-size", value_name = "NUMBER", default_value = "8")]
  pub psql_pool_size: usize,
}

impl Argv {
  pub fn set_globals(&self) {
    if let Some(dur) = self.refresh {
      set_min_refresh_delay(dur as usize);
    }
    if self.quiet {
      set_quiet_output(true);
    }
  }

  // there's gotta be a better way to do this
  pub fn fix(mut self) -> Self {
    // TODO support decrement and expire with replica scanning
    if self.redis_replicas && (self.expire.is_some() || self.decr || self.reset) {
      panic!("Replica scanning is not yet supported with expirations or counter modifications.");
    }
    if self.redis_replicas {
      self.redis_cluster = true;
    }
    if self.extractors.is_empty() {
      panic!("At least one extractor is required ");
    }
    // env vars can be Some("") when left unset
    if let Some(username) = self.redis_username.take() {
      if !username.is_empty() {
        self.redis_username = Some(username);
      }
    }
    if let Some(password) = self.redis_password.take() {
      if !password.is_empty() {
        self.redis_password = Some(password);
      }
    }
    if self.psql_user.is_empty() {
      panic!("Missing PostgreSQL username argv.");
    }
    if self.psql_password.is_empty() {
      panic!("Missing PostgreSQL password argv.");
    }

    if self.redis_tls_key.is_none() {
      self.redis_tls_key = self.tls_key.clone();
    }
    if self.redis_tls_cert.is_none() {
      self.redis_tls_cert = self.tls_cert.clone();
    }
    if self.redis_tls_ca_cert.is_none() {
      self.redis_tls_ca_cert = self.tls_ca_cert.clone();
    }
    if self.psql_tls_key.is_none() {
      self.psql_tls_key = self.tls_key.clone();
    }
    if self.psql_tls_cert.is_none() {
      self.psql_tls_cert = self.tls_cert.clone();
    }
    if self.psql_tls_ca_cert.is_none() {
      self.psql_tls_ca_cert = self.tls_ca_cert.clone();
    }

    self
  }

  pub fn read_init_files(&self) -> Vec<String> {
    if let Some(ref path) = self.psql_init {
      debug!("Reading SQL init from {}", path);
      let metadata = fs::metadata(path).unwrap();
      if metadata.is_dir() {
        let mut sorted: Vec<_> = fs::read_dir(path)
          .unwrap()
          .into_iter()
          .map(|f| f.expect("Failed to inspect SQL file dir "))
          .collect();
        sorted.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        sorted
          .into_iter()
          .flat_map(|file| {
            let file = fs::read_to_string(file.path()).unwrap();
            file
              .split(";")
              .into_iter()
              .map(|s| format!("{};", s.trim()))
              .collect::<Vec<_>>()
          })
          .collect()
      } else {
        vec![fs::read_to_string(&path).unwrap()]
      }
    } else {
      Vec::new()
    }
  }
}