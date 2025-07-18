use crate::{
  argv::Argv,
  progress::{global_progress, min_refresh_delay},
  ClusterNode,
};
use fred::{
  cmd,
  prelude::*,
  types::{
    config::{ClusterDiscoveryPolicy, Config, ReplicaConfig, Server, ServerConfig, TlsConnector, UnresponsiveConfig},
    scan::{ScanResult, Scanner},
    Builder,
  },
};
use futures::{
  future::{select, try_join_all, Either},
  pin_mut, Stream, TryStreamExt,
};
use log::{debug, error, log_enabled, trace};
use openssl::{
  pkey::PKey,
  ssl::{SslConnector, SslMethod},
  x509::X509,
};
use postgres_openssl::MakeTlsConnector;
use regex::Regex;
use std::{
  collections::HashMap,
  fmt::Debug,
  fs,
  future::Future,
  sync::atomic::{AtomicUsize, Ordering},
  time::Duration as StdDuration,
};
use tokio::{pin, task::JoinHandle, time::sleep};

pub const DEFAULT_ESTIMATE_UPDATE_INTERVAL: StdDuration = StdDuration::from_millis(2500);

pub fn incr_atomic(val: &AtomicUsize, amt: usize) -> usize {
  val.fetch_add(amt, Ordering::Acquire).saturating_add(amt)
}

pub fn read_atomic(val: &AtomicUsize) -> usize {
  val.load(Ordering::Acquire)
}

pub fn set_atomic(val: &AtomicUsize, amt: usize) -> usize {
  val.swap(amt, Ordering::SeqCst)
}

fn map_tls_error<E: Debug>(e: E) -> Error {
  Error::new(ErrorKind::Tls, format!("{:?}", e))
}

fn build_tls_config(argv: &Argv) -> Result<Option<TlsConnector>, Error> {
  use fred::native_tls::{Certificate, Identity, TlsConnector};

  if argv.redis_tls {
    let mut builder = TlsConnector::builder();
    if let Some(ca_cert_path) = argv.redis_tls_ca_cert.as_ref() {
      debug!("Reading CA certificate from {}", ca_cert_path);
      let buf = fs::read(ca_cert_path)?;
      let trusted_cert = Certificate::from_pem(&buf)?;
      builder.add_root_certificate(trusted_cert);
    }

    if let Some(key_path) = argv.redis_tls_key.as_ref() {
      if let Some(cert_path) = argv.redis_tls_cert.as_ref() {
        debug!("Reading client key from {}, cert from {}", key_path, cert_path);
        let client_key_buf = fs::read(key_path)?;
        let client_cert_buf = fs::read(cert_path)?;

        let client_cert_chain = if let Some(ca_cert_path) = argv.redis_tls_ca_cert.as_ref() {
          let ca_cert_buf = fs::read(ca_cert_path)?;

          let mut chain = Vec::with_capacity(ca_cert_buf.len() + client_cert_buf.len());
          chain.extend(&client_cert_buf);
          chain.extend(&ca_cert_buf);
          chain
        } else {
          client_cert_buf
        };

        let identity = Identity::from_pkcs8(&client_cert_chain, &client_key_buf)?;
        builder.identity(identity);
      }
    }

    builder.build().map(|t| Some(t.into())).map_err(|e| e.into())
  } else {
    Ok(None)
  }
}

pub fn build_postgres_tls(argv: &Argv) -> Result<Option<MakeTlsConnector>, Error> {
  if argv.psql_tls {
    let mut builder = SslConnector::builder(SslMethod::tls()).map_err(map_tls_error)?;

    if let Some(ca_cert_path) = argv.psql_tls_ca_cert.as_ref() {
      debug!("Reading PostgreSQL CA certificate from {}", ca_cert_path);
      let buf = fs::read(ca_cert_path)?;
      let trusted_cert = X509::from_pem(&buf).map_err(map_tls_error)?;
      builder.cert_store_mut().add_cert(trusted_cert).map_err(map_tls_error)?;
    }

    if let Some(key_path) = argv.psql_tls_key.as_ref() {
      if let Some(cert_path) = argv.psql_tls_cert.as_ref() {
        debug!(
          "Reading PostgreSQL client key from {}, cert from {}",
          key_path,
          cert_path
        );
        let client_key_buf = fs::read(key_path)?;
        let client_cert_buf = fs::read(cert_path)?;

        let client_cert_chain = if let Some(ca_cert_path) = argv.psql_tls_ca_cert.as_ref() {
          let ca_cert_buf = fs::read(ca_cert_path)?;

          let mut chain = Vec::with_capacity(ca_cert_buf.len() + client_cert_buf.len());
          chain.extend(&client_cert_buf);
          chain.extend(&ca_cert_buf);
          chain
        } else {
          client_cert_buf
        };
        let pkey = PKey::private_key_from_pem(&client_key_buf).map_err(map_tls_error)?;

        let cert_ref = X509::from_pem(&client_cert_chain).map_err(map_tls_error)?;
        builder.set_private_key(&pkey).map_err(map_tls_error)?;
        builder.set_certificate(cert_ref.as_ref()).map_err(map_tls_error)?;
        builder.check_private_key().map_err(map_tls_error)?;
      }
    }

    Ok(Some(MakeTlsConnector::new(builder.build())))
  } else {
    Ok(None)
  }
}

/// Discover any other servers that should be scanned.
pub fn discover_servers(argv: &Argv, client: &Client) -> Result<Vec<Server>, Error> {
  let config = client.client_config();

  Ok(if client.is_clustered() {
    // use cached primary nodes or one replica per primary
    let primary_nodes = client
      .cached_cluster_state()
      .map(|state| state.unique_primary_nodes())
      .ok_or_else(|| Error::new(ErrorKind::Config, "Missing cluster state."))?;

    if argv.redis_replicas {
      // pick one replica per primary node
      let replicas = client.replicas().nodes();
      let mut inverted = HashMap::with_capacity(primary_nodes.len());
      for (replica, primary) in replicas.into_iter() {
        let _ = inverted.insert(primary, replica);
      }

      inverted.into_values().collect()
    } else {
      primary_nodes
    }
  } else if config.server.is_sentinel() {
    client.active_connections()
  } else {
    config.server.hosts()
  })
}

/// Create a new `Builder` instance by replacing the destination server with a centralized server config to the
/// provided `server`.
pub fn change_builder_server(builder: &Builder, server: &Server) -> Result<Builder, Error> {
  let config = builder
    .get_config()
    .map(|config| {
      let mut config = config.clone();
      config.server = ServerConfig::Centralized { server: server.clone() };
      config
    })
    .ok_or_else(|| Error::new(ErrorKind::Config, "Invalid client builder."))?;

  let mut out = Builder::from_config(config);
  out.set_connection_config(builder.get_connection_config().clone());
  out.set_performance_config(builder.get_performance_config().clone());
  if let Some(policy) = builder.get_policy() {
    out.set_policy(policy.clone());
  }
  Ok(out)
}

/// Create a client builder and the initial connection to the server.
pub async fn init(argv: &Argv) -> Result<(Builder, Client), Error> {
  let server = if argv.redis_cluster || argv.redis_replicas {
    ServerConfig::Clustered {
      hosts: vec![Server::new(&argv.redis_host, argv.redis_port)],
      policy: ClusterDiscoveryPolicy::ConfigEndpoint,
    }
  } else {
    ServerConfig::Centralized {
      server: Server::new(&argv.redis_host, argv.redis_port),
    }
  };
  let config = Config {
    server,
    tls: build_tls_config(argv)?.map(|t| t.into()),
    username: argv.redis_username.clone(),
    password: argv.redis_password.clone(),
    database: argv.redis_db,
    fail_fast: true,
    ..Default::default()
  };

  let mut builder = Builder::from_config(config);
  builder.with_connection_config(|config| {
    config.unresponsive = UnresponsiveConfig {
      max_timeout: Some(StdDuration::from_millis(10_000)),
      interval: StdDuration::from_millis(2_000),
    };

    if argv.redis_replicas {
      config.replica = ReplicaConfig {
        lazy_connections: true,
        primary_fallback: true,
        ignore_reconnection_errors: true,
        ..Default::default()
      };
    }
  });
  if let Some(dur) = argv.redis_reconnect {
    builder.set_policy(ReconnectPolicy::new_constant(0, dur));
  }

  let client = builder.build()?;
  client.init().await?;
  Ok((builder, client))
}

pub async fn update_estimate(server: Server, client: Client) -> Result<(), Error> {
  loop {
    tokio::time::sleep(DEFAULT_ESTIMATE_UPDATE_INTERVAL).await;

    trace!("Updating estimate for {}", server);
    let estimate: u64 = client.dbsize().await?;
    global_progress().update_estimate(&server, estimate);
  }
}

pub async fn check_readonly(node: &ClusterNode, client: &Client) -> Result<(), Error> {
  if node.readonly {
    client.custom::<(), ()>(cmd!("READONLY"), vec![]).await?;
  }

  Ok(())
}

/// Returns whether the key should be skipped based on the argv `filter` and `reject` expressions.
pub fn should_skip_key_by_regexp(filter: &Option<Regex>, reject: &Option<Regex>, key: &Key) -> bool {
  let matches_filter = if let Some(ref regex) = filter {
    regexp_match(regex, key)
  } else {
    true
  };
  let matches_reject = if let Some(ref regex) = reject {
    regexp_match(regex, key)
  } else {
    false
  };

  !(matches_filter && !matches_reject)
}

/// Returns whether the key matches the provided regexp.
pub fn regexp_match(regex: &Regex, key: &Key) -> bool {
  let key_str = key.as_str_lossy();
  if log_enabled!(log::Level::Trace) {
    trace!(
      "Checking {} against {}: {}",
      key_str,
      regex.as_str(),
      regex.is_match(key_str.as_ref())
    );
  }
  regex.is_match(key_str.as_ref())
}

/// Returns whether the key matches the provided regexp.
pub fn regexp_capture(regex: &Regex, key: &Key, delimiter: &str) -> Option<String> {
  let key_str = key.as_str_lossy();
  let out: Vec<String> = regex
    .captures(key_str.as_ref())
    .iter()
    .flat_map(|s| {
      let mut out = Vec::with_capacity(s.len() - 1);
      for i in 0..s.len() - 1 {
        if let Some(val) = s.get(i + 1) {
          out.push(val.as_str().to_string());
        }
      }

      out
    })
    .collect();

  let out = out.join(delimiter);
  if out.is_empty() {
    None
  } else {
    Some(out)
  }
}

pub async fn scan_server<F, Fut>(
  server: Server,
  ignore: bool,
  delay: u64,
  scanner: impl Stream<Item = Result<ScanResult, Error>>,
  func: F,
) -> Result<(usize, usize), Error>
where
  Fut: Future<Output = Result<(usize, usize, usize, usize), Error>>,
  F: Fn(usize, usize, usize, usize, Vec<Key>) -> Fut,
{
  let mut last_error = None;
  let (mut local_scanned, mut local_success, mut local_skipped, mut local_errored) = (0, 0, 0, 0);
  pin_mut!(scanner);

  loop {
    let mut page = match scanner.try_next().await {
      Ok(Some(page)) => page,
      Ok(None) => break,
      Err(e) => {
        last_error = Some(e);
        break;
      },
    };

    let keys = page.take_results().unwrap_or_default();
    let (scanned, success, skipped, errored) =
      match func(local_scanned, local_success, local_skipped, local_errored, keys).await {
        Ok(counts) => counts,
        Err(e) => {
          error!("Error in scan loop: {:?}", e);
          last_error = Some(e);
          break;
        },
      };
    local_scanned = scanned;
    local_success = success;
    local_skipped = skipped;
    local_errored = errored;

    if delay > 0 && page.has_more() {
      if delay > min_refresh_delay() / 2 {
        global_progress().update(&server, format!("Sleeping for {}ms", delay), Some(local_scanned as u64));
      }

      sleep(StdDuration::from_millis(delay)).await;
    }

    global_progress().update(
      &server,
      format!(
        "{} success, {} skipped, {} errored",
        local_success, local_skipped, local_errored
      ),
      Some(local_scanned as u64),
    );

    page.next();
  }

  if let Some(error) = last_error {
    global_progress().finish(&server, format!("Errored: {:?}", error));

    if ignore {
      Ok((local_success, local_scanned))
    } else {
      Err(error)
    }
  } else {
    let percent = if local_scanned == 0 {
      0.0
    } else {
      local_success as f64 / local_scanned as f64 * 100.0
    };

    global_progress().finish(
      &server,
      format!(
        "Finished scanning ({}/{} = {:.2}% success, {} skipped, {} errored).",
        local_success, local_scanned, percent, local_skipped, local_errored
      ),
    );
    Ok((local_success, local_scanned))
  }
}

pub async fn wait_with_interrupts(ops: Vec<JoinHandle<Result<(), Error>>>) -> Result<(), Error> {
  let ctrl_c_ft = tokio::signal::ctrl_c();
  let ops_ft = try_join_all(ops);
  pin!(ctrl_c_ft);
  pin!(ops_ft);

  match select(ops_ft, ctrl_c_ft).await {
    Either::Left((lhs, _)) => match lhs?.into_iter().find_map(|e| e.err()) {
      None => Ok(()),
      Some(err) => Err(err),
    },
    Either::Right((_, _)) => Ok(()),
  }
}
