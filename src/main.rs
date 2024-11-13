use crate::{
  argv::Argv,
  progress::{Counters, Progress},
};
use clap::Parser;
use fred::{
  clients::RedisClient,
  error::RedisError,
  prelude::RedisErrorKind,
  types::{Builder, Server},
};
use log::{debug, log_enabled, trace, Level};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_postgres::{error::Error as PostgresError, Client, NoTls};

mod argv;
mod exporter;
mod index;
mod progress;
mod scanner;
mod utils;

/// A cluster node identifier and any information necessary to connect to it.
pub(crate) struct ClusterNode {
  pub server:   Server,
  pub builder:  Builder,
  pub readonly: bool,
}

type PostgresResult = Result<(), PostgresError>;

/// Initialize the client and discover any other servers to be scanned.
async fn init_redis(argv: &Argv) -> Result<(RedisClient, Vec<ClusterNode>), RedisError> {
  debug!("Connecting to Redis...");
  status!("Discovering servers...");

  let (builder, client) = utils::init(argv).await?;
  let nodes = utils::discover_servers(argv, &client)
    .await?
    .into_iter()
    .map(|server| {
      let builder = utils::change_builder_server(&builder, &server)?;
      Ok(ClusterNode {
        server,
        builder,
        readonly: argv.redis_replicas,
      })
    })
    .collect::<Result<Vec<_>, RedisError>>()?;

  if log_enabled!(Level::Debug) {
    let servers: Vec<_> = nodes.iter().map(|s| format!("{}", s.server)).collect();
    debug!("Discovered Redis nodes: {:?}", servers);
  }
  Ok((client, nodes))
}

async fn init_psql(argv: &Argv) -> Result<(Client, JoinHandle<PostgresResult>), RedisError> {
  let conn_str = format!(
    "host={} port={} user={} password={} dbname={} keepalives=1 keepalives_idle=30",
    argv.psql_host, argv.psql_port, argv.psql_user, argv.psql_password, argv.psql_db
  );

  trace!("Connecting to PostgreSQL: {}", conn_str);
  let (client, jh) = if let Some(tls) = utils::build_postgres_tls(argv)? {
    let (client, connection) = tokio_postgres::connect(&conn_str, tls)
      .await
      .map_err(|e| RedisError::new(RedisErrorKind::IO, format!("PostgreSQL: {:?}", e)))?;
    let jh = utils::watch_psql_task(connection, argv.quiet);
    (client, jh)
  } else {
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
      .await
      .map_err(|e| RedisError::new(RedisErrorKind::IO, format!("PostgreSQL: {:?}", e)))?;
    let jh = utils::watch_psql_task(connection, argv.quiet);
    (client, jh)
  };

  let init_files = argv.read_init_files();
  exporter::init_sql(&client, init_files).await?;
  Ok((client, jh))
}

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  pretty_env_logger::try_init_timed().expect("Failed to initialize logger");

  let argv = Arc::new(Argv::parse().fix());
  argv.set_globals();
  debug!("Argv: {:?}", argv);
  if argv.redis_tls || argv.psql_tls {
    openssl::init();
  }
  let (_, nodes) = init_redis(&argv).await.expect("Failed to initialize Redis client");
  let (psql_client, psql_conn) = init_psql(&argv).await.expect("Failed to initialize PostgreSQL client");

  let progress = Arc::new(Progress::default());
  let counters = Arc::new(Counters::new());
  let quiet = argv.quiet;

  let sigint_task = tokio::spawn(async move {
    let _ = tokio::signal::ctrl_c().await;
    if quiet {
      eprintln!("Sigint received, stopping Redis scanner and saving to PostgreSQL.");
    } else {
      status!("Sigint received, stopping Redis scanner and saving to PostgreSQL.");
    }
  });

  let index = scanner::index(&argv, &counters, &progress, nodes).await?;
  sigint_task.abort();
  if log_enabled!(Level::Debug) {
    debug!("Indexed: {:?}", index);
  }

  if !argv.dry_run {
    debug!("Saving index to PostgreSQL...");
    let sigint_task = tokio::spawn(async move {
      loop {
        let _ = tokio::signal::ctrl_c().await;
        if quiet {
          eprintln!("SIGINT received. Exiting now could result in lost data. Send SIGKILL to stop.");
        } else {
          status!("SIGINT received. Exiting now could result in lost data. Send SIGKILL to stop.");
        }
      }
    });
    exporter::save(&argv, &progress, index, psql_client).await?;
    sigint_task.abort();
    psql_conn.abort();
  } else if argv.quiet {
    println!("Skip saving to PostgreSQL during dry run.");
  } else {
    status!("Skip saving to PostgreSQL during dry run.");
  }

  progress.update_totals(&counters);
  Ok(())
}
