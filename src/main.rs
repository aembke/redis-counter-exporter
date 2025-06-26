use crate::{
  argv::Argv,
  progress::{Counters, Progress},
};
use clap::Parser;
use fred::{
  clients::Client,
  error::Error,
  types::{config::Server, Builder},
};
use log::{debug, log_enabled, trace, Level};
use postgres_openssl::MakeTlsConnector;
use r2d2_postgres::{
  r2d2::{self},
  PostgresConnectionManager,
};
use std::sync::Arc;
use tokio_postgres::NoTls;

mod argv;
mod exporter;
mod index;
mod progress;
mod scanner;
mod utils;

/// A cluster node identifier and any information necessary to connect to it.
pub(crate) struct ClusterNode {
  pub server: Server,
  pub builder: Builder,
  pub readonly: bool,
}

/// Initialize the client and discover any other servers to be scanned.
async fn init_redis(argv: &Argv) -> Result<(Client, Vec<ClusterNode>), Error> {
  debug!("Connecting to Redis...");
  status!("Discovering servers...");

  let (builder, client) = utils::init(argv).await?;
  let nodes = utils::discover_servers(argv, &client)?
    .into_iter()
    .map(|server| {
      let builder = utils::change_builder_server(&builder, &server)?;
      Ok(ClusterNode {
        server,
        builder,
        readonly: argv.redis_replicas,
      })
    })
    .collect::<Result<Vec<_>, Error>>()?;

  if log_enabled!(Level::Debug) {
    let servers: Vec<_> = nodes.iter().map(|s| format!("{}", s.server)).collect();
    debug!("Discovered Redis nodes: {:?}", servers);
  }
  Ok((client, nodes))
}

async fn init_psql_no_tls(argv: &Argv) -> Result<r2d2::Pool<r2d2_postgres::PostgresConnectionManager<NoTls>>, Error> {
  let conn_str = format!(
    "host={} port={} user={} password={} dbname={} keepalives=1 keepalives_idle=30",
    argv.psql_host, argv.psql_port, argv.psql_user, argv.psql_password, argv.psql_db
  );

  trace!("Connecting to PostgreSQL: {}", conn_str);
  let manager = PostgresConnectionManager::new(conn_str.parse().unwrap(), NoTls);
  let pool = r2d2::Pool::new(manager).unwrap();

  let init_files = argv.read_init_files();
  exporter::init_sql(pool.clone(), init_files).await?;
  Ok(pool)
}

async fn init_psql_tls(
  argv: &Argv,
  tls: MakeTlsConnector,
) -> Result<r2d2::Pool<r2d2_postgres::PostgresConnectionManager<MakeTlsConnector>>, Error> {
  let conn_str = format!(
    "host={} port={} user={} password={} dbname={} keepalives=1 keepalives_idle=30",
    argv.psql_host, argv.psql_port, argv.psql_user, argv.psql_password, argv.psql_db
  );

  let manager = PostgresConnectionManager::new(conn_str.parse().unwrap(), tls);
  let pool = r2d2::Pool::new(manager).unwrap();

  let init_files = argv.read_init_files();
  exporter::init_sql(pool.clone(), init_files).await?;
  Ok(pool)
}

enum PostgresPool {
  NoTls(r2d2::Pool<r2d2_postgres::PostgresConnectionManager<NoTls>>),
  Tls(r2d2::Pool<r2d2_postgres::PostgresConnectionManager<MakeTlsConnector>>),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  pretty_env_logger::try_init_timed().expect("Failed to initialize logger");

  let argv = Arc::new(Argv::parse().fix());
  argv.set_globals();
  debug!("Argv: {:?}", argv);
  if argv.redis_tls || argv.psql_tls {
    openssl::init();
  }
  let (_, nodes) = init_redis(&argv).await.expect("Failed to initialize Redis client");

  let conn_str = format!(
    "host={} port={} user={} password={} dbname={} keepalives=1 keepalives_idle=30",
    argv.psql_host, argv.psql_port, argv.psql_user, argv.psql_password, argv.psql_db
  );

  let pool = match utils::build_postgres_tls(&argv).unwrap() {
    Some(tls) => {
      debug!("Connecting to PostgreSQL with TLS: {}", conn_str);
      PostgresPool::Tls(init_psql_tls(&argv, tls).await?)
    },
    None => {
      debug!("Connecting to PostgreSQL without TLS: {}", conn_str);
      PostgresPool::NoTls(init_psql_no_tls(&argv).await?)
    },
  };

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
    match pool {
      PostgresPool::NoTls(pool) => {
        exporter::save(&argv, &progress, index, pool.into()).await?;
      },
      PostgresPool::Tls(pool) => {
        exporter::save(&argv, &progress, index, pool.into()).await?;
      },
    }
    sigint_task.abort();
  } else if argv.quiet {
    println!("Skip saving to PostgreSQL during dry run.");
  } else {
    status!("Skip saving to PostgreSQL during dry run.");
  }

  progress.update_totals(&counters);
  Ok(())
}
