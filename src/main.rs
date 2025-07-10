use crate::{
  argv::Argv,
  progress::{Counters, Progress},
};
use clap::Parser;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use fred::{
  clients::Client,
  error::Error,
  types::{config::Server, Builder},
};
use log::{debug, log_enabled, trace, Level};
use std::{sync::Arc, time::Duration};
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

async fn init_psql_pool(argv: &Argv) -> anyhow::Result<Pool> {
  let mut pg_config = tokio_postgres::Config::new();
  pg_config
    .host(argv.psql_host.as_str())
    .port(argv.psql_port)
    .user(argv.psql_user.as_str())
    .password(argv.psql_password.as_str())
    .dbname(argv.psql_db.as_str())
    .keepalives(true)
    .keepalives_idle(Duration::from_secs(30));
  trace!("Connecting to PostgreSQL: {:?}", &pg_config);
  let mgr_config = ManagerConfig {
    // guarantees that the database connection is ready to be used
    recycling_method: RecyclingMethod::Verified,
  };

  let mgr = if let Some(tls) = utils::build_postgres_tls(argv)? {
    Manager::from_config(pg_config, tls, mgr_config)
  } else {
    Manager::from_config(pg_config, NoTls, mgr_config)
  };

  let pool = Pool::builder(mgr).max_size(argv.psql_pool_size).build()?;
  let client = pool
    .get()
    .await
    .map_err(|e| anyhow::anyhow!("Failed to get PostgreSQL client from pool: {:?}", e))?;
  let init_files = argv.read_init_files();
  exporter::init_sql(&client, init_files).await?;

  Ok(pool)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  pretty_env_logger::try_init_timed().expect("Failed to initialize logger");

  let argv = Arc::new(Argv::parse().fix());
  argv.set_globals();
  debug!("Argv: {:?}", argv);
  if argv.redis_tls || argv.psql_tls {
    openssl::init();
  }
  let (_, nodes) = init_redis(&argv).await.expect("Failed to initialize Redis client");
  let psql_pool = init_psql_pool(&argv)
    .await
    .expect("Failed to initialize PostgreSQL connection pool");

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
    exporter::save(&argv, &progress, index, psql_pool).await?;
    sigint_task.abort();
  } else if argv.quiet {
    println!("Skip saving to PostgreSQL during dry run.");
  } else {
    status!("Skip saving to PostgreSQL during dry run.");
  }

  progress.update_totals(&counters);
  Ok(())
}
