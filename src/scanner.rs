use crate::{
  argv::Argv,
  index::{Extractor, Index},
  progress,
  progress::{global_progress, setup_event_logs, Counters, Progress},
  status,
  utils,
  ClusterNode,
};
use fred::{
  clients::Client,
  error::Error,
  prelude::*,
  types::{config::Server, ExpireOptions},
};
use log::{debug, error};
use regex::Regex;
use std::sync::Arc;

#[derive(Clone)]
struct Shared {
  argv:     Arc<Argv>,
  counters: Arc<Counters>,
  index:    Arc<Index>,
  #[allow(dead_code)]
  progress: Arc<Progress>,
}

async fn scan_node(state: Shared, server: Server, client: Client) -> Result<(usize, usize), Error> {
  let scanner = client.scan(state.argv.pattern.clone(), Some(state.argv.page_size), None);
  let filter = state.argv.filter.as_ref().and_then(|s| Regex::new(s).ok());
  let reject = state.argv.reject.as_ref().and_then(|s| Regex::new(s).ok());

  debug!("Scanning {}", server);
  utils::scan_server(
    server.clone(),
    state.argv.ignore,
    state.argv.redis_delay,
    scanner,
    move |mut scanned, mut success, mut skipped, mut errored, keys| {
      let (filter, reject, client, server, state) = (
        filter.clone(),
        reject.clone(),
        client.clone(),
        server.clone(),
        state.clone(),
      );

      async move {
        state.counters.incr_scanned(keys.len());
        scanned += keys.len();

        let keys: Vec<_> = keys
          .into_iter()
          .filter(|key| {
            if utils::should_skip_key_by_regexp(&filter, &reject, key) {
              skipped += 1;
              state.counters.incr_skipped(1);
              false
            } else {
              true
            }
          })
          .collect();

        if !keys.is_empty() {
          debug!("Calling GET or GETSET on {} keys...", keys.len());
          // if this fails in this context it's a bug
          let pipeline = client.pipeline();
          for key in keys.iter() {
            if state.argv.reset && !state.argv.dry_run {
              let _: () = pipeline.getset(key.clone(), 0).await?;
            } else {
              let _: () = pipeline.get(key.clone()).await?;
            }
          }

          let counts = match pipeline.all::<Vec<Option<u64>>>().await {
            Ok(counts) => counts,
            Err(e) => {
              error!("{} Error calling GET or GETSET: {:?}", server, e);

              if state.argv.ignore {
                return Ok((scanned, success, skipped, errored));
              } else {
                return Err(e);
              }
            },
          };

          for (idx, key) in keys.iter().enumerate() {
            if let Some(value) = counts[idx] {
              if value == 0 {
                skipped += 1;
                state.counters.incr_skipped(1);
                continue;
              }
              match state.index.upsert(key, value) {
                Ok(false) => {
                  skipped += 1;
                  state.counters.incr_skipped(1);
                },
                Ok(true) => {
                  state.counters.incr_success(1);
                  success += 1;
                },
                Err(e) => {
                  if !state.argv.ignore {
                    state.counters.incr_errored(1);
                    errored += 1;
                    return Err(e);
                  } else {
                    state.counters.incr_skipped(1);
                    skipped += 1;
                  }
                },
              }
            }
          }

          if !state.argv.dry_run {
            if state.argv.decr {
              let pipeline = client.pipeline();
              for (idx, key) in keys.iter().enumerate() {
                if let Some(value) = counts[idx] {
                  let _: () = pipeline.decr_by(key, value as i64).await?;
                }
              }
              if let Err(e) = pipeline.last::<()>().await {
                if !state.argv.ignore {
                  state.counters.incr_errored(1);
                  errored += 1;
                  return Err(e);
                } else {
                  state.counters.incr_skipped(1);
                  skipped += 1;
                }
              }
            }
            if let Some(expire) = state.argv.expire {
              let pipeline = client.pipeline();
              for key in keys.iter() {
                let opts = if state.argv.expire_gt {
                  Some(ExpireOptions::GT)
                } else {
                  None
                };
                let _: () = pipeline.pexpire(key, expire, opts).await?;
              }
              if let Err(e) = pipeline.last::<()>().await {
                if !state.argv.ignore {
                  state.counters.incr_errored(1);
                  errored += 1;
                  return Err(e);
                } else {
                  state.counters.incr_skipped(1);
                  skipped += 1;
                }
              }
            }
          }
        }

        Ok((scanned, success, skipped, errored))
      }
    },
  )
  .await
}

pub async fn index(
  argv: &Arc<Argv>,
  counters: &Arc<Counters>,
  progress: &Arc<Progress>,
  nodes: Vec<ClusterNode>,
) -> Result<Arc<Index>, Error> {
  let extractors = Extractor::from_argv(&argv);
  let state = Shared {
    argv:     argv.clone(),
    counters: counters.clone(),
    progress: progress.clone(),
    index:    Index::new(&argv, extractors),
  };
  debug!("Using extractors: {:?}", state.index.extractors());
  let mut tasks = Vec::with_capacity(nodes.len());
  let totals_jh = progress::watch_totals(&state.counters);

  status!("Connecting to servers...");
  for node in nodes.into_iter() {
    let state = state.clone();
    tasks.push(tokio::spawn(async move {
      let client = node.builder.build()?;
      client.init().await?;
      utils::check_readonly(&node, &client).await?;

      let estimate: u64 = client.dbsize().await?;
      global_progress().add_server(&node.server, Some(estimate), None);
      let estimate_task = tokio::spawn(utils::update_estimate(node.server.clone(), client.clone()));
      let event_task = setup_event_logs(&client);

      let result = scan_node(state, node.server.clone(), client).await;
      debug!("[{}] Finished scanning", node.server);
      estimate_task.abort();
      event_task.abort();
      result.map(|_| ())
    }));
  }

  debug!("Scanning {} nodes...", tasks.len());
  if let Err(e) = utils::wait_with_interrupts(tasks).await {
    debug!("Received scanning error: {:?}", e);
    if !e.is_canceled() {
      eprintln!("Fatal error while scanning: {:?}", e);
    }
  };

  totals_jh.abort();
  progress.update_totals(&counters);
  progress.totals.finish();
  Ok(state.index)
}
