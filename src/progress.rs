use crate::{progress, utils};
use fred::{
  clients::RedisClient,
  interfaces::{ClientLike, EventInterface},
  types::Server,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, error};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::{
  borrow::Cow,
  collections::HashMap,
  fmt,
  fmt::Formatter,
  sync::{atomic::AtomicUsize, Arc},
  time::{Duration as StdDuration, Duration, Instant},
};
use tokio::{task::JoinHandle, time::sleep};

const SPINNER_BAR_STYLE_TEMPLATE: &str = "[{elapsed_precise}] {prefix:.bold} {spinner} {msg}";
const COUNTER_BAR_STYLE_TEMPLATE: &str = "[{elapsed_precise}] [{eta_precise}] {prefix:.bold} {bar:25} \
                                          {human_pos}/{human_len} {percent_precise}% {per_sec} {msg}";
const STATUS_BAR_STYLE_TEMPLATE: &str = "{prefix:.bold} {wide_msg}";
static QUIET_OUTPUT: AtomicUsize = AtomicUsize::new(0);
static MIN_REFRESH_DELAY: AtomicUsize = AtomicUsize::new(0);
static PROGRESS: Lazy<Progress> = Lazy::new(Progress::default);

pub fn global_progress() -> &'static Progress {
  &PROGRESS
}

pub fn quiet_output() -> bool {
  utils::read_atomic(&QUIET_OUTPUT) != 0
}

pub fn min_refresh_delay() -> u64 {
  utils::read_atomic(&MIN_REFRESH_DELAY) as u64
}

pub fn set_min_refresh_delay(dur: usize) {
  utils::set_atomic(&MIN_REFRESH_DELAY, dur);
}

pub fn set_quiet_output(val: bool) {
  let val: usize = if val { 1 } else { 0 };
  utils::set_atomic(&QUIET_OUTPUT, val);
}

/// Shared operation counters.
pub struct Counters {
  pub scanned: AtomicUsize,
  pub skipped: AtomicUsize,
  pub errored: AtomicUsize,
  pub success: AtomicUsize,
}

impl fmt::Display for Counters {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "scanned: {}, skipped: {}, errored: {}, success: {}",
      utils::read_atomic(&self.scanned),
      utils::read_atomic(&self.skipped),
      utils::read_atomic(&self.errored),
      utils::read_atomic(&self.success)
    )
  }
}

#[allow(dead_code)]
impl Counters {
  pub fn new() -> Arc<Self> {
    Arc::new(Counters {
      scanned: AtomicUsize::new(0),
      skipped: AtomicUsize::new(0),
      errored: AtomicUsize::new(0),
      success: AtomicUsize::new(0),
    })
  }

  pub fn incr_scanned(&self, amt: usize) -> usize {
    utils::incr_atomic(&self.scanned, amt)
  }

  pub fn incr_skipped(&self, amt: usize) -> usize {
    utils::incr_atomic(&self.skipped, amt)
  }

  pub fn incr_errored(&self, amt: usize) -> usize {
    utils::incr_atomic(&self.errored, amt)
  }

  pub fn incr_success(&self, amt: usize) -> usize {
    utils::incr_atomic(&self.success, amt)
  }

  pub fn set_scanned(&self, amt: usize) -> usize {
    utils::set_atomic(&self.scanned, amt)
  }

  pub fn set_skipped(&self, amt: usize) -> usize {
    utils::set_atomic(&self.skipped, amt)
  }

  pub fn set_errored(&self, amt: usize) -> usize {
    utils::set_atomic(&self.errored, amt)
  }

  pub fn set_success(&self, amt: usize) -> usize {
    utils::set_atomic(&self.success, amt)
  }

  pub fn read_scanned(&self) -> usize {
    utils::read_atomic(&self.scanned)
  }

  pub fn read_skipped(&self) -> usize {
    utils::read_atomic(&self.skipped)
  }

  pub fn read_errored(&self) -> usize {
    utils::read_atomic(&self.errored)
  }

  pub fn read_success(&self) -> usize {
    utils::read_atomic(&self.success)
  }
}

#[macro_export]
macro_rules! check_quiet {
  () => {
    if progress::quiet_output() {
      return;
    }
  };
}

#[macro_export]
macro_rules! status {
  ($msg:expr) => {
    if !progress::quiet_output() {
      progress::global_progress().status.set_message($msg);
    }
  };
  ($prefix:expr, $msg:expr) => {
    if !progress::quiet_output() {
      progress::global_progress().status.set_prefix($prefix);
      progress::global_progress().status.set_message($msg);
    }
  };
}

pub struct ServerBars {
  pub bars:    HashMap<Server, ProgressBar>,
  pub updated: HashMap<Server, Instant>,
}

impl ServerBars {
  pub fn add_bar(&mut self, server: Server, bar: ProgressBar) {
    self.updated.insert(server.clone(), Instant::now());
    self.bars.insert(server, bar);
  }

  pub fn try_update_bar<F>(&mut self, server: &Server, func: F)
  where
    F: FnOnce(&ProgressBar),
  {
    let should_update = if min_refresh_delay() > 0 {
      let now = Instant::now();

      if let Some(last_updated) = self.updated.get(server) {
        let dur = now
          .checked_duration_since(*last_updated)
          .unwrap_or_else(|| Duration::from_millis(0));

        if dur.as_millis() >= min_refresh_delay() as u128 {
          self.updated.insert(server.clone(), now);
          true
        } else {
          false
        }
      } else {
        self.updated.insert(server.clone(), now);
        false
      }
    } else {
      true
    };

    if should_update {
      if let Some(bar) = self.bars.get(server) {
        func(bar);
      }
    }
  }
}

pub struct Progress {
  pub multi:    MultiProgress,
  pub bars:     RwLock<ServerBars>,
  pub status:   ProgressBar,
  pub totals:   ProgressBar,
  pub postgres: RwLock<Option<ProgressBar>>,
}

impl Default for Progress {
  fn default() -> Self {
    let multi = MultiProgress::new();
    let bars = RwLock::new(ServerBars {
      bars:    HashMap::new(),
      updated: HashMap::new(),
    });

    let status_style = ProgressStyle::with_template(STATUS_BAR_STYLE_TEMPLATE).expect("Failed to create status bar");
    let total_style =
      ProgressStyle::with_template(SPINNER_BAR_STYLE_TEMPLATE).expect("Failed to create counter template");

    let status = multi.add(ProgressBar::new_spinner());
    if !quiet_output() {
      status.enable_steady_tick(StdDuration::from_millis(1000));
    }
    status.set_style(status_style);
    let totals = multi.add(ProgressBar::new_spinner());
    totals.set_style(total_style);
    totals.set_prefix("[Totals]");
    if !quiet_output() {
      totals.enable_steady_tick(StdDuration::from_millis(1000));
    }

    Progress {
      multi,
      bars,
      status,
      totals,
      postgres: RwLock::new(None),
    }
  }
}

impl Progress {
  pub fn add_server(&self, server: &Server, estimate: Option<u64>, prefix: Option<&str>) {
    check_quiet!();

    let style = if estimate.is_some() {
      ProgressStyle::with_template(COUNTER_BAR_STYLE_TEMPLATE).expect("Failed to create counter template")
    } else {
      ProgressStyle::with_template(SPINNER_BAR_STYLE_TEMPLATE).expect("Failed to create spinner template")
    };
    let bar = if let Some(est) = estimate {
      self.multi.insert_after(&self.status, ProgressBar::new(est))
    } else {
      self.multi.insert_after(&self.status, ProgressBar::new_spinner())
    };

    let prefix = if let Some(prefix) = prefix {
      format!("{} {}", prefix, server)
    } else {
      format!("{}", server)
    };
    bar.set_prefix(prefix);
    bar.set_style(style);
    self.bars.write().add_bar(server.clone(), bar);
  }

  pub fn set_postgres_estimate(&self, estimate: u64) {
    check_quiet!();

    let psql_style = ProgressStyle::with_template(COUNTER_BAR_STYLE_TEMPLATE)
      .expect("Failed to create postgres template progress bar");
    let postgres = self.multi.add(ProgressBar::new_spinner());
    postgres.set_style(psql_style);
    postgres.set_prefix("[PostgreSQL]");
    if !quiet_output() {
      postgres.enable_steady_tick(StdDuration::from_millis(1000));
    }

    postgres.set_length(estimate);
    *self.postgres.write() = Some(postgres);
  }

  pub fn incr_postgres(&self, amt: u64) {
    check_quiet!();

    if let Some(bar) = self.postgres.read().as_ref() {
      bar.inc(amt);
    }
  }

  pub fn update_estimate(&self, server: &Server, estimate: u64) {
    check_quiet!();

    if let Some(bar) = self.bars.write().bars.get_mut(server) {
      bar.set_length(estimate);
    }
  }

  #[allow(dead_code)]
  pub fn remove_server(&self, server: &Server) {
    check_quiet!();

    if let Some(bar) = self.bars.write().bars.remove(server) {
      self.multi.remove(&bar);
      bar.finish_and_clear();
    }
  }

  pub fn update(&self, server: &Server, message: impl Into<Cow<'static, str>>, position: Option<u64>) {
    check_quiet!();

    self.bars.write().try_update_bar(server, move |bar| {
      if let Some(pos) = position {
        if bar.length().is_some() {
          bar.set_position(pos);
        }
      }

      bar.set_message(message);
    });
  }

  pub fn update_totals(&self, counters: &Counters) {
    check_quiet!();
    self.totals.set_message(format!("{}", counters));
  }

  pub fn finish(&self, server: &Server, message: impl Into<Cow<'static, str>>) {
    check_quiet!();

    if let Some(bar) = self.bars.read().bars.get(server) {
      bar.finish_with_message(message);
    }
  }

  #[allow(dead_code)]
  pub fn clear(&self) {
    check_quiet!();

    for (_, bar) in self.bars.write().bars.drain() {
      bar.finish_and_clear();
    }
    if let Some(bar) = self.postgres.write().as_ref() {
      bar.finish_and_clear();
    }
    if let Err(e) = self.multi.clear() {
      debug!("Error clearing progress bars: {:?}", e);
    }
  }
}

pub fn setup_event_logs(client: &RedisClient) -> JoinHandle<()> {
  let client = client.clone();

  tokio::spawn(async move {
    // these clients always use centralized configs to specific servers
    let server = client
      .client_config()
      .server
      .hosts()
      .pop()
      .expect("Failed to read centralized config");

    let mut errors = client.error_rx();
    let mut reconnections = client.reconnect_rx();
    let mut unresponsive = client.unresponsive_rx();

    loop {
      tokio::select! {
        e = errors.recv() => {
          if quiet_output() {
            error!("{} Disconnected {:?}", server, e);
          }else{
            global_progress().update(&server, format!("Disconnected: {:?}", e), None);
          }
        },
        _ = reconnections.recv() => {
          if quiet_output() {
            error!("{} Reconnected", server);
          }else{
            global_progress().update(&server, "Reconnected", None);
          }
        },
        _ = unresponsive.recv() => {
          if quiet_output() {
            error!("{} Unresponsive connection", server);
          }else{
            global_progress().update(&server, "Unresponsive connection.", None);
          }
        }
      }
    }
  })
}

pub fn watch_totals(counters: &Arc<Counters>) -> JoinHandle<()> {
  let counters = counters.clone();

  tokio::spawn(async move {
    if !quiet_output() {
      loop {
        global_progress().update_totals(&counters);
        sleep(Duration::from_secs(1)).await;
      }
    }
  })
}
