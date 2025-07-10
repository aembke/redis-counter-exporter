use crate::{argv::Argv, utils};
use fred::{
  error::{Error, ErrorKind},
  types::Key as RedisKey,
};
use parking_lot::Mutex;
use regex::Regex;
use std::{
  cmp::Ordering,
  collections::HashMap,
  fmt,
  hash::{Hash, Hasher},
  ops::Deref,
  sync::Arc,
};

#[derive(Debug)]
pub enum ColumnKind {
  String,
  Integer,
}

impl Default for ColumnKind {
  fn default() -> Self {
    ColumnKind::String
  }
}

impl From<&str> for ColumnKind {
  fn from(value: &str) -> Self {
    match value {
      "int" | "INT" => ColumnKind::Integer,
      _ => ColumnKind::String,
    }
  }
}

#[derive(Debug)]
pub struct Extractor {
  pub kind: ColumnKind,
  pub regex: Regex,
  pub name: String,
}

impl Ord for Extractor {
  fn cmp(&self, other: &Self) -> Ordering {
    self.name.cmp(&other.name)
  }
}

impl PartialOrd for Extractor {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl PartialEq for Extractor {
  fn eq(&self, other: &Self) -> bool {
    self.name == other.name
  }
}

impl Eq for Extractor {}

impl Extractor {
  pub fn from_argv(argv: &Argv) -> Vec<Self> {
    argv
      .extractors
      .iter()
      .map(|s| {
        let parts: Vec<&str> = s.splitn(2, "=").map(|s| s.trim()).collect();
        if parts.len() != 2 {
          panic!("Invalid extractor argument: {}", s);
        }
        let (kind, name) = if parts[0].contains("::") {
          let inner: Vec<_> = parts[0].split("::").collect();
          if inner.len() == 2 {
            (ColumnKind::from(inner[1]), inner[0].to_string())
          } else {
            (ColumnKind::default(), parts[0].to_string())
          }
        } else {
          (ColumnKind::default(), parts[0].to_string())
        };

        let regex = Regex::new(parts[1]).expect("Invalid regex pattern");
        Extractor { regex, name, kind }
      })
      .collect()
  }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Key(pub(crate) Vec<String>);

impl Hash for Key {
  fn hash<H: Hasher>(&self, state: &mut H) {
    for part in self.0.iter() {
      part.hash(state)
    }
  }
}

pub struct Index {
  delimiter: String,
  filter_missing: bool,
  extractors: Vec<Extractor>,
  inner: Mutex<HashMap<Key, u64>>,
}

impl fmt::Debug for Index {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "{:?}", self.inner.lock().deref())
  }
}

impl Index {
  pub fn new(argv: &Argv, mut extractors: Vec<Extractor>) -> Arc<Index> {
    extractors.sort();

    Arc::new(Index {
      delimiter: argv.extractor_delimiter.clone(),
      filter_missing: argv.filter_missing_groups,
      extractors,
      inner: Mutex::new(HashMap::with_capacity(argv.initial_index_size)),
    })
  }

  pub fn upsert(&self, key: &RedisKey, value: u64) -> Result<bool, Error> {
    let mut parts = Vec::with_capacity(self.extractors.len());
    for extractor in self.extractors.iter() {
      if let Some(group) = utils::regexp_capture(&extractor.regex, key, &self.delimiter) {
        parts.push(group);
      } else if self.filter_missing {
        return Ok(false);
      } else {
        return Err(Error::new(
          ErrorKind::Unknown,
          "Missing group to extract from Redis key.",
        ));
      }
    }
    let key = Key(parts);

    let mut guard = self.inner.lock();
    let old = guard.remove(&key).unwrap_or(0);
    guard.insert(key, old + value);
    Ok(true)
  }

  pub fn extractors(&self) -> &[Extractor] {
    &self.extractors
  }

  pub fn len(&self) -> usize {
    self.inner.lock().len()
  }

  pub fn drain(&self) -> Vec<(Key, u64)> {
    self.inner.lock().drain().collect()
  }
}
