use crate::{
  argv::Argv,
  index::{ColumnKind, Extractor, Index, Key},
  progress::{self, Progress},
  status,
};
use chrono::{DateTime, Utc};
use fred::{
  bytes::BytesMut,
  error::{Error as RedisError, ErrorKind},
};
use log::{debug, error, trace};
use std::{cmp, error::Error, sync::Arc, time::Duration};
use tokio_postgres::{
  types::{Format, IsNull, ToSql, Type},
  Client,
};

#[derive(Debug)]
enum Param {
  Date(DateTime<Utc>),
  String(String),
  Number(i32),
}

impl ToSql for Param {
  fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
  where
    Self: Sized,
  {
    match self {
      Param::Date(dt) => dt.to_sql(ty, out),
      Param::String(s) => s.to_sql(ty, out),
      Param::Number(i) => i.to_sql(ty, out),
    }
  }

  fn accepts(ty: &Type) -> bool
  where
    Self: Sized,
  {
    matches!(*ty, Type::ANY)
  }

  fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
    match self {
      Param::Date(dt) => dt.to_sql_checked(ty, out),
      Param::String(s) => s.to_sql_checked(ty, out),
      Param::Number(i) => i.to_sql_checked(ty, out),
    }
  }

  fn encode_format(&self, _ty: &Type) -> Format {
    match self {
      Param::Date(dt) => dt.encode_format(_ty),
      Param::String(s) => s.encode_format(_ty),
      Param::Number(i) => i.encode_format(_ty),
    }
  }
}

fn build_sql(argv: &Argv, extractors: &[Extractor], values_len: usize) -> String {
  let columns: Vec<String> = extractors.iter().map(|e| e.name.clone()).collect();
  let mut values: Vec<String> = Vec::with_capacity(values_len);
  let mut sql_idx = 1;
  for _ in 0 .. values_len {
    let mut inner = Vec::with_capacity(extractors.len() + 2);
    inner.push(format!("${}", sql_idx));
    inner.push(format!("${}", sql_idx + 1));
    for i in 0 .. extractors.len() {
      inner.push(format!("${}", sql_idx + i + 2));
    }

    values.push(format!("({})", inner.join(",")));
    sql_idx += extractors.len() + 2;
  }

  format!(
    "INSERT INTO {} ({}, {}, {}) VALUES {}",
    argv.psql_table,
    argv.psql_date_col,
    argv.psql_count_col,
    columns.join(", "),
    values.join(", ")
  )
}

fn build_bindings(
  batch: Vec<(Key, u64)>,
  extractors: &[Extractor],
  now: &DateTime<Utc>,
) -> Result<Vec<Param>, RedisError> {
  let mut out = Vec::with_capacity(batch.len() * (2 + extractors.len()));
  for (key, value) in batch.into_iter() {
    out.push(Param::Date(now.clone()));
    out.push(Param::Number(value as i32));
    for (idx, part) in key.0.into_iter().enumerate() {
      out.push(match extractors[idx].kind {
        ColumnKind::String => Param::String(part),
        ColumnKind::Integer => Param::Number(part.parse::<i32>()?),
      });
    }
  }

  Ok(out)
}

pub async fn init_sql(client: &Client, statements: Vec<String>) -> Result<(), RedisError> {
  if statements.is_empty() {
    return Ok(());
  }
  status!(format!("Running {} init statements...", statements.len()));

  for statement in statements.into_iter() {
    debug!("Running SQL init: {}", statement);
    if let Err(e) = client.execute(&statement, &[]).await {
      return Err(RedisError::new(ErrorKind::Unknown, format!("PostgreSQL init: {:?}", e)));
    }
  }
  Ok(())
}

pub async fn save(
  argv: &Arc<Argv>,
  progress: &Arc<Progress>,
  index: Arc<Index>,
  client: Client,
) -> Result<(), RedisError> {
  let index_len = index.len();
  status!(format!("Saving {} results...", index_len));
  progress.set_postgres_estimate(index_len as u64);

  let now = Utc::now();
  let mut indexed = index.drain();
  let extractors = index.extractors();
  while !indexed.is_empty() {
    let batch_size = cmp::min(indexed.len(), argv.psql_batch as usize);
    let mut batch = Vec::with_capacity(batch_size);
    for _ in 0 .. batch_size {
      batch.push(indexed.pop().unwrap());
    }
    let sql = build_sql(&argv, extractors, batch.len());
    let bindings = build_bindings(batch, &extractors, &now)?;
    trace!("Sending SQL: {}, with bindings: {:?}", sql, bindings);
    let params: Vec<_> = bindings.iter().map(|s| s as &(dyn ToSql + Sync)).collect();

    if let Err(e) = client.execute(&sql, params.as_slice()).await {
      error!("Error writing to PostgreSQL: {:?}", e);
      if argv.quiet {
        eprintln!("Error writing to PostgreSQL: {:?}", e);
      } else {
        status!(format!("Error writing to PostgreSQL: {:?}", e));
      }
      if argv.ignore {
        continue;
      } else {
        return Err(RedisError::new(ErrorKind::Unknown, format!("{:?}", e)));
      }
    }

    progress.incr_postgres(batch_size as u64);
    if let Some(delay) = argv.psql_delay {
      tokio::time::sleep(Duration::from_millis(delay)).await;
    }
  }

  if let Some(bar) = progress.postgres.read().as_ref() {
    bar.finish();
  }
  Ok(())
}
