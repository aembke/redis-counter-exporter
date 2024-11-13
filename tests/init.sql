CREATE TABLE IF NOT EXISTS counters (
    created timestamp with time zone not null default now(),
    counter int not null default 0,
    org_id int not null,
    user_id int not null
);

CREATE INDEX ON counters (org_id);
CREATE INDEX ON counters (user_id);
CREATE INDEX ON counters (counter);
CREATE INDEX ON counters (created);

RUST_LOG=trace cargo run --release -- --redis-host redis-cluster-1 --redis-port 30001 --redis-cluster --pattern "*" -e "org_id::int=\w+:(\w+):\w+" -e "user_id::int=\w+:\w+:(\w+)" --psql-host psql --psql-port 5432 --psql-db redis-counter-exporter --psql-table counters