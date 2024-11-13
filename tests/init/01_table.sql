CREATE TABLE IF NOT EXISTS counters (
    created timestamp with time zone not null default now(),
    counter int not null default 0,
    org_id int not null,
    user_id int not null
);