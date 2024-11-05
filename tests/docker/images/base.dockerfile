# https://github.com/docker/for-mac/issues/5548#issuecomment-1029204019
# FROM rust:1.82-slim-buster
FROM rust:1.82-slim-bullseye
WORKDIR /project

ARG RUST_LOG
ARG REDIS_VERSION
ARG REDIS_USERNAME
ARG REDIS_PASSWORD
ARG PSQL_USERNAME
ARG PSQL_PASSWORD

# specific to my laptop
# RUN useradd -m -u 501 -g root host_user

RUN USER=root apt-get update && apt-get install -y build-essential libssl-dev dnsutils curl pkg-config cmake ca-certificates gnupg
RUN USER=root curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null
RUN USER=root sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/postgresql.list'
RUN USER=root apt-get update && apt-get install -y postgresql-client

RUN echo "REDIS_VERSION=$REDIS_VERSION"
# For debugging
RUN cargo --version && rustc --version