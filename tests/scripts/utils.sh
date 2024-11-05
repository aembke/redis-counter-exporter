#!/bin/bash

# Returns 0 if not installed, 1 otherwise.
function check_redis {
  if [ -d "$PWD/tests/tmp/redis_$REDIS_VERSION/redis-$REDIS_VERSION" ]; then
    echo "Skipping redis install."
    return 1
  else
    echo "Redis install not found."
    return 0
  fi
}

function install_redis {
  echo "Installing..."
  pushd $PWD > /dev/null
  rm -rf tests/tmp/redis_cluster_$REDIS_VERSION
  cd tests/tmp

  if [ -z "$USE_VALKEY" ]; then
    echo "Installing Redis from redis.io"
    curl -O "http://download.redis.io/releases/redis-$REDIS_VERSION.tar.gz"
  else
    echo "Installing valkey from github"
    curl -O -L "https://github.com/valkey-io/valkey/archive/refs/tags/redis-$REDIS_VERSION.tar.gz" --output redis-$REDIS_VERSION.tar.gz
  fi

  mkdir redis_$REDIS_VERSION
  tar xf redis-$REDIS_VERSION.tar.gz -C redis_$REDIS_VERSION
  rm redis-$REDIS_VERSION.tar.gz

  if [ -z "$USE_VALKEY" ]; then
    cd redis_$REDIS_VERSION/redis-$REDIS_VERSION
  else
    mv redis_$REDIS_VERSION/valkey-redis-$REDIS_VERSION redis_$REDIS_VERSION/redis-$REDIS_VERSION
    cd redis_$REDIS_VERSION/redis-$REDIS_VERSION
  fi

  make BUILD_TLS=yes -j"${PARALLEL_JOBS}"
  cp redis.conf redis.conf.bk
  popd > /dev/null
}