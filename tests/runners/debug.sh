#!/bin/bash

# boot all the redis servers and start a bash shell on a new container
docker-compose -f tests/docker/compose/psql.yml \
  -f tests/docker/compose/redis.yml \
  -f tests/docker/compose/debug.yml run --rm debug