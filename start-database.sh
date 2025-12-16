#!/usr/bin/env bash
# Use this script to start a docker container for a local development database

POSTGRES_DB=test
POSTGRES_USER=test
POSTGRES_PASSWORD=test
POSTGRES_PORT=5432

DB_CONTAINER_NAME=asyncpg-datalayer-postgres-16

if [ "$(docker ps -aq -f name="${DB_CONTAINER_NAME}")" ]; then
  docker rm -f "${DB_CONTAINER_NAME}"
fi

docker run -it \
  --name "${DB_CONTAINER_NAME}" \
  -e "POSTGRES_USER=${POSTGRES_USER}" \
  -e "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
  -e "POSTGRES_DB=${POSTGRES_DB}" \
  -p "${POSTGRES_PORT}:5432" \
  "postgres:16-alpine"
