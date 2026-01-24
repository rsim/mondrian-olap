#!/bin/bash

set -ev

# Wait for ClickHouse to be ready
until clickhouse-client --host localhost --query "SELECT 1" 2>/dev/null; do
  echo "Waiting for ClickHouse to be ready..."
  sleep 2
done

# Create database and user for tests
clickhouse-client --host localhost --multiquery <<SQL
CREATE USER IF NOT EXISTS mondrian_test IDENTIFIED WITH plaintext_password BY 'mondrian_test';
CREATE DATABASE IF NOT EXISTS mondrian_test;
GRANT ALL ON mondrian_test.* TO mondrian_test;
SQL
