#!/bin/bash

set -ev

for adapter in mysql jdbc_mysql postgresql oracle; do
  bin/rake spec MONDRIAN_DRIVER=$adapter
done
