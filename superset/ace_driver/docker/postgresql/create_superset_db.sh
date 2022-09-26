#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

docker exec -it postgres psql -h localhost -U postgres -d postgres -f $TEST_HOME/create.sql


