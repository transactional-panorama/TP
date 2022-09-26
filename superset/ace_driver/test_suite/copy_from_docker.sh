#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

docker cp superset:../ACE/ACE/superset/ace_driver/test_suite/experiment_results $TEST_HOME/
