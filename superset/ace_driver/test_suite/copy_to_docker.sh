#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

docker cp $TEST_HOME/test_dash_config_viewport.sh superset:../ACE/ACE/superset/ace_driver/test_suite/
docker cp $TEST_HOME/test_read_behavior.sh superset:../ACE/ACE/superset/ace_driver/test_suite/
docker cp $TEST_HOME/test_k_relaxed.sh superset:../ACE/ACE/superset/ace_driver/test_suite/
docker cp $TEST_HOME/test_dash_config_shift.sh superset:../ACE/ACE/superset/ace_driver/test_suite/
