#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

export STAT_DIR="$TEST_HOME/stat_dir"
source $TEST_HOME/config/default.conf

python3 $TEST_HOME/../test_driver.py \
	--server_addr $SERVER_ADDR \
	--username $USERNAME \
	--password $PASSWORD \
	--dashboard $DASHBOARD \
	--viewport_range $VIEWPORT_RANGE \
	--shift_step $SHIFT_STEP \
	--read_behavior $READ_BEHAVIOR \
	--viewport_start $VIEWPORT_START \
	--write_behavior $WRITE_BEHAVIOR \
	--refresh_interval $REFRESH_INTERVAL \
	--num_refresh $NUM_REFRESH \
	--mvc_property $MVC_PROPERTY \
	--opt_viewport $OPT_VIEWPORT \
	--opt_exec_time $OPT_EXEC_TIME \
	--opt_skip_write $OPT_SKIP_WRITE \
	--stat_dir $STAT_DIR \
	--db_name $DB_NAME \
	--db_username $DB_USERNAME \
	--db_password $DB_PASSWORD \
	--db_host $DB_HOST \
	--db_port $DB_PORT
