#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

SERVER_ADDR="localhost:8088"
USERNAME="admin"
PASSWORD="admin"
DASHBOARD="TPCH"
READ_BEHAVIOR="not_change" # options: ["not_change", "regular_change", "see_change"]
WRITE_BEHAVIOR="source_data_change" # options: ["filter_change", "source_data_change"]
REFRESH_INTERVAL=30
NUM_REFRESH=1
MVC_PROPERTY=2 # optinos: [1: MV, 2: MVCC, 3: MCM, 4: MCF]
OPT_VIEWPORT="True"
OPT_EXEC_TIME="True"
OPT_SKIP_WRITE="True"
STAT_DIR="$TEST_HOME/stat_dir"
DB_NAME="superset"
DB_USERNAME="totemtang"
DB_PASSWORD="1234"
DB_HOST="localhost"
DB_PORT="5432"

python3 $TEST_HOME/../test_driver.py \
	--server_addr $SERVER_ADDR \
	--username $USERNAME \
	--password $PASSWORD \
	--dashboard $DASHBOARD \
	--read_behavior $READ_BEHAVIOR \
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
