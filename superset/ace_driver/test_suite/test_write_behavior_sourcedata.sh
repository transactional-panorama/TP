#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

REPORT_HOME="$TEST_HOME/experiment_results/write_behavior/source_data_change"
source $TEST_HOME/config/default.conf

declare -a START_RUN=1
declare -a END_RUN=3
declare -a REFRESH_INTERVAL_OPTIONS=(40 45 50 55 60)
declare -a MVC_PROPERTY_OPTIONS=(1 2 3 4)
WRITE_BEHAVIOR="source_data_change"
NUM_REFRESH=3

for RUN in `seq $START_RUN $END_RUN`
do
    for REFRESH_INTERVAL in "${REFRESH_INTERVAL_OPTIONS[@]}"
    do
    	for MVC_PROPERTY in "${MVC_PROPERTY_OPTIONS[@]}"
    	do
    	    STAT_DIR="$REPORT_HOME/INTERVAL$REFRESH_INTERVAL/MVC$MVC_PROPERTY/RUN$RUN"
    	    mkdir -p $STAT_DIR
    	    rm -f $STAT_DIR/*

	    timeout 30m python3 $TEST_HOME/../test_driver.py \
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

	    now="$(date)"
            echo "$now: Finished, $REPORT_HOME/INTERVAL$REFRESH_INTERVAL/MVC$MVC_PROPERTY/RUN$RUN" >> $TEST_HOME/test.log
    	done
    done
done 

