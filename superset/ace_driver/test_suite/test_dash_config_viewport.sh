#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

REPORT_HOME="$TEST_HOME/experiment_results/dash_config/viewport_range"
source $TEST_HOME/config/default.conf

CHART_NUM=22
declare -a START_RUN=1
declare -a END_RUN=10
declare -a VIEWPORT_RANGE_OPTIONS=(4 10 16 22)
declare -a MVC_PROPERTY_OPTIONS=(1 2 3 4 5)

for RUN in `seq $START_RUN $END_RUN`
do
    for VIEWPORT_RANGE in "${VIEWPORT_RANGE_OPTIONS[@]}"
    do
        VIEWPORT_START=$(( $RANDOM % ($CHART_NUM - $VIEWPORT_RANGE + 1) ))
    	for MVC_PROPERTY in "${MVC_PROPERTY_OPTIONS[@]}"
    	do
    	    STAT_DIR="$REPORT_HOME/VIEWPORT$VIEWPORT_RANGE/MVC$MVC_PROPERTY/RUN$RUN"
    	    mkdir -p $STAT_DIR
    	    rm -f $STAT_DIR/*

	    timeout 30m python3 $TEST_HOME/../test_driver.py \
		--server_addr $SERVER_ADDR \
		--username $USERNAME \
		--password $PASSWORD \
		--dashboard $DASHBOARD \
		--viewport_range $VIEWPORT_RANGE \
		--shift_step $SHIFT_STEP \
		--explore_range $EXPLORE_RANGE \
		--read_behavior $READ_BEHAVIOR \
		--viewport_start $VIEWPORT_START \
		--write_behavior $WRITE_BEHAVIOR \
		--refresh_interval $REFRESH_INTERVAL \
		--num_refresh $NUM_REFRESH \
		--mvc_property $MVC_PROPERTY \
		--k_relaxed $K_RELAXED \
		--opt_viewport $OPT_VIEWPORT \
		--opt_exec_time $OPT_EXEC_TIME \
		--opt_skip_write $OPT_SKIP_WRITE \
		--stat_dir $STAT_DIR \
		--db_name $DB_NAME \
		--db_username $DB_USERNAME \
		--db_password $DB_PASSWORD \
		--db_host $DB_HOST \
		--db_port $DB_PORT \
		--sf $SF

	    now="$(date)"
            echo "$now: Finished, VIEWPORT_START: $VIEWPORT_START; $STAT_DIR" >> $TEST_HOME/test.log
    	done
    done
done 

