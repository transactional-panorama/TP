#!/bin/bash

ABS_PATH="$(readlink -f "${BASH_SOURCE}")"
TEST_HOME="$(dirname $ABS_PATH)"

HOST_DATA_PATH=$TEST_HOME/pg_data
CONTAINER_DATA_PATH=/var/lib/postgresql/data
CNAME=postgres

docker run --name $CNAME --network host \
	-e POSTGRES_PASSWORD=1234 \
	-e PGDATA=$CONTAINER_DATA_PATH/pg_data \
	-v $HOST_DATA_PATH:$CONTAINER_DATA_PATH \
       	-d postgres:10.21

cp -r $TEST_HOME/postgresql/scripts $HOST_DATE_PATH/

# Create the superset db and a new role
docker exec -it $CNAME \
       psql -h localhost \
       -U postgres \
       -d postgres \
       -f $CONTAINER_DATA_PATH/scripts/create_supreset.sql

# Load TPC-H data
docker exec -it $CNAME \
	psql -h localhost \
	-U totemtang \
	-d superset \
	-f $CONTAINER_DATA_PATH/scripts/create_tpch.sql

docker exec -it $CNAME \
	psql -h localhost \
	-U totemtang \
	-d superset \
	-f $CONTAINER_DATA_PATH/scripts/create_tpch_full.sql

docker exec -it $CNAME \
	psql -h localhost \
	-U totemtang \
	-d superset \
	-f $CONTAINER_DATA_PATH/scripts/load_tpch.sql

docker exec -it $CNAME \
	psql -h localhost \
	-U totemtang \
	-d superset \
	-f $CONTAINER_DATA_PATH/scripts/load_tpch_full.sql


