#!/bin/bash

HOST_DATA_PATH='/home/totemtang/ACE/docker/pg_data'
CONTAINER_DATA_PATH='/var/lib/postgresql/data'

docker run --name postgres --network host \
	-e POSTGRES_PASSWORD=1234 \
	-e PGDATA=$CONTAINER_DATA_PATH/pg_data \
	-v $HOST_DATA_PATH:$CONTAINER_DATA_PATH \
       	-d postgres:10.21
