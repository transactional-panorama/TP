#!/bin/bash

docker run --name superset \
       	--network host \
       	-d ace-server
