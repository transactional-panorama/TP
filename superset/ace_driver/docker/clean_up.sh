#!/bin/bash

docker rm $(docker ps -a -f status=exited -f status=created -q)
docker image rm ace-server:1.0.0
