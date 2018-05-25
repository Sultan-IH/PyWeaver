#!/usr/bin/env bash

id=$(docker ps | awk 'FNR > 1 {print $1}') # where name matches the progam name
docker kill --signal SIGINT id
docker pull ksula0155/pyweaver:$1
docker run -v tarantula_logs:/logs -d ksula0155/pyweaver:$1
