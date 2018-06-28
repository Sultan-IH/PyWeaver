#!/usr/bin/env bash

id=$(docker ps | grep "$1" | awk '{ print $1 }') # where name matches the program name
docker kill --signal=SIGINT id
cd $1 # change the current directory to the thingy
docker pull ksula0155/$1:$2 # pull the latest release of the wanted version
source ./launch.sh $2 # launch it
