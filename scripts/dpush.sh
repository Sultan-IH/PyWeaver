#!/usr/bin/env bash
version=$(grep "version:" $CONFIG_FILE_PATH | cut -c 10-)
echo "Building version number: " $version
docker push $DOCKER_REPO:$version