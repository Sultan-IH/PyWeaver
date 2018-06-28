#!/usr/bin/env bash

# read the CONFIG_FILE_PATH and deploy to servers there
# args to replace are as follows: docker_repo, wanted version
version=$(grep "version:" $CONFIG_FILE_PATH | cut -c 10-)
ssh sultan@159.65.27.43 'bash -s' < ./scripts/replace.sh $DOCKER_REPO $version
