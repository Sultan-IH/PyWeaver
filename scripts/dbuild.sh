#!/usr/bin/env bash
version=$(grep "version:" PyWeaver.config.yaml | cut -c 10-)
echo "Building version number: " $version

docker build -f ./docker/Dockerfile -t ksula0155/pyweaver:$version .