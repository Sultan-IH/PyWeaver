#!/usr/bin/env bash
version=$(grep "version:" PyWeaver.config.yaml | cut -c 10-)
echo "Building version number: " $version
docker push ksula0155/pyweaver:$version