#!/usr/bin/env bash

# tail latest log file
latestlog=$(ls -ltr ./logs | awk 'END{print $9}')
tail -f "./logs/$latestlog"
