#!/bin/sh
# Runs one statement against the shardlab (127.0.0.1:44200 only).
# usage: ./sql.sh "SELECT ..."
jq -nc --arg s "$1" '{stmt:$s}' | curl -s -m 30 -H 'Content-Type: application/json' http://127.0.0.1:44200/_sql -d @-
