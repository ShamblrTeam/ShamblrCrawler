#!/bin/bash
until $1; do
    echo "'$1' crashed with exit code $?. Restarting..." >&2
    sleep 1
done
