#!/bin/bash
set -e

plasma-store-server -m 1000000 -s /tmp/plasma &
PLASMASERVER_PID=$!
./gradlew build --quiet
kill $PLASMASERVER_PID