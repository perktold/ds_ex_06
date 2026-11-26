#!/bin/bash
./gradlew installDist || exit 1
BIN=build/install/ds_ex_06/bin/ds_ex_06
docker exec dapr_redis redis-cli FLUSHALL # clean redis container every run
dapr run --app-id manager -- $BIN manager &
sleep 5   # wait for manager
dapr run --app-id client1 -- $BIN client 1 &
dapr run --app-id client2 -- $BIN client 2 &
dapr run --app-id client3 -- $BIN client 3 &
