#!/bin/bash

docker volume create zk1_data
docker run -d \
    --name zookeeper_1 \
    --network=host \
    -v zk1_data:/data/zookeeper \
    -e KAFKA_HEAP_OPTS="-Xmx256M -Xms256M" \
    -e PROMETHEUS_PORT=7062 \
    -e CLIENT_PORT=2181 \
    -e CLIENT_PORT_ADDRESS=127.0.0.1 \
    -e MY_ID=1 \
    confluentinc/dotnet_test_zookeeper:1
