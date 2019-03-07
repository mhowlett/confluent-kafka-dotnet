#!/bin/bash

docker volume create prometheus

docker run -d \
    --name=prometheus \
    --network=host \
    -v prometheus:/data/prometheus \
    -e SCRAPE_INTERVAL="2s" -e EVAL_INTERVAL="2s" \
    -e ZK_IP=127.0.0.1 -e ZK_METRICS_PORT=7062 \
    -e KAFKA_IP_1=127.0.0.1 -e KAFKA_METRICS_PORT_1=7071 \
    -e KAFKA_IP_2=127.0.0.1 -e KAFKA_METRICS_PORT_2=7072 \
    -e KAFKA_IP_3=127.0.0.1 -e KAFKA_METRICS_PORT_3=7073 \
    confluentinc/dotnet_test_prometheus:1
