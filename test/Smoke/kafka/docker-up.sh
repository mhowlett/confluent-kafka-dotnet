#!/bin/bash

docker volume create kafka1_logs
docker run -d \
    --name kafka_1 \
    --network=host \
    -v kafka1_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e PROMETHEUS_PORT=7071 \
    -e BROKER_ID=0 \
    -e BROKER_IP=0.0.0.0 -e BROKER_PORT=9092 \
    -e ZK_IP_1=127.0.0.1 -e ZK_PORT_1=2181 \
    confluentinc/dotnet_test_kafka:1


docker volume create kafka2_logs
docker run -d \
    --name kafka_2 \
    --network=host \
    -v kafka2_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e PROMETHEUS_PORT=7072 \
    -e BROKER_ID=1 \
    -e BROKER_IP=0.0.0.0 -e BROKER_PORT=9093 \
    -e ZK_IP_1=127.0.0.1 -e ZK_PORT_1=2181 \
    confluentinc/dotnet_test_kafka:1


docker volume create kafka3_logs
docker run -d \
    --name kafka_3 \
    --network=host \
    -v kafka3_logs:/data/kafka-logs \
    -e KAFKA_HEAP_OPTS="-Xmx1G -Xms1G" \
    -e PROMETHEUS_PORT=7073 \
    -e BROKER_ID=2 \
    -e BROKER_IP=0.0.0.0 -e BROKER_PORT=9094 \
    -e ZK_IP_1=127.0.0.1 -e ZK_PORT_1=2181 \
    confluentinc/dotnet_test_kafka:1

