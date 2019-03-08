#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "usage: .. <confluent-kafka-dotnet path>"
fi

docker volume create confluent_kafka_dotnet

docker run -it \
    --name=build \
    --network=host \
    --rm \
    -v confluent_kafka_dotnet:/git \
    mcr.microsoft.com/dotnet/core/sdk:2.1.504-stretch \
    bash -c "sleep 60";

docker cp $1 confluent_kafka_dotnet

docker kill build
