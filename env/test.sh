#!/bin/bash

docker volume create confluent_kafka_dotnet
docker run -it \
    --name=test \
    --network=host \
    --rm \
    -v confluent_kafka_dotnet:/git \
    mcr.microsoft.com/dotnet/core/sdk:2.1.504-stretch \
    bash
