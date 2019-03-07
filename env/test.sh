#!/bin/bash

docker run -it \
    --name=build \
    --network=host \
    --rm \
    -v confluent_kafka_dotnet:/git \
    mcr.microsoft.com/dotnet/core/sdk:2.1.504-stretch \
    bash -c "cd /git/confluent-kafa-dotnet/test/Confluent.Kafka.IntegrationTests; dotnet test -f netcoreapp2.1"
