#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "usage: .. <repo> <branch>"
    exit 1
fi

docker volume create confluent_kafka_dotnet
# note - this will not build (generates errors) for .NET Framework targets
docker run -it \
    --name=build \
    --network=host \
    --rm \
    -v confluent_kafka_dotnet:/git \
    mcr.microsoft.com/dotnet/core/sdk:2.1.504-stretch \
    bash -c "cd /git; git clone --branch $2 https://github.com/$1/confluent-kafka-dotnet.git; cd confluent-kafka-dotnet; dotnet build; exit"
