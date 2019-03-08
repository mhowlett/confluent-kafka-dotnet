#!/bin/bash

export ADVERTISED_IP=$ADVERTISED_IP
./zookeeper/docker-up.sh
./kafka/docker-up.sh
./schemaregistry/docker-up.sh
./prometheus/docker-up.sh
