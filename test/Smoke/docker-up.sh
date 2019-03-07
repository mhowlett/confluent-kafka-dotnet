#!/bin/bash

ADVERTISED_IP=$ADVERTISED_IP ./zookeeper/docker-up.sh
ADVERTISED_IP=$ADVERTISED_IP ./kafka/docker-up.sh
ADVERTISED_IP=$ADVERTISED_IP ./schemaregistry/docker-up.sh
ADVERTISED_IP=$ADVERTISED_IP ./prometheus/docker-up.sh
