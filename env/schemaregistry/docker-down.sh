#!/bin/bash

docker kill schema_registry
docker rm schema_registry

docker kill schema_registry_ssl
docker rm schema_registry_ssl
docker volume rm schema_registry_ssl