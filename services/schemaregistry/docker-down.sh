#!/bin/bash

docker kill schema_registry
docker rm schema_registry

docker kill schema_registry_auth
docker rm schema_registry_auth
docker volume rm schema_registry_auth