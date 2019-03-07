#!/bin/bash

cat <<EOF >$OUT
global:
 scrape_interval: $SCRAPE_INTERVAL
 evaluation_interval: $EVAL_INTERVAL
scrape_configs:
 - job_name: 'kafka'
   static_configs:
    - targets:
      - $KAFKA_IP_1:$KAFKA_METRICS_PORT_1
      - $KAFKA_IP_2:$KAFKA_METRICS_PORT_2
      - $KAFKA_IP_3:$KAFKA_METRICS_PORT_3
 - job_name: 'zookeeper'
   static_configs:
    - targets:
      - $ZK_IP:$ZK_METRICS_PORT
EOF
