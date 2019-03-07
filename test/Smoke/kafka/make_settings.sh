#!/bin/bash

cat <<EOF >$OUT
broker.id=$BROKER_ID

auto.create.topics.enable=false
delete.topic.enable=true
min.insync.replicas=2

listeners=PLAINTEXT://:$BROKER_PORT
advertised.listeners=PLAINTEXT://$BROKER_IP:$BROKER_PORT

num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

log.dirs=/data/kafka-logs

num.partitions=1

num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2

log.retention.hours=-1
log.retention.bytes=-1
log.segment.bytes=1073741824

log.retention.check.interval.ms=300000

zookeeper.connect=$ZK_IP_1:$ZK_PORT_1
zookeeper.connection.timeout.ms=6000

group.initial.rebalance.delay.ms=3
EOF
