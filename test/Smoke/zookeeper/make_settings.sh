#!/bin/bash

cat <<EOF >$OUT
dataDir=/data/zookeeper
clientPort=$CLIENT_PORT
clientPortAddress=$CLIENT_PORT_ADDRESS
maxClientCnxns=0
autopurge.snapRetainCount=3
autopurge.purgeInterval=24
initLimit=5
syncLimit=2
tickTime=2000
EOF

cat <<EOF > /data/zookeeper/myid
$MY_ID
EOF
