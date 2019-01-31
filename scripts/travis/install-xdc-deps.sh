#!/bin/bash

set -e

# Install Kafka
echo Installing Kafka
wget http://www.us.apache.org/dist/kafka/1.1.1/kafka_2.12-1.1.1.tgz -O kafka.tgz
mkdir -p kafka && tar xzf kafka.tgz -C kafka --strip-components 1
nohup bash -c "cd kafka && bin/zookeeper-server-start.sh config/zookeeper.properties &"
nohup bash -c "cd kafka && bin/kafka-server-start.sh config/server.properties &"