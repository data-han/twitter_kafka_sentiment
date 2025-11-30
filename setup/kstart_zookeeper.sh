#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

kafka_version="kafka_2.13-3.1.0"

cd $parent_path/../$kafka_version
sh bin/zookeeper-server-start.sh config/zookeeper.properties
