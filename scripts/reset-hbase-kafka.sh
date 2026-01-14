#!/bin/bash

echo "---> Cleaning HBase tables."

hbase shell <<EOF
  disable 'nbp_monthly'
  drop 'nbp_monthly'
  create 'nbp_monthly', 'rates'

  disable 'nbp_weekly'
  drop 'nbp_weekly'
  create 'nbp_weekly', 'rates'
  
  list
  exit
EOF

echo "---> Deleting Kafka topics."

/usr/local/kafka/bin/kafka-topics.sh --delete --topic comtrade.records --bootstrap-server localhost:9092 || true
/usr/local/kafka/bin/kafka-topics.sh --delete --topic nbp.records --bootstrap-server localhost:9092 || true
/usr/local/kafka/bin/kafka-topics.sh --delete --topic nbp_hbase.records --bootstrap-server localhost:9092 || true

sleep 30

echo "---> Creating Kafka topics."

/usr/local/kafka/bin/kafka-topics.sh \
  --create \
  --topic comtrade.records \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh \
  --create \
  --topic nbp.records \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

/usr/local/kafka/bin/kafka-topics.sh \
  --create \
  --topic nbp_hbase.records \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

echo "---> Success"
