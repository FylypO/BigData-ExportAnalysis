sudo chown -R vagrant:vagrant /usr/local/kafka/logs
sudo chmod -R u+rwX /usr/local/kafka/logs

sudo chown -R vagrant:vagrant /usr/local/kafka
sudo chmod -R u+rwX /usr/local/kafka

/usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
