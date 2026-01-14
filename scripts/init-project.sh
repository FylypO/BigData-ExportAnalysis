sudo chmod +x init-kafka.sh
sudo chmod +x reset-hbase-kafka.sh
sudo chmod +x reset-hdfs.sh
sudo chmod +x reset-WDI.sh

./init-kafka.sh
./reset-hbase-kafka.sh
./reset-hdfs.sh

openssl s_client -connect api.nbp.pl:443 -showcerts < /dev/null | openssl x509 -outform DER > nbp.der

mkdir -p /home/vagrant/nifi/

keytool -import -alias nbp -keystore /home/vagrant/nifi/nifi-truststore_api.jks -file nbp.der -storepass nifiadmin -noprompt

echo"NiFI Password: nifiadmin"

