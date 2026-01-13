#!/bin/bash
# Complete restart of all Big Data components
# Use this when you need a fresh start or things are broken

set -e

echo "=== Complete Big Data Stack Restart ==="
echo "This will stop all services and start them fresh"
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# 1. Stop everything
echo "=== Step 1: Stopping all services ==="

echo "Stopping Flink..."
/usr/local/flink/bin/stop-cluster.sh 2>/dev/null || true

echo "Stopping NiFi..."
if [ -f /usr/local/nifi/bin/nifi.sh ]; then
    /usr/local/nifi/bin/nifi.sh stop 2>/dev/null || true
fi

echo "Stopping Spark..."
/usr/local/spark/sbin/stop-history-server.sh 2>/dev/null || true

echo "Stopping HBase..."
/usr/local/hbase/bin/stop-hbase.sh 2>/dev/null || true
sleep 3
jps | grep -E 'HMaster|HRegionServer|HQuorumPeer' | awk '{print $1}' | xargs -r kill -9 2>/dev/null || true

echo "Stopping Hive..."
# Add your hive stop commands if available

echo "Stopping Hadoop..."
/usr/local/hadoop/sbin/stop-yarn.sh 2>/dev/null || true
/usr/local/hadoop/sbin/stop-dfs.sh 2>/dev/null || true
sleep 3

# Force kill any remaining Java processes (except this script)
echo "Cleaning up remaining processes..."
jps | grep -v "Jps" | awk '{print $1}' | xargs -r kill -9 2>/dev/null || true

# 2. Fix permissions
echo ""
echo "=== Step 2: Fixing permissions ==="
sudo chown -R vagrant:vagrant /var/hadoop 2>/dev/null || true
sudo chmod -R 755 /var/hadoop 2>/dev/null || true
sudo chown -R vagrant:vagrant /usr/local/hadoop/logs 2>/dev/null || true
sudo chown -R vagrant:vagrant /usr/local/hbase/logs 2>/dev/null || true

# 3. Clean temporary files
echo ""
echo "=== Step 3: Cleaning temporary files ==="
rm -rf /tmp/hadoop-vagrant/* 2>/dev/null || true
rm -rf /tmp/hbase-vagrant/* 2>/dev/null || true
rm -rf /tmp/zookeeper/* 2>/dev/null || true
rm -rf /tmp/hive 2>/dev/null || true

# 4. Clean problematic HBase data (optional)
read -p "Clean HBase data in HDFS? This will delete all HBase tables! (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Will clean HBase data after HDFS starts..."
    CLEAN_HBASE=true
else
    CLEAN_HBASE=false
fi

# 5. Start Hadoop
echo ""
echo "=== Step 4: Starting Hadoop ==="
/usr/local/hadoop/sbin/start-dfs.sh
sleep 5

# Wait for NameNode
WAIT=0
while [ $WAIT -lt 30 ]; do
    if jps | grep -q "NameNode"; then
        echo "NameNode is running"
        break
    fi
    sleep 2
    WAIT=$((WAIT + 2))
done

# Wait for DataNode
WAIT=0
while [ $WAIT -lt 30 ]; do
    if jps | grep -q "DataNode"; then
        echo "DataNode is running"
        break
    fi
    sleep 2
    WAIT=$((WAIT + 2))
done

echo "Waiting for HDFS to exit safe mode..."
/usr/local/hadoop/bin/hdfs dfsadmin -safemode wait

# Clean HBase if requested
if [ "$CLEAN_HBASE" = true ]; then
    echo "Cleaning HBase data in HDFS..."
    /usr/local/hadoop/bin/hdfs dfs -rm -r -f /hbase 2>/dev/null || true
    /usr/local/hadoop/bin/hdfs dfs -rm -r -f /user/hbase 2>/dev/null || true
fi

/usr/local/hadoop/sbin/start-yarn.sh
sleep 3

# 6. Start Hive
echo ""
echo "=== Step 5: Starting Hive ==="
/vagrant/scripts/start-hive.sh 2>/dev/null || true
sleep 3

# 7. Start HBase
echo ""
echo "=== Step 6: Starting HBase ==="
/vagrant/scripts/start-hbase.sh

# 8. Start Spark
echo ""
echo "=== Step 7: Starting Spark ==="
/vagrant/scripts/start-spark.sh
sleep 2

# 9. Start Flink
echo ""
echo "=== Step 8: Starting Flink ==="
/vagrant/scripts/start-flink.sh
sleep 2

# 10. Start NiFi (if running as root)
echo ""
echo "=== Step 9: Starting NiFi ==="
if [ "$EUID" -eq 0 ]; then
    /vagrant/scripts/start-nifi.sh 2>/dev/null || true
else
    echo "Skipping NiFi (requires sudo)"
fi

# 11. Final status
echo ""
echo "=== Restart Complete ==="
echo "Running processes:"
jps

echo ""
echo "Verifying critical services..."
sleep 5

# Verify HDFS
if /usr/local/hadoop/bin/hdfs dfsadmin -report &>/dev/null; then
    echo "✓ HDFS is healthy"
else
    echo "✗ HDFS has issues"
fi

# Verify HBase
if echo "status 'simple'" | /usr/local/hbase/bin/hbase shell 2>&1 | grep -q "active master"; then
    echo "✓ HBase is responsive"
else
    echo "✗ HBase is not responding (may need a few more minutes)"
fi

echo ""
echo "If issues persist, check logs:"
echo "  HDFS DataNode: tail -50 /usr/local/hadoop/logs/hadoop-vagrant-datanode-node1.log"
echo "  HBase Master:  tail -50 /usr/local/hbase/logs/hbase-vagrant-master-node1.log"
