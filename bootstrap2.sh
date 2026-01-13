#!/bin/bash
# Improved bootstrap script with proper permission handling and component ordering
# This script safely starts all Big Data components

set -e  # Exit on error

echo "=== Starting Big Data Stack Bootstrap ==="

# Function to check if a process is running
check_process() {
    local process_name=$1
    jps | grep -q "$process_name"
}

# Function to wait for a service
wait_for_service() {
    local service_name=$1
    local max_wait=$2
    local wait_time=0
    
    echo "Waiting for $service_name to start..."
    while [ $wait_time -lt $max_wait ]; do
        if check_process "$service_name"; then
            echo "$service_name is running"
            return 0
        fi
        sleep 2
        wait_time=$((wait_time + 2))
    done
    echo "WARNING: $service_name did not start within $max_wait seconds"
    return 1
}

# 1. Fix permissions first (critical for DataNode and HBase)
echo "=== Step 1: Fixing permissions ==="
if [ -d /var/hadoop ]; then
    sudo chown -R vagrant:vagrant /var/hadoop 2>/dev/null || true
    sudo chmod -R 755 /var/hadoop 2>/dev/null || true
fi

if [ -d /usr/local/hbase/logs ]; then
    sudo chown -R vagrant:vagrant /usr/local/hbase/logs 2>/dev/null || true
fi

if [ -d /usr/local/hadoop/logs ]; then
    sudo chown -R vagrant:vagrant /usr/local/hadoop/logs 2>/dev/null || true
fi

# 2. Clean temporary files if this is a fresh start
echo "=== Step 2: Cleaning stale temporary files ==="
if ! check_process "NameNode"; then
    echo "Fresh start detected, cleaning temporary files..."
    rm -rf /tmp/hadoop-vagrant/* 2>/dev/null || true
    rm -rf /tmp/hbase-vagrant/* 2>/dev/null || true
    # Don't remove /tmp/zookeeper/* as HBase manages it
fi

# 3. Start Hadoop (HDFS + YARN)
echo "=== Step 3: Starting Hadoop ==="
if ! check_process "NameNode"; then
    /vagrant/scripts/start-hadoop.sh
    wait_for_service "NameNode" 20
    wait_for_service "DataNode" 20
    
    # Wait for HDFS to exit safe mode
    echo "Waiting for HDFS to exit safe mode..."
    sleep 5
    /usr/local/hadoop/bin/hdfs dfsadmin -safemode wait
else
    echo "Hadoop already running"
fi

# 4. Verify HDFS is healthy before proceeding
echo "=== Step 4: Verifying HDFS health ==="
if ! /usr/local/hadoop/bin/hdfs dfsadmin -report &>/dev/null; then
    echo "ERROR: HDFS is not healthy. Check DataNode logs."
    exit 1
fi

# 5. Start Hive
echo "=== Step 5: Starting Hive ==="
/vagrant/scripts/start-hive.sh
sleep 3

# 6. Start HBase (with proper cleanup if needed)
echo "=== Step 6: Starting HBase ==="
if check_process "HMaster"; then
    echo "HBase already running"
else
    # Check if HBase had issues before
    if [ -f /usr/local/hbase/logs/hbase-vagrant-master-node1.log ]; then
        if grep -q "Failed update hbase:meta" /usr/local/hbase/logs/hbase-vagrant-master-node1.log 2>/dev/null; then
            echo "Detected previous HBase failure, cleaning up..."
            /usr/local/hadoop/bin/hdfs dfs -rm -r -f /hbase 2>/dev/null || true
            rm -rf /tmp/zookeeper/* 2>/dev/null || true
        fi
    fi
    
    /vagrant/scripts/start-hbase.sh
    
    # Wait for all HBase components
    wait_for_service "HQuorumPeer" 15
    wait_for_service "HMaster" 30
    wait_for_service "HRegionServer" 20
    
    # Verify HBase is actually working
    echo "Verifying HBase..."
    sleep 5
    if ! echo "status 'simple'" | /usr/local/hbase/bin/hbase shell 2>&1 | grep -q "active master"; then
        echo "WARNING: HBase might not be fully initialized. Check logs if issues persist."
    fi
fi

# 7. Start Spark
echo "=== Step 7: Starting Spark History Server ==="
/vagrant/scripts/start-spark.sh
sleep 2

# 8. Start NiFi (requires sudo for port binding)
echo "=== Step 8: Starting NiFi ==="
if [ "$EUID" -eq 0 ]; then
    /vagrant/scripts/start-nifi.sh
else
    echo "NiFi requires sudo, skipping... (run: sudo /vagrant/scripts/start-nifi.sh)"
fi

# 9. Start Flink
echo "=== Step 9: Starting Flink ==="
/vagrant/scripts/start-flink.sh
sleep 2

# 10. Final status check
echo ""
echo "=== Bootstrap Complete ==="
echo "Running Java processes:"
jps

echo ""
echo "=== Service Status ==="
echo "HDFS NameNode:    $(check_process 'NameNode' && echo 'Running' || echo 'NOT Running')"
echo "HDFS DataNode:    $(check_process 'DataNode' && echo 'Running' || echo 'NOT Running')"
echo "YARN ResourceMgr: $(check_process 'ResourceManager' && echo 'Running' || echo 'NOT Running')"
echo "HBase ZooKeeper:  $(check_process 'HQuorumPeer' && echo 'Running' || echo 'NOT Running')"
echo "HBase Master:     $(check_process 'HMaster' && echo 'Running' || echo 'NOT Running')"
echo "HBase RegionSvr:  $(check_process 'HRegionServer' && echo 'Running' || echo 'NOT Running')"
echo "Spark History:    $(check_process 'HistoryServer' && echo 'Running' || echo 'NOT Running')"
echo "Flink:            $(check_process 'StandaloneSessionClusterEntrypoint' && echo 'Running' || echo 'NOT Running')"

echo ""
echo "If HBase is not running properly, check:"
echo "  tail -50 /usr/local/hbase/logs/hbase-vagrant-master-node1.log"
echo "  tail -50 /usr/local/hadoop/logs/hadoop-vagrant-datanode-node1.log"
