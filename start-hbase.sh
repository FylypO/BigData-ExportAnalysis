#!/bin/bash
# Safe HBase startup script with automatic recovery
# Replace your existing /vagrant/scripts/start-hbase.sh with this

set -e

echo "Starting HBase with safety checks..."

# Check if HDFS is running
if ! jps | grep -q "DataNode"; then
    echo "ERROR: HDFS DataNode is not running. Start Hadoop first."
    exit 1
fi

# Check if HDFS is healthy
if ! /usr/local/hadoop/bin/hdfs dfsadmin -report &>/dev/null; then
    echo "ERROR: HDFS is not healthy. Fix HDFS issues first."
    exit 1
fi

# Function to stop HBase completely
stop_hbase_completely() {
    echo "Stopping all HBase processes..."
    /usr/local/hbase/bin/stop-hbase.sh 2>/dev/null || true
    sleep 2
    
    # Force kill any remaining processes
    jps | grep -E 'HMaster|HRegionServer|HQuorumPeer' | awk '{print $1}' | xargs -r kill -9 2>/dev/null || true
    sleep 2
}

# Check if HBase is already running
if jps | grep -q "HMaster"; then
    echo "HBase Master already running"
    
    # Verify it's actually working
    if echo "status 'simple'" | /usr/local/hbase/bin/hbase shell 2>&1 | grep -q "active master"; then
        echo "HBase is healthy and running"
        exit 0
    else
        echo "HBase Master running but not responding properly, restarting..."
        stop_hbase_completely
    fi
fi

# Check for previous failures in logs
if [ -f /usr/local/hbase/logs/hbase-vagrant-master-node1.log ]; then
    if grep -q "Failed update hbase:meta\|EOFException\|Permission denied" /usr/local/hbase/logs/hbase-vagrant-master-node1.log 2>/dev/null; then
        echo "Detected previous HBase/HDFS errors, performing cleanup..."
        
        # Stop everything
        stop_hbase_completely
        
        # Clean up HDFS
        echo "Cleaning HBase data in HDFS..."
        /usr/local/hadoop/bin/hdfs dfs -rm -r -f /hbase 2>/dev/null || true
        /usr/local/hadoop/bin/hdfs dfs -rm -r -f /user/hbase 2>/dev/null || true
        
        # Clean up local ZooKeeper data
        echo "Cleaning ZooKeeper data..."
        rm -rf /tmp/zookeeper/* 2>/dev/null || true
        
        # Clean up local HBase temp files
        rm -rf /tmp/hbase-vagrant/* 2>/dev/null || true
        
        # Backup old logs
        if [ -f /usr/local/hbase/logs/hbase-vagrant-master-node1.log ]; then
            mv /usr/local/hbase/logs/hbase-vagrant-master-node1.log \
               /usr/local/hbase/logs/hbase-vagrant-master-node1.log.$(date +%Y%m%d_%H%M%S) 2>/dev/null || true
        fi
        
        echo "Cleanup complete, will start fresh..."
        sleep 3
    fi
fi

# Ensure proper permissions
echo "Ensuring proper permissions..."
sudo chown -R vagrant:vagrant /var/hadoop 2>/dev/null || true
sudo chmod -R 755 /var/hadoop 2>/dev/null || true
sudo chown -R vagrant:vagrant /usr/local/hbase/logs 2>/dev/null || true

# Start HBase
echo "Starting HBase services..."
/usr/local/hbase/bin/start-hbase.sh

# Wait for ZooKeeper
echo "Waiting for ZooKeeper..."
sleep 5
WAIT=0
while [ $WAIT -lt 30 ]; do
    if jps | grep -q "HQuorumPeer"; then
        echo "ZooKeeper is running"
        break
    fi
    sleep 2
    WAIT=$((WAIT + 2))
done

# Wait for HMaster
echo "Waiting for HMaster..."
sleep 5
WAIT=0
while [ $WAIT -lt 60 ]; do
    if jps | grep -q "HMaster"; then
        echo "HMaster is running"
        break
    fi
    
    # Check if HMaster crashed
    if [ -f /usr/local/hbase/logs/hbase-vagrant-master-node1.log ]; then
        if grep -q "Master exiting\|Failed construction" /usr/local/hbase/logs/hbase-vagrant-master-node1.log 2>/dev/null; then
            echo "ERROR: HMaster failed to start. Check logs:"
            echo "  tail -50 /usr/local/hbase/logs/hbase-vagrant-master-node1.log"
            exit 1
        fi
    fi
    
    sleep 2
    WAIT=$((WAIT + 2))
done

# Wait for RegionServer
echo "Waiting for RegionServer..."
sleep 5
WAIT=0
while [ $WAIT -lt 30 ]; do
    if jps | grep -q "HRegionServer"; then
        echo "RegionServer is running"
        break
    fi
    sleep 2
    WAIT=$((WAIT + 2))
done

# Final verification
echo "Verifying HBase is responsive..."
sleep 5

if echo "status 'simple'" | /usr/local/hbase/bin/hbase shell 2>&1 | grep -q "active master"; then
    echo "✓ HBase started successfully and is responsive!"
    jps | grep -E 'HQuorumPeer|HMaster|HRegionServer'
    exit 0
else
    echo "WARNING: HBase processes are running but shell is not responding properly"
    echo "This might resolve itself in a few minutes, or check logs:"
    echo "  tail -50 /usr/local/hbase/logs/hbase-vagrant-master-node1.log"
    jps | grep -E 'HQuorumPeer|HMaster|HRegionServer'
    exit 1
fi
