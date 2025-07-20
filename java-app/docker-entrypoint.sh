#!/bin/bash
set -e

echo "Starting Java application..."

# Set variables for readiness checks
HBASE_HOST=${HBASE_ZOOKEEPER_QUORUM:-hbase}
ZK_PORT=${HBASE_ZOOKEEPER_PORT:-2181}
MASTER_PORT=16000
REGION_PORT=16020
WEB_UI_PORT=16010

# Increased timeout for comprehensive checks
timeout=120  # Increased from 60s to 120s

# Function for checking port availability
check_port() {
    local host=$1
    local port=$2
    local service=$3
    local max_tries=$4
    local counter=0
    
    echo "Checking if $service is available at $host:$port..."
    while ! nc -z $host $port >/dev/null 2>&1; do
        counter=$((counter+1))
        if [ $counter -ge $max_tries ]; then
            echo "⚠️ Timed out waiting for $service to be ready at $host:$port!"
            return 1
        fi
        echo "Waiting for $service... ($counter/$max_tries)"
        sleep 2
    done
    echo "✅ $service is available at $host:$port!"
    return 0
}

# Wait for ZooKeeper to be available
echo "Step 1/4: Checking ZooKeeper availability"
check_port $HBASE_HOST $ZK_PORT "ZooKeeper" $timeout || exit 1

# Wait for HBase Master to be available
echo "Step 2/4: Checking HBase Master availability"
check_port $HBASE_HOST $MASTER_PORT "HBase Master" $timeout || exit 1

# Wait for HBase RegionServer to be available
echo "Step 3/4: Checking HBase RegionServer availability"
check_port $HBASE_HOST $REGION_PORT "HBase RegionServer" $timeout || exit 1

# Check Web UI which indicates fully functional HBase
echo "Step 4/4: Checking HBase Web UI availability"
check_port $HBASE_HOST $WEB_UI_PORT "HBase Web UI" $timeout || exit 1

# Advanced check: Test ZooKeeper connectivity
echo "Advanced check: Testing ZooKeeper connectivity with 'stat' command"
if command -v nc &> /dev/null; then
    if echo "stat" | nc $HBASE_HOST $ZK_PORT 2>&1 | grep -q "Zookeeper version"; then
        echo "✅ ZooKeeper is responding to 'stat' command - fully operational!"
    else
        echo "⚠️ ZooKeeper is reachable but might not be fully initialized (stat command failed)"
        echo "Continuing anyway, but table creation might fail..."
    fi
else
    echo "⚠️ Cannot perform advanced ZooKeeper check (nc command not available for text mode)"
fi

# Create the HBase table if CREATE_SENSOR_TABLE is set to true
if [ "$CREATE_SENSOR_TABLE" = "true" ]; then
    echo "Creating SensorData table in HBase..."
    # Run the CreateSensorTable class
    cd /app
    # Print JAR contents to debug
    echo "JAR Contents:"
    jar -tf app.jar | grep -i CreateSensorTable
    
    # Run with explicit classpath referencing the correct main class path
    java -cp app.jar com.example.CreateSensorTable
    
    # Check if table creation was successful
    if [ $? -ne 0 ]; then
        echo "Failed to create SensorData table in HBase!"
        exit 1
    fi
    echo "SensorData table created successfully!"
fi

# Test connection to Orion-LD if TEST_ORION_FETCHER is set to true
if [ "$TEST_ORION_FETCHER" = "true" ]; then
    echo "===== TESTING CONNECTION TO ORION-LD ====="
    # Print JAR contents to debug
    echo "JAR Contents for OrionFetcher:"
    jar -tf app.jar | grep -i OrionFetcher
    
    # Set Orion host environment variable for the test
    export ORION_HOST=${ORION_HOST:-fiware-orion}
    echo "Using ORION_HOST: $ORION_HOST"
    
    # Check if Orion-LD is reachable
    echo "Checking if Orion-LD is reachable at $ORION_HOST:1026..."
    if nc -z $ORION_HOST 1026; then
        echo "✅ Orion-LD is reachable at $ORION_HOST:1026"
    else
        echo "⚠️ Cannot reach Orion-LD at $ORION_HOST:1026"
        echo "Will try to run test anyway..."
    fi
    
    # Show the classpath to debug
    echo "Running OrionFetcher with classpath: app.jar"
    
    # Run OrionFetcher test with verbose output
    echo "Starting OrionFetcher test..."
    java -cp app.jar com.example.OrionFetcher
    
    # Check if Orion connection was successful
    if [ $? -ne 0 ]; then
        echo "⚠️ OrionFetcher test failed (exit code $?)" 
        echo "This might be due to Orion-LD not being ready or network issues"
        echo "Continuing anyway..."
        # We don't exit here to avoid stopping the container
    else
        echo "✅ OrionFetcher test completed successfully!"
    fi
    echo "===== ORION-LD TEST COMPLETED ====="
fi

echo "Running main application..."
exec java -jar app.jar
