#!/bin/bash
set -e

# Initialize variables from environment
EXPORT_TO_CSV=${EXPORT_TO_CSV:-false}
TEST_CONNECTION=${TEST_CONNECTION:-false}
SENSOR_TYPE=${SENSOR_TYPE:-}
ROOM_FILTER=${ROOM_FILTER:-}
ROW_LIMIT=${ROW_LIMIT:-0}
OUTPUT_PATH=${OUTPUT_PATH:-"/tmp/mahout/input/sensor_data.csv"}

# Log startup information
echo "Starting HBase to Mahout exporter"
echo "EXPORT_TO_CSV: $EXPORT_TO_CSV"
echo "TEST_CONNECTION: $TEST_CONNECTION"
echo "ROW_LIMIT: $ROW_LIMIT"
echo "OUTPUT_PATH: $OUTPUT_PATH"

if [ "$TEST_CONNECTION" = "true" ]; then
    echo "Testing HBase connection..."
    java -cp /app/app.jar com.example.HBaseToMahoutExporter --test
    exit_code=$?
    if [ $exit_code -eq 0 ]; then
        echo "Connection test PASSED"
    else
        echo "Connection test FAILED"
    fi
    exit $exit_code
elif [ "$EXPORT_TO_CSV" = "true" ]; then
    echo "Exporting data to CSV..."
    
    # Use HBaseToMahoutExporter
    cmd="java -cp /app/app.jar com.example.HBaseToMahoutExporter --export $OUTPUT_PATH"
    
    if [ -n "$SENSOR_TYPE" ]; then
        cmd="$cmd --sensor-type $SENSOR_TYPE"
    fi
    
    if [ -n "$ROOM_FILTER" ]; then
        cmd="$cmd --room $ROOM_FILTER"
    fi
    
    if [ -n "$ROW_LIMIT" ]; then
        cmd="$cmd --limit $ROW_LIMIT"
    fi
    
    echo "Executing: $cmd"
    eval $cmd
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "CSV export completed successfully"
        if [ -f "$OUTPUT_PATH" ]; then
            echo "Output file created: $OUTPUT_PATH"
            echo "File size: $(stat -c%s "$OUTPUT_PATH") bytes"
            echo "First few lines:"
            head -5 "$OUTPUT_PATH" || true
        fi
    else
        echo "CSV export failed with exit code: $exit_code"
    fi
    
    exit $exit_code
else
    echo "No operation specified. Set TEST_CONNECTION=true or EXPORT_TO_CSV=true"
    exit 1
fi
