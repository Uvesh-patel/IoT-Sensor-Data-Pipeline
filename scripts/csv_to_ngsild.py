#!/usr/bin/env python3
"""
CSV to NGSI-LD Converter

This script reads CSV files containing sensor measurements (without headers)
and converts them to NGSI-LD compliant JSON documents for Orion-LD Context Broker ingestion.

Each CSV file is expected to have two columns:
1. ISO timestamp (e.g., 2023-01-01T00:00:00Z)
2. Numeric value (float or integer)

The filename format is expected to be: Room_SensorType.csv
"""

import csv
import json
import os
import pathlib
import datetime
from typing import Dict, List, Tuple


def extract_room_and_sensor(filename: str) -> Tuple[str, str]:
    """
    Extract room and sensor type from filename.
    Example: Kitchen_Brightness.csv -> room=kitchen, sensor=brightness
    """
    base_name = os.path.splitext(os.path.basename(filename))[0]
    parts = base_name.split('_')
    
    # Extract room (first part)
    room = parts[0].lower()
    
    # Extract sensor type (remaining parts joined with underscores)
    sensor = '_'.join(parts[1:]).lower()
    
    return room, sensor


def get_sensor_type(sensor_name: str) -> str:
    """
    Map sensor name to appropriate NGSI-LD entity type.
    Uses substring matching to handle variations like left/right prefixes.
    
    Args:
        sensor_name: The sensor name extracted from filename (lowercase)
        
    Returns:
        Appropriate NGSI-LD entity type
    """
    # Convert to lowercase for case-insensitive matching
    sensor_name = sensor_name.lower()
    
    # Check for sensor types using substring matching
    if "brightness" in sensor_name:
        return "BrightnessSensor"
    elif "humidity" in sensor_name:
        return "HumiditySensor"
    elif "thermostattemperature" in sensor_name:
        return "ThermostatTemperatureSensor"
    elif "virtual_outdoortemperature" in sensor_name:
        return "VirtualOutdoorTemperatureSensor"
    elif "outdoortemperature" in sensor_name:
        return "OutdoorTemperatureSensor"
    elif "setpointhistory" in sensor_name:
        return "SetpointHistorySensor"
    elif "temperature" in sensor_name:
        # Must be after the more specific temperature types
        return "TemperatureSensor"
    
    # Fallback (should never happen with the 37 specified files)
    return "Sensor"


def create_ngsi_ld_entity(
    room: str, 
    sensor_name: str, 
    index: int, 
    timestamp: str, 
    value: float
) -> Dict:
    """
    Create an NGSI-LD compliant entity.
    """
    # Create a unique ID
    entity_id = f"urn:ngsi-ld:Sensor:{room}_{sensor_name}_{index:04d}"
    
    # Determine entity type based on sensor name
    entity_type = get_sensor_type(sensor_name)
    
    # Create the NGSI-LD entity
    entity = {
        "id": entity_id,
        "type": entity_type,
        "room": {
            "type": "Property",
            "value": room
        },
        "timestamp": {
            "type": "Property",
            "value": timestamp
        },
        "@context": [
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        ]
    }
    
    # Add sensor-specific property
    entity[sensor_name] = {
        "type": "Property",
        "value": value
    }
    
    return entity


def unix_to_iso(unix_timestamp: str) -> str:
    """
    Convert Unix timestamp to ISO 8601 format.
    """
    try:
        # Convert to integer and then to datetime
        dt = datetime.datetime.fromtimestamp(int(unix_timestamp), tz=datetime.timezone.utc)
        # Format as ISO 8601
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        # If conversion fails, return original string
        return unix_timestamp

def process_csv_file(csv_path: str, output_dir: str) -> None:
    """
    Process a single CSV file and convert it to NGSI-LD entities.
    """
    # Extract room and sensor type from filename
    room, sensor_name = extract_room_and_sensor(csv_path)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read CSV file with tab delimiter and create NGSI-LD entities
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        
        for idx, row in enumerate(csv_reader):
            if len(row) < 2:
                print(f"Warning: Skipping invalid row in {csv_path}: {row}")
                continue
                
            # Parse timestamp and value
            unix_timestamp = row[0]
            iso_timestamp = unix_to_iso(unix_timestamp)
            value = float(row[1])  # Convert to float
            
            # Create NGSI-LD entity
            entity = create_ngsi_ld_entity(room, sensor_name, idx, iso_timestamp, value)
            
            # Create output filename
            output_filename = f"{room}_{sensor_name}_{idx:04d}.json"
            output_path = os.path.join(output_dir, output_filename)
            
            # Write entity to JSON file
            with open(output_path, 'w') as json_file:
                json.dump(entity, json_file, indent=2)
                
            print(f"Created NGSI-LD entity: {output_filename}")


def main() -> None:
    """
    Main function to process all CSV files.
    """
    # Define directory paths
    base_dir = pathlib.Path(__file__).parent.parent
    data_dir = os.path.join(base_dir, "data", "measurements")
    output_dir = os.path.join(base_dir, "output", "json")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each CSV file
    for csv_file in os.listdir(data_dir):
        if csv_file.lower().endswith('.csv'):
            csv_path = os.path.join(data_dir, csv_file)
            process_csv_file(csv_path, output_dir)
            print(f"Processed file: {csv_file}")


if __name__ == "__main__":
    main()
