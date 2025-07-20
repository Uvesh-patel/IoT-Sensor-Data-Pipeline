#!/usr/bin/env python3
"""
Verify NGSI-LD entities stored in Orion-LD Context Broker.

This script queries the Orion-LD Context Broker to retrieve
and display NGSI-LD entities to verify successful upload.
"""

import requests
import random
import time
import json
import sys
import os

# Configuration
ORION_LD_URL = "http://localhost:1026"
NGSI_LD_ENTITIES_ENDPOINT = "/ngsi-ld/v1/entities"
NGSI_LD_TYPES_ENDPOINT = "/ngsi-ld/v1/types"
V2_ENTITIES_ENDPOINT = "/v2/entities"
VERSION_ENDPOINT = "/version"
LIMIT = 10
ACCEPT_HEADER = "application/ld+json"
DEFAULT_ENTITY_TYPE = "BrightnessSensor"  # Short form, without full URI  # Default entity type for filtering


def check_orion_availability():
    """Check if Orion-LD Context Broker is available."""
    try:
        response = requests.get(f"{ORION_LD_URL}{VERSION_ENDPOINT}")
        if response.status_code == 200:
            print(f"[OK] Orion-LD is available. Version: {response.json().get('version', 'unknown')}")
            return True
        else:
            print(f"[ERROR] Orion-LD returned HTTP {response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"[ERROR] Failed to connect to Orion-LD: {e}")
        return False


def fetch_entities(entity_type=None, limit=LIMIT, count_only=False):
    """Fetch entities from Orion-LD Context Broker."""
    url = ORION_LD_URL + NGSI_LD_ENTITIES_ENDPOINT
    params = {"limit": limit}
    
    # Add type filter if specified
    if entity_type:
        params["type"] = entity_type
    
    # If counting only, set count parameter
    if count_only:
        params["count"] = "true"  # Must be string 'true', not boolean True
        params["limit"] = 1      # Must be at least 1 per the API requirements
    
    headers = {
        "Accept": ACCEPT_HEADER
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            if count_only:
                # Extract count from header
                count = int(response.headers.get('NGSILD-Results-Count', '0'))
                return count
            return response.json()
        else:
            print(f"[ERROR] Failed to fetch entities: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            return [] if not count_only else 0
    except requests.RequestException as e:
        print(f"[ERROR] Request error: {e}")
        return [] if not count_only else 0


def display_entity(entity, index=None):
    """Display a summary of an NGSI-LD entity."""
    try:
        entity_id = entity.get("id", "Unknown")
        entity_type = entity.get("type", "Unknown")
        room = None
        timestamp = None
        sensor_value = None
        
        # Extract room property
        if "room" in entity and "value" in entity["room"]:
            room = entity["room"]["value"]
        
        # Extract timestamp property
        if "timestamp" in entity and "value" in entity["timestamp"]:
            timestamp = entity["timestamp"]["value"]
        
        # Find sensor property based on entity type
        sensor_type = entity_type.lower().replace("sensor", "").strip()
        for key in entity.keys():
            if key.lower() == sensor_type or sensor_type in key.lower():
                if "value" in entity[key]:
                    sensor_value = entity[key]["value"]
                    sensor_type = key
                    break
        
        # Display entity information
        prefix = f"[{index}] " if index is not None else ""
        print(f"{prefix}Entity: {entity_id}")
        print(f"  Type: {entity_type}")
        print(f"  Room: {room}")
        print(f"  Sensor: {sensor_type}")
        print(f"  Value: {sensor_value}")
        print(f"  Timestamp: {timestamp}")
        print("-" * 50)
    except Exception as e:
        print(f"[ERROR] Error displaying entity: {e}")


def get_all_entity_types():
    """Get all entity types available in Orion-LD."""
    print("Using known entity types for Orion-LD...")
    
    # All seven entity types found in the JSON files
    known_types = [
        "BrightnessSensor",
        "HumiditySensor",
        "TemperatureSensor",
        "ThermostatTemperatureSensor",
        "VirtualOutdoorTemperatureSensor",
        "OutdoorTemperatureSensor",
        "SetpointHistorySensor"
    ]
    
    # Alternative forms with full URIs
    alt_forms = [
        "https://uri.etsi.org/ngsi-ld/default-context/BrightnessSensor",
        "https://uri.etsi.org/ngsi-ld/default-context/HumiditySensor",
        "https://uri.etsi.org/ngsi-ld/default-context/TemperatureSensor",
        "https://uri.etsi.org/ngsi-ld/default-context/ThermostatTemperatureSensor",
        "https://uri.etsi.org/ngsi-ld/default-context/VirtualOutdoorTemperatureSensor",
        "https://uri.etsi.org/ngsi-ld/default-context/OutdoorTemperatureSensor",
        "https://uri.etsi.org/ngsi-ld/default-context/SetpointHistorySensor"
    ]
    
    found_types = []
    
    # Check if entities exist for each type name format
    for entity_type in known_types:
        entities = fetch_entities(entity_type=entity_type, limit=1)
        if entities:
            print(f"Found entities with type: {entity_type}")
            found_types.append(entity_type)
    
    # If no entities found with short names, try alternative forms
    if not found_types:
        print("No entities found with short names, trying full URIs...")
        for entity_type in alt_forms:
            entities = fetch_entities(entity_type=entity_type, limit=1)
            if entities:
                short_type = entity_type.split("/")[-1]
                print(f"Found entities with type: {short_type} (full URI)")
                found_types.append(entity_type)
    
    # If still no types found, fall back to defaults
    if not found_types:
        print("No entity types found, using defaults")
        return known_types
    
    print(f"Found {len(found_types)} entity types in the system")
    return found_types

def count_all_entities():
    """Count all entities across all types."""
    # First, get all entity types
    types = get_all_entity_types()
    
    # Count entities for each type
    total_count = 0
    counts_by_type = {}
    
    for entity_type in types:
        count = fetch_entities(entity_type=entity_type, count_only=True)
        counts_by_type[entity_type] = count
        total_count += count
    
    return total_count, counts_by_type

def get_random_entities(count=10):
    """Get random entities from different types."""
    types = get_all_entity_types()
    if not types:
        return []
    
    all_entities = []
    
    # Collect entities from each type
    for entity_type in types:
        entities = fetch_entities(entity_type=entity_type, limit=100)
        all_entities.extend(entities)
    
    # Select random entities
    if len(all_entities) <= count:
        return all_entities
    
    return random.sample(all_entities, count)

def main():
    """Main function to verify NGSI-LD entities in Orion-LD."""
    # Check if Orion-LD is available
    if not check_orion_availability():
        print("Orion-LD is not available. Please make sure it is running.")
        sys.exit(1)
    
    # Count all entities in the system
    print("\nCounting all entities in Orion-LD...")
    total_count, counts_by_type = count_all_entities()
    
    # Display summary of entity types and counts
    print(f"\nTotal Entities: {total_count}")
    print("\nEntity Types Summary:")
    for entity_type, count in counts_by_type.items():
        short_type = entity_type.split("/")[-1] if "/" in entity_type else entity_type
        print(f"  {short_type}: {count} entities")
    print("-" * 50)
    
    # Get and display 10 random entities
    print(f"\nShowing {LIMIT} random entities:")
    random_entities = get_random_entities(LIMIT)
    
    if not random_entities:
        print("No entities found in Orion-LD.")
        sys.exit(1)
    
    # Display detailed information for random entities
    for i, entity in enumerate(random_entities):
        display_entity(entity, i + 1)


if __name__ == "__main__":
    main()
