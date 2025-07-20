#!/usr/bin/env python3
"""
Verify that an NGSI-LD entity was properly stored in Orion-LD by querying it.
"""

import json
import sys
import urllib.request
import urllib.error

# Configuration
ORION_LD_URL = "http://localhost:1026"
NGSI_LD_ENTITIES_ENDPOINT = "/ngsi-ld/v1/entities"
ENTITY_ID = "urn:ngsi-ld:Sensor:bathroom_brightness_0000"


def check_orion_availability():
    """Check if Orion-LD is available using urllib."""
    try:
        with urllib.request.urlopen(f"{ORION_LD_URL}/version") as response:
            if response.status == 200:
                data = json.loads(response.read())
                print(f"[SUCCESS] Orion-LD is available. Version: {data.get('orionld version', 'unknown')}")
                return True
            else:
                print(f"[ERROR] Failed to connect to Orion-LD: HTTP {response.status}")
                return False
    except Exception as e:
        print(f"[ERROR] Error checking Orion-LD availability: {e}")
        return False


def get_entity_by_id(entity_id):
    """Fetch an NGSI-LD entity by its ID."""
    url = f"{ORION_LD_URL}{NGSI_LD_ENTITIES_ENDPOINT}/{entity_id}"
    headers = {
        "Accept": "application/ld+json"
    }
    
    print(f"Fetching entity: {entity_id}")
    print(f"URL: {url}")
    
    try:
        req = urllib.request.Request(url=url, headers=headers)
        with urllib.request.urlopen(req) as response:
            status_code = response.status
            print(f"Response status code: {status_code}")
            
            if status_code == 200:
                response_data = json.loads(response.read().decode('utf-8'))
                print("[SUCCESS] Entity retrieved successfully")
                return response_data
            else:
                print(f"[ERROR] Unexpected status code: {status_code}")
                return None
                
    except urllib.error.HTTPError as e:
        print(f"[ERROR] HTTP error: {e.code} - {e.reason}")
        if e.code == 404:
            print("Entity not found. It may not have been created properly.")
        try:
            error_body = e.read().decode('utf-8')
            print(f"Error details: {error_body}")
        except:
            pass
        return None
    except Exception as e:
        print(f"[ERROR] Request error: {e}")
        return None


def pretty_print_entity(entity):
    """Pretty print an NGSI-LD entity."""
    if not entity:
        print("No entity data to display.")
        return
    
    print("\n=== ENTITY DETAILS ===")
    print(f"ID: {entity.get('id')}")
    print(f"Type: {entity.get('type')}")
    
    # Extract room information
    if "room" in entity and "value" in entity["room"]:
        print(f"Room: {entity['room']['value']}")
    
    # Extract sensor properties
    for key, value in entity.items():
        if key not in ["id", "type", "room", "@context", "timestamp"] and isinstance(value, dict) and "value" in value:
            print(f"{key.capitalize()}: {value['value']}")
    
    # Extract timestamp
    if "timestamp" in entity and "value" in entity["timestamp"]:
        print(f"Timestamp: {entity['timestamp']['value']}")
    
    print("=====================\n")
    
    # Print full JSON for reference
    print("Full JSON response:")
    print(json.dumps(entity, indent=2))


def main():
    """Main function."""
    # Check if Orion-LD is available
    if not check_orion_availability():
        print("Orion-LD is not available. Please make sure it is running.")
        sys.exit(1)
    
    # Fetch the entity
    entity = get_entity_by_id(ENTITY_ID)
    
    # Display entity details
    pretty_print_entity(entity)


if __name__ == "__main__":
    main()
