#!/usr/bin/env python3
"""
Test uploading a single NGSI-LD document to Orion-LD Context Broker.
"""

import json
import os
import sys
import subprocess
import time
import urllib.request
import urllib.error

# Configuration
ORION_LD_URL = "http://localhost:1026"
NGSI_LD_ENTITIES_ENDPOINT = "/ngsi-ld/v1/entities"
CONTENT_TYPE = "application/ld+json"
TEST_FILE = "../output/json/bathroom_brightness_0000.json"


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


def load_json_file(file_path):
    """Load a JSON file and return its contents."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def upload_entity(entity_data):
    """Upload an NGSI-LD entity to Orion-LD Context Broker using urllib."""
    url = f"{ORION_LD_URL}{NGSI_LD_ENTITIES_ENDPOINT}"
    headers = {
        "Content-Type": CONTENT_TYPE,
    }
    
    print(f"Posting to URL: {url}")
    print(f"Headers: {headers}")
    print(f"Data payload: {json.dumps(entity_data, indent=2)}")
    
    try:
        # Convert JSON to bytes
        data = json.dumps(entity_data).encode('utf-8')
        
        # Create request
        req = urllib.request.Request(
            url=url,
            data=data,
            headers=headers,
            method='POST'
        )
        
        # Send request
        with urllib.request.urlopen(req) as response:
            status_code = response.status
            print(f"Response status code: {status_code}")
            
            # Get response body if any
            try:
                response_body = response.read().decode('utf-8')
                if response_body:
                    print(f"Response body: {response_body}")
            except:
                response_body = ""
            
            # Check if successful (201 Created)
            if status_code == 201:
                print("[SUCCESS] Entity created successfully")
                return True
            else:
                print(f"[ERROR] Unexpected status code: {status_code}")
                return False
                
    except urllib.error.HTTPError as e:
        print(f"[ERROR] HTTP error: {e.code} - {e.reason}")
        try:
            error_body = e.read().decode('utf-8')
            print(f"Error details: {error_body}")
        except:
            pass
        return False
    except Exception as e:
        print(f"[ERROR] Request error: {e}")
        return False


def main():
    """Main function to test uploading a single entity."""
    # Check if Orion-LD is available
    if not check_orion_availability():
        print("Orion-LD is not available. Please make sure it is running.")
        sys.exit(1)
    
    # Load the test JSON file
    print(f"Loading file: {TEST_FILE}")
    entity_data = load_json_file(TEST_FILE)
    
    if not entity_data:
        print("Failed to load entity data.")
        sys.exit(1)
    
    # Print entity data
    print(f"Entity data: {json.dumps(entity_data, indent=2)}")
    
    # Upload the entity
    success = upload_entity(entity_data)
    
    if success:
        print("\nTest completed successfully.")
    else:
        print("\nTest completed with errors.")


if __name__ == "__main__":
    main()
