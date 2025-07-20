#!/usr/bin/env python3
"""
Parallel upload of NGSI-LD JSON documents to Orion-LD Context Broker.
Uses ThreadPoolExecutor for concurrent uploads with progress tracking.
"""

import json
import os
import sys
import urllib.request
import urllib.error
import time
import glob
import logging
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("upload.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
ORION_LD_URL = "http://localhost:1026"
NGSI_LD_ENTITIES_ENDPOINT = "/ngsi-ld/v1/entities"
CONTENT_TYPE = "application/ld+json"
JSON_DIR = "../output/json"
MAX_RETRIES = 2  # Reduced for performance
DEFAULT_MAX_WORKERS = 30


def check_orion_availability():
    """Check if Orion-LD is available using urllib."""
    try:
        with urllib.request.urlopen(f"{ORION_LD_URL}/version") as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                logger.info(f"Orion-LD is available. Version: {data.get('orionld version', 'unknown')}")
                return True
            else:
                logger.error(f"Failed to connect to Orion-LD: HTTP {response.status}")
                return False
    except Exception as e:
        logger.error(f"Error checking Orion-LD availability: {e}")
        return False


def load_json_file(file_path):
    """Load a JSON file and return its contents."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None


def upload_entity(file_path, retries=MAX_RETRIES):
    """Upload an NGSI-LD entity from a file to Orion-LD."""
    # Load entity data
    entity_data = load_json_file(file_path)
    if not entity_data:
        return file_path, False, "Failed to load entity data"
    
    url = f"{ORION_LD_URL}{NGSI_LD_ENTITIES_ENDPOINT}"
    headers = {
        "Content-Type": CONTENT_TYPE,
    }
    
    entity_id = os.path.basename(file_path).replace(".json", "")
    
    for attempt in range(retries):
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
                
                # Check if successful (201 Created)
                if status_code == 201:
                    return file_path, True, "Created"
                else:
                    return file_path, False, f"Unexpected status code: {status_code}"
                    
        except urllib.error.HTTPError as e:
            if e.code == 409:  # Conflict - entity already exists
                return file_path, True, "Already exists"
            elif e.code >= 500 and attempt < retries - 1:
                # Server error, retry after a short delay
                time.sleep(0.5)
                continue
            else:
                try:
                    error_body = e.read().decode('utf-8')
                    return file_path, False, f"HTTP error: {e.code} - {e.reason}, Details: {error_body}"
                except:
                    return file_path, False, f"HTTP error: {e.code} - {e.reason}"
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(0.5)
                continue
            return file_path, False, f"Request error: {e}"
    
    return file_path, False, "Max retries exceeded"


def get_json_files(start_idx=None, end_idx=None, max_files=None):
    """Get JSON files with optional range filtering."""
    all_files = sorted(glob.glob(os.path.join(JSON_DIR, "*.json")))
    
    if start_idx is not None and end_idx is not None:
        files = all_files[start_idx:end_idx]
    elif max_files:
        files = all_files[:max_files]
    else:
        files = all_files
        
    return files


def parallel_upload(file_list, max_workers):
    """Upload entities in parallel using ThreadPoolExecutor."""
    total_files = len(file_list)
    success_count = 0
    already_exists_count = 0
    error_count = 0
    
    logger.info(f"Uploading {total_files} JSON files with {max_workers} workers")
    
    results = {
        "success": [],
        "already_exists": [],
        "error": []
    }
    
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(upload_entity, file_path): file_path for file_path in file_list}
        
        # Process results as they complete with progress bar
        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Uploading"):
            file_path, success, message = future.result()
            entity_id = os.path.basename(file_path).replace(".json", "")
            
            if success:
                if message == "Created":
                    success_count += 1
                    results["success"].append(entity_id)
                    if success_count % 100 == 0:
                        logger.info(f"Successfully uploaded {success_count} entities")
                else:
                    already_exists_count += 1
                    results["already_exists"].append(entity_id)
            else:
                error_count += 1
                results["error"].append((entity_id, message))
                logger.error(f"Failed to upload {entity_id}: {message}")
    
    return {
        "total": total_files,
        "success": success_count,
        "already_exists": already_exists_count,
        "errors": error_count,
        "error_details": results["error"]
    }


def main():
    """Main function to upload entities in parallel."""
    parser = argparse.ArgumentParser(description='Upload NGSI-LD entities to Orion-LD in parallel')
    parser.add_argument('--workers', type=int, default=DEFAULT_MAX_WORKERS, 
                        help=f'Number of worker threads (default: {DEFAULT_MAX_WORKERS})')
    parser.add_argument('--start', type=int, help='Starting index for file range')
    parser.add_argument('--end', type=int, help='Ending index for file range (exclusive)')
    parser.add_argument('--max', type=int, help='Maximum number of files to upload')
    parser.add_argument('--verbose', '-v', action='store_true', help='Increase output verbosity')
    
    args = parser.parse_args()
    
    # Set log level based on verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Check if Orion-LD is available
    if not check_orion_availability():
        logger.error("Orion-LD is not available. Please make sure it is running.")
        sys.exit(1)
    
    # Get JSON files (with optional range filtering)
    json_files = get_json_files(args.start, args.end, args.max)
    if not json_files:
        logger.error(f"No JSON files found in {JSON_DIR} with the specified range")
        sys.exit(1)
    
    # Start timer
    start_time = time.time()
    
    # Upload entities in parallel
    results = parallel_upload(json_files, args.workers)
    
    # End timer
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # Print summary
    logger.info("\n=== UPLOAD SUMMARY ===")
    logger.info(f"Total files processed: {results['total']}")
    logger.info(f"Successfully created: {results['success']}")
    logger.info(f"Already existed: {results['already_exists']}")
    logger.info(f"Errors: {results['errors']}")
    if results['errors'] > 0:
        logger.info("Sample errors:")
        for i, (entity_id, error) in enumerate(results['error_details'][:5]):
            logger.info(f" - {entity_id}: {error}")
        if results['errors'] > 5:
            logger.info(f"  ...and {results['errors'] - 5} more errors (see log file)")
    
    logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
    logger.info(f"Average rate: {results['total'] / elapsed_time:.2f} entities/second")
    logger.info("=====================\n")


if __name__ == "__main__":
    main()
