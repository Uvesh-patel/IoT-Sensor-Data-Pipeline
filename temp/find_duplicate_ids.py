#!/usr/bin/env python3
"""
Scan NGSI-LD JSON files to detect duplicate entity IDs.

This script recursively scans the specified directory for JSON files,
extracts the entity ID from each file, and reports on duplicate IDs.
"""

import os
import json
import sys
from collections import defaultdict


def find_project_root():
    """
    Find the project root directory that contains 'output/json'
    by walking up from the current directory until the folder is found.
    
    Returns:
        str: Path to output/json directory or None if not found
    """
    # Start from the current working directory
    current_dir = os.path.abspath(os.getcwd())
    max_levels = 5  # Don't go up more than 5 levels
    
    for _ in range(max_levels):
        # Check if this directory contains the output/json folder
        json_dir = os.path.join(current_dir, 'output', 'json')
        if os.path.isdir(json_dir):
            return json_dir
            
        # Check if this directory contains an 'output' folder
        output_dir = os.path.join(current_dir, 'output')
        if os.path.isdir(output_dir):
            # Check if output has a json subfolder
            json_dir = os.path.join(output_dir, 'json')
            if os.path.isdir(json_dir):
                return json_dir
        
        # Go one directory up
        parent = os.path.dirname(current_dir)
        if parent == current_dir:  # Reached root
            break
        current_dir = parent
    
    return None


def scan_json_files(directory):
    """
    Recursively scan JSON files in the given directory and check for duplicate IDs.
    
    Args:
        directory: Path to the directory containing JSON files
        
    Returns:
        tuple: (total_files, unique_ids, duplicate_map)
            - total_files: Total number of JSON files scanned
            - unique_ids: Set of unique entity IDs found
            - duplicate_map: Dictionary mapping duplicate IDs to lists of filenames
    """
    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' not found.")
        sys.exit(1)
    
    total_files = 0
    unique_ids = set()
    id_to_files = defaultdict(list)
    
    # Recursively scan directory
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                total_files += 1
                
                # Print progress every 10,000 files
                if total_files % 10000 == 0:
                    print(f"Scanned {total_files} files...")
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        if 'id' in data:
                            entity_id = data['id']
                            unique_ids.add(entity_id)
                            id_to_files[entity_id].append(file_path)
                        else:
                            print(f"Warning: No 'id' field found in {file_path}")
                            
                except json.JSONDecodeError:
                    print(f"Warning: Invalid JSON in {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    
    # Find duplicates (IDs with more than one file)
    duplicate_map = {entity_id: files for entity_id, files in id_to_files.items() if len(files) > 1}
    
    return total_files, unique_ids, duplicate_map


def main():
    """Main function to find duplicate IDs in NGSI-LD JSON files."""
    # Check if directory is provided as an argument
    if len(sys.argv) > 1:
        json_directory = sys.argv[1]
    else:
        # Auto-locate the output/json directory
        json_directory = find_project_root()
        if not json_directory:
            print("Error: Could not locate the 'output/json' directory.")
            print("Please run this script from within the project directory,")
            print("or provide the path to the JSON directory as an argument:")
            print("python find_duplicate_ids.py <json_directory>")
            sys.exit(1)
    
    print(f"Scanning directory: {json_directory}")
    print("=" * 80)
    
    # Scan JSON files
    total_files, unique_ids, duplicate_map = scan_json_files(json_directory)
    
    # Calculate statistics
    num_unique_ids = len(unique_ids)
    num_duplicate_ids = len(duplicate_map)
    
    # Print results
    print("\nSummary:")
    print("=" * 80)
    print(f"Total files scanned: {total_files}")
    print(f"Total unique IDs found: {num_unique_ids}")
    print(f"Number of duplicate IDs: {num_duplicate_ids}")
    
    if total_files > num_unique_ids:
        print(f"Duplication rate: {(total_files - num_unique_ids) / total_files * 100:.2f}%")
    
    # Print examples of duplicates
    if duplicate_map:
        print("\nExample Duplicate IDs:")
        print("=" * 80)
        
        # Show up to 10 examples
        count = 0
        for entity_id, files in duplicate_map.items():
            if count >= 10:
                break
                
            print(f"\nDuplicate ID: {entity_id}")
            print(f"Found in {len(files)} files:")
            for file_path in files[:5]:  # Show at most 5 files per ID
                relative_path = os.path.basename(file_path)
                print(f"  - {relative_path}")
            
            if len(files) > 5:
                print(f"  - ... and {len(files) - 5} more files")
                
            count += 1
            
        if num_duplicate_ids > 10:
            print(f"\n... and {num_duplicate_ids - 10} more duplicate IDs")
    
    # Print suggestions
    print("\nSuggestions:")
    print("=" * 80)
    
    if num_duplicate_ids > 0:
        expected_count = total_files - (total_files - num_unique_ids)
        print(f"The discrepancy between your expected count (~285,000) and observed count (~125,000)")
        print(f"can be explained by {total_files - num_unique_ids} duplicate entity IDs.")
        print("\nPossible causes of duplicate IDs:")
        print("1. ID generation algorithm creating collisions")
        print("2. Repeated data processing creating duplicate files")
        print("3. Incorrect concatenation of identifiers")
        
        print("\nSuggestions to fix ID generation:")
        print("1. Use UUID v4 for guaranteed uniqueness: 'urn:ngsi-ld:Sensor:uuid-v4'")
        print("2. Add timestamps to IDs: 'urn:ngsi-ld:Sensor:room_type_timestamp'")
        print("3. Check your ID creation algorithm for incorrect concatenation or loops")
        print("4. Add a database check before file generation to prevent duplicates")
    else:
        print("No duplicate IDs found. If entity counts still don't match, check for:")
        print("1. Failed uploads due to network or server issues")
        print("2. Entity format not compatible with Orion-LD")
        print("3. Different ID formats in files vs. Orion-LD (check case sensitivity, encoding)")


if __name__ == "__main__":
    main()
