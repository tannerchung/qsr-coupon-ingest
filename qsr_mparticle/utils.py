"""
Utility Module

This module provides helper functions for the QSR coupon integration.
"""

import hashlib
import json
import logging
from datetime import datetime
from typing import Dict, Any


def setup_logging(log_level: int, log_file: str) -> None:
    """
    Configure logging for the application.
    
    Args:
        log_level: Logging level (e.g., logging.INFO, logging.DEBUG)
        log_file: Path to the log file
    """
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )


def generate_unique_id(row: Dict[str, str]) -> str:
    """
    Generate a deterministic unique ID based on row data for deduplication.
    This ensures the same ID will be generated if the same row is processed multiple times.
    
    Args:
        row: Dictionary containing row data from CSV
        
    Returns:
        String containing a UUID-like unique ID
    """
    # Create a string representation of the row to hash
    row_str = json.dumps(row, sort_keys=True)
    
    # Generate a hash
    hash_obj = hashlib.sha256(row_str.encode())
    
    # Return a UUID-like string based on the hash
    hash_hex = hash_obj.hexdigest()
    return f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}"


def create_mparticle_event(row: Dict[str, str], environment: str = "development") -> Dict[str, Any]:
    """
    Create mParticle event from CSV row.
    
    Args:
        row: Dictionary containing row data from CSV
        environment: mParticle environment ('development' or 'production')
        
    Returns:
        Dictionary containing formatted mParticle event
    """
    # Generate unique ID for deduplication
    event_id = generate_unique_id(row)
    
    # Get current timestamp in milliseconds
    current_time_ms = int(datetime.now().timestamp() * 1000)
    
    # Create basic event structure following mParticle API specs
    event = {
        "schema_version": 2,
        "environment": environment,
        "events": [
            {
                "data": {
                    "event_name": "qsr_coupon_signup",
                    "custom_event_type": "other",
                    "timestamp_unixtime_ms": current_time_ms,
                    "custom_attributes": {
                        "event_id": event_id  # Include the unique ID as an attribute
                    }
                },
                "event_type": "custom_event"
            }
        ],
        "user_identities": {},
        "device_info": {
            "platform": "web"
        }
    }
    
    # Add user identities from the CSV if they exist
    if "email" in row and row["email"]:
        event["user_identities"]["email"] = row["email"]
    if "customer_id" in row and row["customer_id"]:
        event["user_identities"]["customer_id"] = row["customer_id"]
    
    # Add remaining CSV columns as custom attributes
    for key, value in row.items():
        if key not in ["email", "customer_id"] and value:  # Skip fields already used elsewhere
            event["events"][0]["data"]["custom_attributes"][key] = value
    
    return event
