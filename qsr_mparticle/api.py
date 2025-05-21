"""
mParticle API Integration Module

This module handles the communication with mParticle's Server-to-Server API,
including sending events and handling rate limiting with exponential backoff.
"""

import logging
import random
import time
import requests
from typing import Dict, Any


logger = logging.getLogger(__name__)

# mParticle API configuration
MPARTICLE_API_URL = "https://s2s.mparticle.com/v2/events"


def send_event_to_mparticle(
    event: Dict[str, Any], 
    api_key: str, 
    api_secret: str, 
    max_retries: int = 5
) -> bool:
    """
    Send event to mParticle with exponential backoff and jitter.
    
    Args:
        event: mParticle event payload
        api_key: mParticle API key
        api_secret: mParticle API secret
        max_retries: Maximum number of retry attempts
        
    Returns:
        Boolean indicating success or failure
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.post(
                MPARTICLE_API_URL,
                json=event,
                auth=(api_key, api_secret),
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 429:  # Rate limited
                # Calculate backoff time with jitter
                backoff_seconds = min(30, (2 ** retry_count))
                jitter = random.uniform(0, 1)
                backoff_time = backoff_seconds + jitter
                
                logger.warning(f"Rate limited. Retrying in {backoff_time:.2f} seconds")
                time.sleep(backoff_time)
                retry_count += 1
                continue
            
            if 200 <= response.status_code < 300:
                # Success
                logger.debug(f"Successfully sent event to mParticle")
                return True
            else:
                logger.error(f"Error sending event to mParticle: {response.status_code}, {response.text}")
                
                # For certain errors, we might want to retry
                if response.status_code >= 500:  # Server errors
                    backoff_seconds = min(30, (2 ** retry_count))
                    jitter = random.uniform(0, 1)
                    backoff_time = backoff_seconds + jitter
                    
                    logger.warning(f"Server error. Retrying in {backoff_time:.2f} seconds")
                    time.sleep(backoff_time)
                    retry_count += 1
                    continue
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception when sending event: {e}")
            
            # Calculate backoff time with jitter
            backoff_seconds = min(30, (2 ** retry_count))
            jitter = random.uniform(0, 1)
            backoff_time = backoff_seconds + jitter
            
            logger.warning(f"Retrying in {backoff_time:.2f} seconds")
            time.sleep(backoff_time)
            retry_count += 1
    
    # If we've exhausted retries
    logger.error(f"Failed to send event after {max_retries} attempts")
    return False
