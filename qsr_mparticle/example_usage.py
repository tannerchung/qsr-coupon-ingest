#!/usr/bin/env python3
"""
Example script demonstrating how to use the QSR mParticle integration.
"""

import os
import sys
import logging
from qsr_mparticle.processor import process_csv_data
from qsr_mparticle.utils import setup_logging

# Configure logging
setup_logging(logging.INFO, 'example_import.log')
logger = logging.getLogger(__name__)

# Configuration
CSV_FILE = 'examples/sample.csv'  # Path to your CSV file
API_KEY = os.environ.get('MPARTICLE_API_KEY')  # Get from environment variables
API_SECRET = os.environ.get('MPARTICLE_API_SECRET')  # Get from environment variables

# Ensure we have API credentials
if not API_KEY or not API_SECRET:
    logger.error("API key and secret must be provided via environment variables")
    print("Error: Set MPARTICLE_API_KEY and MPARTICLE_API_SECRET environment variables")
    sys.exit(1)

def main():
    """Run the example."""
    print("Starting QSR coupon import process...")
    
    try:
        # Process the CSV file
        results = process_csv_data(
            csv_file_path=CSV_FILE,
            api_key=API_KEY,
            api_secret=API_SECRET,
            environment='development',
            batch_size=50,
            max_workers=5
        )
        
        # Display results
        print(f"Processing complete!")
        print(f"Results: {results['success']}/{results['total']} successful ({results['failed']} failed)")
        
        if results['failed'] > 0:
            print("Some events failed to process. Check logs for details.")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        logger.exception("Process failed with error")
        return 1

if __name__ == "__main__":
    sys.exit(main())
