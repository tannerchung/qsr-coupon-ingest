#!/usr/bin/env python3
"""
QSR Coupon Data Integration CLI

This module provides the command line interface for importing coupon data from CSV files
and sending them to mParticle as custom events.
"""

import argparse
import logging
import sys
import time
from typing import Dict, Any

from qsr_mparticle.processor import process_csv_data
from qsr_mparticle.utils import setup_logging


def parse_args() -> Dict[str, Any]:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Process QSR coupon CSV data and send to mParticle'
    )
    parser.add_argument('csv_file', help='Path to the CSV file to process')
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=100, 
        help='Batch size for processing'
    )
    parser.add_argument(
        '--max-workers', 
        type=int, 
        default=10, 
        help='Maximum number of parallel workers'
    )
    parser.add_argument(
        '--api-key', 
        required=True, 
        help='mParticle API key'
    )
    parser.add_argument(
        '--api-secret', 
        required=True, 
        help='mParticle API secret'
    )
    parser.add_argument(
        '--environment', 
        choices=['development', 'production'], 
        default='development', 
        help='mParticle environment'
    )
    parser.add_argument(
        '--verbose', 
        action='store_true', 
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--log-file', 
        default='coupon_import.log', 
        help='Path to log file'
    )
    parser.add_argument(
        '--data-center', 
        choices=['us', 'eu'], 
        default='us', 
        help='mParticle data center location'
    )
    parser.add_argument(
        '--retry-failed', 
        action='store_true', 
        default=True,
        help='Retry failed events after initial processing (default: True)'
    )
    parser.add_argument(
        '--no-retry', 
        dest='retry_failed',
        action='store_false',
        help='Disable retrying failed events'
    )
    parser.add_argument(
        '--save-failed', 
        help='Save failed events to this CSV file for manual retry'
    )
    parser.add_argument(
        '--enable-streaming',
        action='store_true',
        default=True,
        help='Enable streaming processing for large files (default: True)'
    )
    parser.add_argument(
        '--disable-streaming',
        dest='enable_streaming',
        action='store_false',
        help='Disable streaming processing'
    )
    parser.add_argument(
        '--enable-deduplication',
        action='store_true',
        default=True,
        help='Enable in-memory deduplication cache (default: True)'
    )
    parser.add_argument(
        '--disable-deduplication',
        dest='enable_deduplication',
        action='store_false',
        help='Disable deduplication cache'
    )
    parser.add_argument(
        '--enable-batching',
        action='store_true',
        default=True,
        help='Enable API request batching (default: True)'
    )
    parser.add_argument(
        '--disable-batching',
        dest='enable_batching',
        action='store_false',
        help='Disable API request batching'
    )
    parser.add_argument(
        '--enable-checkpoints',
        action='store_true',
        default=True,
        help='Enable checkpoint/resume functionality (default: True)'
    )
    parser.add_argument(
        '--disable-checkpoints',
        dest='enable_checkpoints',
        action='store_false',
        help='Disable checkpoint/resume functionality'
    )
    parser.add_argument(
        '--enable-auto-tuning',
        action='store_true',
        default=True,
        help='Enable performance auto-tuning (default: True)'
    )
    parser.add_argument(
        '--disable-auto-tuning',
        dest='enable_auto_tuning',
        action='store_false',
        help='Disable performance auto-tuning'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=5000,
        help='Chunk size for streaming processing (default: 5000)'
    )

    return vars(parser.parse_args())


def main():
    """Main entry point for the CLI."""
    args = parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args['verbose'] else logging.INFO
    setup_logging(log_level, args['log_file'])
    
    logger = logging.getLogger(__name__)
    logger.info("Starting QSR coupon import process")
    
    start_time = time.time()
    
    try:
        # Run the processing
        results = process_csv_data(
            csv_file_path=args['csv_file'],
            api_key=args['api_key'],
            api_secret=args['api_secret'],
            environment=args['environment'],
            batch_size=args['batch_size'],
            max_workers=args['max_workers'],
            data_center=args['data_center'],
            retry_failed=args['retry_failed'],
            save_failed_file=args['save_failed'],
            enable_streaming=args['enable_streaming'],
            enable_deduplication=args['enable_deduplication'],
            enable_batching=args['enable_batching'],
            enable_checkpoints=args['enable_checkpoints'],
            enable_auto_tuning=args['enable_auto_tuning'],
            chunk_size=args['chunk_size']
        )
        
        # Log the results
        elapsed_time = time.time() - start_time
        logger.info(f"Processing complete in {elapsed_time:.2f} seconds")
        
        if 'retry_successful' in results and results['retry_successful'] > 0:
            logger.info(
                f"Results: {results['success']}/{results['total']} successful "
                f"({results['failed']} failed, {results['retry_successful']} recovered through retry)"
            )
        else:
            logger.info(
                f"Results: {results['success']}/{results['total']} successful "
                f"({results['failed']} failed)"
            )
        
        # Log optimization results
        if 'deduplicated' in results and results['deduplicated'] > 0:
            logger.info(f"Optimization: {results['deduplicated']} events deduplicated")
        
        logger.info("Processing completed successfully")
        
        if results['failed'] > 0:
            logger.warning(f"Some events failed to process. Check logs for details.")
            return 1
        
        return 0
        
    except Exception as e:
        logger.exception(f"Process failed with error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
