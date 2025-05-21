"""
CSV Data Processor Module

This module handles the processing of CSV files, including reading, validation,
batch processing, and coordination of sending events to mParticle.
"""

import csv
import logging
import concurrent.futures
from typing import Dict, List, Any

from qsr_mparticle.api import send_event_to_mparticle
from qsr_mparticle.utils import generate_unique_id, create_mparticle_event


logger = logging.getLogger(__name__)


def read_and_validate_csv(file_path: str) -> List[Dict[str, str]]:
    """
    Read CSV file and validate its format.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        List of dictionaries containing the CSV data
        
    Raises:
        ValueError: If the CSV is missing required columns
        FileNotFoundError: If the CSV file is not found
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Check for required columns
            if not reader.fieldnames:
                raise ValueError("CSV file has no headers")
                
            required_columns = ['email', 'coupon_code']
            missing_columns = [col for col in required_columns if col not in reader.fieldnames]
            
            if missing_columns:
                raise ValueError(f"CSV file is missing required columns: {', '.join(missing_columns)}")
            
            return list(reader)
            
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise


def process_csv_batch(
    batch: List[Dict[str, str]], 
    api_key: str, 
    api_secret: str, 
    environment: str
) -> Dict[str, int]:
    """
    Process a batch of CSV rows and send events to mParticle.
    
    Args:
        batch: List of CSV row dictionaries
        api_key: mParticle API key
        api_secret: mParticle API secret
        environment: mParticle environment ('development' or 'production')
        
    Returns:
        Dictionary with counts of successful and failed events
    """
    results = {"success": 0, "failed": 0}
    
    for row in batch:
        # Generate event
        event = create_mparticle_event(row, environment)
        
        # Send to mParticle
        if send_event_to_mparticle(event, api_key, api_secret):
            results["success"] += 1
        else:
            results["failed"] += 1
    
    return results


def process_csv_data(
    csv_file_path: str, 
    api_key: str, 
    api_secret: str, 
    environment: str = "development", 
    batch_size: int = 100, 
    max_workers: int = 10
) -> Dict[str, int]:
    """
    Process all data from a CSV file and send to mParticle.
    
    Args:
        csv_file_path: Path to the CSV file
        api_key: mParticle API key
        api_secret: mParticle API secret
        environment: mParticle environment ('development' or 'production')
        batch_size: Number of records to process in each batch
        max_workers: Maximum number of parallel processing threads
        
    Returns:
        Dictionary with counts of total, successful, and failed events
    """
    logger.info(f"Starting processing of {csv_file_path}")
    
    # Read and validate CSV data
    csv_data = read_and_validate_csv(csv_file_path)
    
    total_rows = len(csv_data)
    logger.info(f"Found {total_rows} rows in CSV")
    
    if total_rows == 0:
        logger.warning("CSV file is empty or has no data rows")
        return {"total": 0, "success": 0, "failed": 0}
    
    # Split data into batches
    batches = [csv_data[i:i+batch_size] for i in range(0, total_rows, batch_size)]
    logger.info(f"Split into {len(batches)} batches of up to {batch_size} rows each")
    
    total_successful = 0
    total_failed = 0
    
    # Process batches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks for each batch
        future_to_batch = {
            executor.submit(
                process_csv_batch, 
                batch, 
                api_key, 
                api_secret, 
                environment
            ): i 
            for i, batch in enumerate(batches)
        }
        
        # Process completed tasks as they finish
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                results = future.result()
                total_successful += results["success"]
                total_failed += results["failed"]
                
                # Calculate and display progress
                progress_pct = ((batch_index + 1) / len(batches)) * 100
                logger.info(
                    f"Completed batch {batch_index+1}/{len(batches)} ({progress_pct:.1f}%): "
                    f"{results['success']} successful, {results['failed']} failed"
                )
            except Exception as e:
                logger.error(f"Batch {batch_index+1} generated an exception: {e}")
                # Count all rows in this batch as failed
                total_failed += len(batches[batch_index])
    
    return {
        "total": total_rows,
        "success": total_successful,
        "failed": total_failed
    }
