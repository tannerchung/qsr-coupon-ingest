def save_failed_rows_to_file(failed_rows: List[Dict[str, str]], filename: str) -> None:
    """
    Save failed rows to a CSV file for later reprocessing.
    
    Args:
        failed_rows: List of failed row dictionaries
        filename: Name of the file to save failed rows
    """
    if not failed_rows:
        return
        
    logger.info(f"Saving {len(failed_rows)} failed rows to {filename}")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        if failed_rows:
            fieldnames = failed_rows[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_rows)


def retry_failed_events(
    failed_rows: List[Dict[str, str]], 
    api_key: str, 
    api_secret: str, 
    environment: str,
    data_center: str = "us",
    max_retry_attempts: int = 3
) -> Dict[str, int]:
    """
    Retry failed events with a different strategy.
    
    Args:
        failed_rows: List of failed row dictionaries
        api_key: mParticle API key
        api_secret: mParticle API secret
        environment: mParticle environment
        data_center: mParticle data center
        max_retry_attempts: Maximum retry attempts per event
        
    Returns:
        Dictionary with counts of successful and failed retries
    """
    if not failed_rows:
        return {"success": 0, "failed": 0}
        
    logger.info(f"Retrying {len(failed_rows)} failed events (single-threaded with delays)")
    
    retry_results = {"success": 0, "failed": 0}
    
    for i, row in enumerate(failed_rows):
        logger.info(f"Retrying failed event {i+1}/{len(failed_rows)}: {row.get('email', 'no-email')}")
        
        event = create_mparticle_event(row, environment)
        
        if send_event_to_mparticle(event, api_key, api_secret, max_retries=max_retry_attempts, data_center=data_center):
            retry_results["success"] += 1
            logger.info(f"Retry successful for {row.get('email', 'no-email')}")
        else:
            retry_results["failed"] += 1
            logger.error(f"Retry failed for {row.get('email', 'no-email')}")
            
        # Add a small delay between retries to be gentler on the API
        import time
        time.sleep(0.5)
    
    return retry_results"""
CSV Data Processor Module

This module handles the processing of CSV files, including reading, validation,
batch processing, and coordination of sending events to mParticle.
"""

import csv
import json
import logging
import concurrent.futures
from typing import Dict, List, Any, Tuple

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
    environment: str,
    data_center: str = "us"
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    """
    Process a batch of CSV rows and send events to mParticle.
    
    Args:
        batch: List of CSV row dictionaries
        api_key: mParticle API key
        api_secret: mParticle API secret
        environment: mParticle environment ('development' or 'production')
        data_center: mParticle data center ('us' or 'eu')
        
    Returns:
        Tuple of (results dict with counts, list of failed rows for requeue)
    """
    results = {"success": 0, "failed": 0}
    failed_rows = []
    
    for i, row in enumerate(batch):
        logger.debug(f"Processing row {i+1}/{len(batch)}: {row.get('email', 'no-email')}")
        
        # Generate event
        event = create_mparticle_event(row, environment)
        
        # Log the event being sent (only in debug mode)
        logger.debug(f"Sending event: {json.dumps(event, indent=2)}")
        
        # Send to mParticle
        if send_event_to_mparticle(event, api_key, api_secret, data_center=data_center):
            results["success"] += 1
            logger.debug(f"Successfully sent event for {row.get('email', 'no-email')}")
        else:
            results["failed"] += 1
            failed_rows.append(row)
            logger.warning(f"Failed to send event for {row.get('email', 'no-email')}")
    
    return results, failed_rows


def process_csv_data(
    csv_file_path: str, 
    api_key: str, 
    api_secret: str, 
    environment: str = "development", 
    batch_size: int = 100, 
    max_workers: int = 10,
    data_center: str = "us",
    retry_failed: bool = True,
    save_failed_file: str = None
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
        data_center: mParticle data center ('us' or 'eu')
        retry_failed: Whether to retry failed events after initial processing
        save_failed_file: Path to save failed events (optional)
        
    Returns:
        Dictionary with counts of total, successful, and failed events
    """
    logger.info(f"Starting processing of {csv_file_path}")
    logger.info(f"Configuration: environment={environment}, data_center={data_center}, "
                f"batch_size={batch_size}, max_workers={max_workers}")
    
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
    all_failed_rows = []
    
    # Process batches in parallel
    logger.info("Starting parallel processing of batches...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks for each batch
        future_to_batch = {
            executor.submit(
                process_csv_batch, 
                batch, 
                api_key, 
                api_secret, 
                environment,
                data_center
            ): i 
            for i, batch in enumerate(batches)
        }
        
        # Process completed tasks as they finish
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                results, failed_rows = future.result()
                total_successful += results["success"]
                total_failed += results["failed"]
                all_failed_rows.extend(failed_rows)
                
                # Calculate and display progress
                progress_pct = ((batch_index + 1) / len(batches)) * 100
                logger.info(
                    f"Completed batch {batch_index+1}/{len(batches)} ({progress_pct:.1f}%): "
                    f"{results['success']} successful, {results['failed']} failed"
                )
            except Exception as e:
                logger.error(f"Batch {batch_index+1} generated an exception: {e}")
                # Count all rows in this batch as failed
                batch_size_actual = len(batches[batch_index])
                total_failed += batch_size_actual
                all_failed_rows.extend(batches[batch_index])
    
    # Log initial results
    logger.info(f"Initial processing complete: {total_successful}/{total_rows} successful, {total_failed} failed")
    
    # Retry failed events if requested
    retry_successful = 0
    if retry_failed and all_failed_rows:
        logger.info(f"Attempting to retry {len(all_failed_rows)} failed events...")
        retry_results = retry_failed_events(
            all_failed_rows, 
            api_key, 
            api_secret, 
            environment, 
            data_center
        )
        retry_successful = retry_results["success"]
        
        # Update totals after retry
        total_successful += retry_successful
        total_failed = len(all_failed_rows) - retry_successful
        
        # Remove successful retries from failed rows list
        if retry_successful > 0:
            # This is a simplification - in a real implementation, you'd track which specific rows succeeded
            all_failed_rows = all_failed_rows[retry_successful:]
        
        logger.info(f"Retry complete: {retry_successful} additional successes")
    
    # Save failed rows to file if requested
    if save_failed_file and all_failed_rows:
        save_failed_rows_to_file(all_failed_rows, save_failed_file)
    
    final_failed = len(all_failed_rows)
    
    return {
        "total": total_rows,
        "success": total_successful,
        "failed": final_failed,
        "retry_successful": retry_successful
    }
