"""
CSV Data Processor Module with Advanced Optimizations

This module handles streaming CSV processing, batch optimization, caching,
checkpoint management, and performance monitoring.
"""

import csv
import json
import logging
import concurrent.futures
import pandas as pd
import time
from typing import Dict, List, Any, Tuple, Generator, Optional

from qsr_mparticle.api import OptimizedMParticleClient, send_events_batch_to_mparticle
from qsr_mparticle.utils import (
    generate_unique_id, create_mparticle_event, DeduplicationCache,
    ProcessingCheckpoint, PerformanceMonitor
)


logger = logging.getLogger(__name__)


def read_and_validate_csv(file_path: str) -> List[Dict[str, str]]:
    """
    Read CSV file and validate its format (for small files).
    
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


def stream_csv_chunks(file_path: str, chunk_size: int = 1000) -> Generator[List[Dict[str, str]], None, None]:
    """
    Stream CSV file in chunks for memory-efficient processing of large files.
    
    Args:
        file_path: Path to the CSV file
        chunk_size: Number of rows per chunk
        
    Yields:
        List of dictionaries for each chunk
    """
    logger.info(f"Streaming CSV file in chunks of {chunk_size}")
    
    try:
        # Use pandas for efficient chunked reading
        chunk_count = 0
        for chunk_df in pd.read_csv(file_path, chunksize=chunk_size):
            chunk_count += 1
            
            # Validate required columns in first chunk
            if chunk_count == 1:
                required_columns = ['email', 'coupon_code']
                missing_columns = [col for col in required_columns if col not in chunk_df.columns]
                
                if missing_columns:
                    raise ValueError(f"CSV file is missing required columns: {', '.join(missing_columns)}")
            
            # Convert to list of dictionaries
            chunk_data = chunk_df.to_dict('records')
            
            # Clean up any NaN values
            for row in chunk_data:
                for key, value in row.items():
                    if pd.isna(value):
                        row[key] = ""
            
            logger.debug(f"Yielding chunk {chunk_count} with {len(chunk_data)} rows")
            yield chunk_data
            
    except Exception as e:
        logger.error(f"Error streaming CSV file: {e}")
        raise


def process_csv_batch_optimized(
    batch: List[Dict[str, str]], 
    api_key: str, 
    api_secret: str, 
    environment: str,
    data_center: str = "us",
    dedup_cache: Optional[DeduplicationCache] = None,
    enable_batching: bool = True
) -> Tuple[Dict[str, int], List[Dict[str, str]]]:
    """
    Process a batch of CSV rows with all optimizations enabled.
    
    Args:
        batch: List of CSV row dictionaries
        api_key: mParticle API key
        api_secret: mParticle API secret
        environment: mParticle environment ('development' or 'production')
        data_center: mParticle data center ('us' or 'eu')
        dedup_cache: Deduplication cache instance
        enable_batching: Whether to use API request batching
        
    Returns:
        Tuple of (results dict with counts, list of failed rows for requeue)
    """
    results = {"success": 0, "failed": 0, "deduplicated": 0}
    failed_rows = []
    events_to_send = []
    
    # Process each row and create events
    for i, row in enumerate(batch):
        logger.debug(f"Processing row {i+1}/{len(batch)}: {row.get('email', 'no-email')}")
        
        # Generate event
        event = create_mparticle_event(row, environment)
        
        # Check for duplicates if cache is enabled
        if dedup_cache and dedup_cache.is_duplicate(event):
            results["deduplicated"] += 1
            logger.debug(f"Skipped duplicate event for {row.get('email', 'no-email')}")
            continue
        
        events_to_send.append((event, row))
    
    # Send events using optimized client
    if events_to_send:
        if enable_batching:
            # Use batch API for efficiency
            events_only = [event for event, _ in events_to_send]
            batch_results = send_events_batch_to_mparticle(
                events_only, api_key, api_secret, data_center, batch_size=10
            )
            
            # For batch sending, we can't determine individual failures
            # So we assume all succeeded or all failed based on batch result
            if batch_results["success"] > 0:
                results["success"] += len(events_to_send)
            else:
                results["failed"] += len(events_to_send)
                failed_rows.extend([row for _, row in events_to_send])
        else:
            # Send individual events for more precise error handling
            client = OptimizedMParticleClient(
                api_key=api_key,
                api_secret=api_secret,
                data_center=data_center,
                enable_batching=False
            )
            
            try:
                for event, row in events_to_send:
                    if client.send_event(event):
                        results["success"] += 1
                        logger.debug(f"Successfully sent event for {row.get('email', 'no-email')}")
                    else:
                        results["failed"] += 1
                        failed_rows.append(row)
                        logger.warning(f"Failed to send event for {row.get('email', 'no-email')}")
            finally:
                client.close()
    
    return results, failed_rows


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
    
    # Use single-threaded approach for retries
    client = OptimizedMParticleClient(
        api_key=api_key,
        api_secret=api_secret,
        data_center=data_center,
        enable_batching=False  # Individual events for precise control
    )
    
    try:
        for i, row in enumerate(failed_rows):
            logger.info(f"Retrying failed event {i+1}/{len(failed_rows)}: {row.get('email', 'no-email')}")
            
            event = create_mparticle_event(row, environment)
            
            if client.send_event(event):
                retry_results["success"] += 1
                logger.info(f"Retry successful for {row.get('email', 'no-email')}")
            else:
                retry_results["failed"] += 1
                logger.error(f"Retry failed for {row.get('email', 'no-email')}")
                
            # Add a small delay between retries to be gentler on the API
            time.sleep(0.5)
    finally:
        client.close()
    
    return retry_results


def process_csv_data(
    csv_file_path: str, 
    api_key: str, 
    api_secret: str, 
    environment: str = "development", 
    batch_size: int = 100, 
    max_workers: int = 10,
    data_center: str = "us",
    retry_failed: bool = True,
    save_failed_file: str = None,
    enable_streaming: bool = True,
    enable_deduplication: bool = True,
    enable_batching: bool = True,
    enable_checkpoints: bool = True,
    enable_auto_tuning: bool = True,
    chunk_size: int = 5000
) -> Dict[str, int]:
    """
    Process all data from a CSV file with all optimizations enabled.
    
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
        enable_streaming: Use streaming for large files
        enable_deduplication: Enable in-memory deduplication cache
        enable_batching: Enable API request batching
        enable_checkpoints: Enable checkpoint/resume functionality
        enable_auto_tuning: Enable performance auto-tuning
        chunk_size: Size of chunks for streaming processing
        
    Returns:
        Dictionary with counts of total, successful, and failed events
    """
    logger.info(f"Starting optimized processing of {csv_file_path}")
    logger.info(f"Configuration: environment={environment}, data_center={data_center}, "
                f"batch_size={batch_size}, max_workers={max_workers}")
    logger.info(f"Optimizations: streaming={enable_streaming}, dedup={enable_deduplication}, "
                f"batching={enable_batching}, checkpoints={enable_checkpoints}, auto_tune={enable_auto_tuning}")
    
    # Initialize optimization components
    dedup_cache = DeduplicationCache() if enable_deduplication else None
    checkpoint = ProcessingCheckpoint() if enable_checkpoints else None
    performance_monitor = PerformanceMonitor() if enable_auto_tuning else None
    
    # Try to resume from checkpoint
    if checkpoint and checkpoint.load_checkpoint():
        logger.info(f"Resuming from checkpoint: {checkpoint.processed_count} events processed")
    
    total_successful = 0
    total_failed = 0
    total_processed = 0
    all_failed_rows = []
    
    try:
        # Determine processing approach based on file size and settings
        if enable_streaming:
            # Stream large files in chunks
            logger.info("Using streaming processing for memory efficiency")
            
            for chunk in stream_csv_chunks(csv_file_path, chunk_size):
                if not chunk:
                    continue
                
                chunk_start_time = time.time()
                
                # Auto-tune parameters if enabled
                if performance_monitor:
                    batch_size, max_workers = performance_monitor.auto_tune_parameters()
                
                # Process chunk in batches
                chunk_results = process_chunk_in_batches(
                    chunk, api_key, api_secret, environment, data_center,
                    batch_size, max_workers, dedup_cache, enable_batching
                )
                
                # Update totals
                total_successful += chunk_results["success"]
                total_failed += chunk_results["failed"]
                total_processed += len(chunk)
                all_failed_rows.extend(chunk_results.get("failed_rows", []))
                
                # Record performance metrics
                if performance_monitor:
                    chunk_duration = time.time() - chunk_start_time
                    performance_monitor.record_batch_metrics(
                        len(chunk), max_workers, chunk_duration,
                        chunk_results["success"], len(chunk)
                    )
                
                # Save checkpoint if enabled
                if checkpoint:
                    checkpoint.processed_count = total_processed
                    checkpoint.success_count = total_successful
                    checkpoint.total_rows = total_processed  # Update as we go
                    if checkpoint.should_save_checkpoint():
                        checkpoint.save_checkpoint()
                
                logger.info(f"Processed chunk: {chunk_results['success']}/{len(chunk)} successful, "
                           f"Total: {total_successful}/{total_processed}")
        
        else:
            # Traditional approach for smaller files
            logger.info("Using traditional processing approach")
            csv_data = read_and_validate_csv(csv_file_path)
            total_rows = len(csv_data)
            
            if checkpoint:
                checkpoint.total_rows = total_rows
            
            if total_rows == 0:
                logger.warning("CSV file is empty or has no data rows")
                return {"total": 0, "success": 0, "failed": 0}
            
            # Process in batches
            batches = [csv_data[i:i+batch_size] for i in range(0, total_rows, batch_size)]
            logger.info(f"Split into {len(batches)} batches of up to {batch_size} rows each")
            
            batch_results = process_chunk_in_batches(
                csv_data, api_key, api_secret, environment, data_center,
                batch_size, max_workers, dedup_cache, enable_batching
            )
            
            total_successful = batch_results["success"]
            total_failed = batch_results["failed"]
            total_processed = total_rows
            all_failed_rows = batch_results.get("failed_rows", [])
    
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        if checkpoint:
            checkpoint.save_checkpoint()
        raise
    
    # Log initial results
    deduplicated_count = getattr(dedup_cache, 'deduplicated_count', 0) if dedup_cache else 0
    logger.info(f"Initial processing complete: {total_successful}/{total_processed} successful, "
                f"{total_failed} failed, {deduplicated_count} deduplicated")
    
    # Retry failed events if requested
    retry_successful = 0
    if retry_failed and all_failed_rows:
        logger.info(f"Attempting to retry {len(all_failed_rows)} failed events...")
        retry_results = retry_failed_events(
            all_failed_rows, api_key, api_secret, environment, data_center
        )
        retry_successful = retry_results["success"]
        
        # Update totals after retry
        total_successful += retry_successful
        final_failed = len(all_failed_rows) - retry_successful
        
        logger.info(f"Retry complete: {retry_successful} additional successes")
    else:
        final_failed = len(all_failed_rows)
    
    # Save failed rows to file if requested
    if save_failed_file and all_failed_rows:
        remaining_failed = all_failed_rows[retry_successful:] if retry_successful > 0 else all_failed_rows
        save_failed_rows_to_file(remaining_failed, save_failed_file)
    
    # Clean up checkpoint on success
    if checkpoint and final_failed == 0:
        checkpoint.clean_checkpoint()
    elif checkpoint:
        checkpoint.save_checkpoint()
    
    # Log performance summary
    if performance_monitor:
        perf_summary = performance_monitor.get_performance_summary()
        logger.info(f"Performance summary: {perf_summary}")
    
    # Log deduplication stats
    if dedup_cache:
        cache_stats = dedup_cache.get_stats()
        logger.info(f"Deduplication stats: {cache_stats}")
    
    return {
        "total": total_processed,
        "success": total_successful,
        "failed": final_failed,
        "retry_successful": retry_successful,
        "deduplicated": deduplicated_count
    }


def process_chunk_in_batches(
    data: List[Dict[str, str]],
    api_key: str,
    api_secret: str,
    environment: str,
    data_center: str,
    batch_size: int,
    max_workers: int,
    dedup_cache: Optional[DeduplicationCache],
    enable_batching: bool
) -> Dict[str, Any]:
    """Process a chunk of data in parallel batches"""
    
    # Split data into batches
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    
    total_successful = 0
    total_failed = 0
    all_failed_rows = []
    
    # Process batches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create tasks for each batch
        future_to_batch = {
            executor.submit(
                process_csv_batch_optimized, 
                batch, 
                api_key, 
                api_secret, 
                environment,
                data_center,
                dedup_cache,
                enable_batching
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
    
    return {
        "success": total_successful,
        "failed": total_failed,
        "failed_rows": all_failed_rows
    }
