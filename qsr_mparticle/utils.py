"""
Utility Module with Advanced Optimizations

This module provides helper functions including deduplication caching,
performance monitoring, and checkpoint management.
"""

import hashlib
import json
import logging
import pickle
import os
import time
import psutil
from datetime import datetime
from typing import Dict, Any, Optional, Set
from collections import deque
from statistics import mean


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


class DeduplicationCache:
    """In-memory cache for deduplication of events within a single run"""
    
    def __init__(self, max_size: int = 50000):
        self.sent_events: Set[str] = set()
        self.max_size = max_size
        self.logger = logging.getLogger(__name__)
    
    def is_duplicate(self, event_data: Dict[str, Any]) -> bool:
        """
        Check if event was already processed in this run
        
        Args:
            event_data: Dictionary containing event data
            
        Returns:
            True if event is a duplicate, False otherwise
        """
        event_hash = self._hash_event(event_data)
        
        if event_hash in self.sent_events:
            self.logger.debug("Skipping duplicate event")
            return True
        
        # Add to cache (with size limit)
        if len(self.sent_events) >= self.max_size:
            # Remove oldest 20% when cache is full
            removal_count = int(self.max_size * 0.2)
            old_events = list(self.sent_events)[:removal_count]
            for old_event in old_events:
                self.sent_events.discard(old_event)
            self.logger.info(f"Cache cleanup: removed {removal_count} old entries")
        
        self.sent_events.add(event_hash)
        return False
    
    def _hash_event(self, event_data: Dict[str, Any]) -> str:
        """Generate hash for event data"""
        # Use a subset of event data for hashing to avoid timestamp differences
        hashable_data = {
            'email': event_data.get('user_identities', {}).get('email', ''),
            'custom_attributes': event_data.get('events', [{}])[0].get('data', {}).get('custom_attributes', {})
        }
        return hashlib.md5(str(sorted(hashable_data.items())).encode()).hexdigest()
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {
            'cached_events': len(self.sent_events),
            'max_size': self.max_size
        }


class ProcessingCheckpoint:
    """Checkpoint system for resumable processing"""
    
    def __init__(self, checkpoint_file: str = 'processing_checkpoint.pkl'):
        self.checkpoint_file = checkpoint_file
        self.processed_count = 0
        self.failed_events = []
        self.start_time = time.time()
        self.total_rows = 0
        self.success_count = 0
        self.logger = logging.getLogger(__name__)
    
    def save_checkpoint(self) -> None:
        """Save current progress to disk"""
        checkpoint_data = {
            'processed_count': self.processed_count,
            'failed_events': self.failed_events,
            'start_time': self.start_time,
            'total_rows': self.total_rows,
            'success_count': self.success_count,
            'timestamp': time.time()
        }
        
        try:
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            self.logger.debug(f"Checkpoint saved: {self.processed_count}/{self.total_rows} processed")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> bool:
        """
        Resume from saved checkpoint
        
        Returns:
            True if checkpoint was loaded, False if no checkpoint exists
        """
        if not os.path.exists(self.checkpoint_file):
            return False
        
        try:
            with open(self.checkpoint_file, 'rb') as f:
                data = pickle.load(f)
                
            self.processed_count = data.get('processed_count', 0)
            self.failed_events = data.get('failed_events', [])
            self.start_time = data.get('start_time', time.time())
            self.total_rows = data.get('total_rows', 0)
            self.success_count = data.get('success_count', 0)
            
            self.logger.info(f"Checkpoint loaded: {self.processed_count}/{self.total_rows} processed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to load checkpoint: {e}")
            return False
    
    def clean_checkpoint(self) -> None:
        """Remove checkpoint file after successful completion"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                self.logger.info("Checkpoint file cleaned up")
        except Exception as e:
            self.logger.error(f"Failed to clean checkpoint: {e}")
    
    def should_save_checkpoint(self) -> bool:
        """Determine if checkpoint should be saved (every 1000 events)"""
        return self.processed_count % 1000 == 0


class PerformanceMonitor:
    """Monitor and auto-tune performance parameters"""
    
    def __init__(self):
        self.metrics = {
            'batch_times': deque(maxlen=20),
            'success_rates': deque(maxlen=20),
            'memory_usage': deque(maxlen=20),
            'cpu_usage': deque(maxlen=20),
            'throughput': deque(maxlen=20)
        }
        self.optimal_batch_size = 100
        self.optimal_workers = 10
        self.logger = logging.getLogger(__name__)
    
    def record_batch_metrics(self, batch_size: int, workers: int, duration: float, 
                           success_count: int, total_count: int) -> None:
        """
        Record performance metrics for a batch
        
        Args:
            batch_size: Size of the processed batch
            workers: Number of workers used
            duration: Time taken to process the batch
            success_count: Number of successful events
            total_count: Total number of events in batch
        """
        self.metrics['batch_times'].append(duration)
        self.metrics['success_rates'].append(success_count / total_count if total_count > 0 else 0)
        self.metrics['memory_usage'].append(psutil.virtual_memory().percent)
        self.metrics['cpu_usage'].append(psutil.cpu_percent())
        self.metrics['throughput'].append(total_count / duration if duration > 0 else 0)
        
        self.logger.debug(f"Batch metrics: {batch_size} events, {workers} workers, "
                         f"{duration:.2f}s, {success_count}/{total_count} success")
    
    def auto_tune_parameters(self) -> tuple[int, int]:
        """
        Automatically adjust batch size and worker count based on performance
        
        Returns:
            Tuple of (optimal_batch_size, optimal_workers)
        """
        if len(self.metrics['batch_times']) < 5:
            return self.optimal_batch_size, self.optimal_workers
        
        avg_success_rate = mean(self.metrics['success_rates'])
        avg_memory = mean(self.metrics['memory_usage'])
        avg_cpu = mean(self.metrics['cpu_usage'])
        avg_throughput = mean(self.metrics['throughput'])
        
        # Auto-tuning logic
        previous_batch_size = self.optimal_batch_size
        previous_workers = self.optimal_workers
        
        # Increase performance if system can handle it
        if (avg_memory < 70 and avg_cpu < 80 and avg_success_rate > 0.95 and 
            avg_throughput > 10):  # 10 events/second threshold
            
            self.optimal_batch_size = min(200, int(self.optimal_batch_size * 1.1))
            self.optimal_workers = min(20, self.optimal_workers + 1)
        
        # Decrease if performance is poor
        elif avg_success_rate < 0.8 or avg_memory > 90 or avg_cpu > 95:
            self.optimal_batch_size = max(50, int(self.optimal_batch_size * 0.9))
            self.optimal_workers = max(3, self.optimal_workers - 1)
        
        # Log changes
        if (self.optimal_batch_size != previous_batch_size or 
            self.optimal_workers != previous_workers):
            self.logger.info(f"Auto-tuned parameters: batch_size {previous_batch_size}→{self.optimal_batch_size}, "
                           f"workers {previous_workers}→{self.optimal_workers}")
        
        return self.optimal_batch_size, self.optimal_workers
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get summary of performance metrics"""
        if not self.metrics['batch_times']:
            return {}
        
        return {
            'avg_batch_time': mean(self.metrics['batch_times']),
            'avg_success_rate': mean(self.metrics['success_rates']),
            'avg_memory_usage': mean(self.metrics['memory_usage']),
            'avg_cpu_usage': mean(self.metrics['cpu_usage']),
            'avg_throughput': mean(self.metrics['throughput']),
            'optimal_batch_size': self.optimal_batch_size,
            'optimal_workers': self.optimal_workers
        }
