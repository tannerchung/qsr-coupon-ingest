"""
mParticle API Integration Module with Advanced Optimizations

This module handles the communication with mParticle's Server-to-Server API,
including batch processing, connection pooling, rate limiting, and circuit breaker patterns.
"""

import logging
import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import deque
from typing import Dict, List, Any, Optional
import threading


logger = logging.getLogger(__name__)

# mParticle API configuration
MPARTICLE_API_ENDPOINTS = {
    "us": "https://s2s.mparticle.com/v2/events",
    "eu": "https://s2s.eu-west-1.mparticle.com/v2/events"
}


class RateLimiter:
    """Proactive rate limiter to prevent hitting API limits"""
    
    def __init__(self, max_requests: int = 100, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
    
    def acquire(self) -> None:
        """Acquire permission to make a request, blocking if necessary"""
        with self.lock:
            now = time.time()
            
            # Remove old requests outside time window
            while self.requests and self.requests[0] <= now - self.time_window:
                self.requests.popleft()
            
            # Check if we can make request
            if len(self.requests) >= self.max_requests:
                sleep_time = self.time_window - (now - self.requests[0]) + 0.1
                logger.warning(f"Rate limit reached. Waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                
                # Recheck after waiting
                now = time.time()
                while self.requests and self.requests[0] <= now - self.time_window:
                    self.requests.popleft()
            
            self.requests.append(now)


class CircuitBreaker:
    """Circuit breaker pattern to handle systematic failures"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED -> OPEN -> HALF_OPEN -> CLOSED
        self.lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                    logger.info("Circuit breaker moving to HALF_OPEN state")
                else:
                    raise Exception("Circuit breaker is OPEN - API calls blocked")
        
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            raise e
    
    def _record_failure(self):
        """Record a failure and potentially open the circuit"""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logger.error(f"Circuit breaker OPENED after {self.failure_count} failures")
    
    def _record_success(self):
        """Record a success and potentially close the circuit"""
        with self.lock:
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                logger.info("Circuit breaker CLOSED - API recovered")
            self.failure_count = 0


class OptimizedMParticleClient:
    """High-performance mParticle API client with connection pooling and batching"""
    
    def __init__(self, api_key: str, api_secret: str, data_center: str = "us", 
                 enable_batching: bool = True, batch_size: int = 10):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_url = MPARTICLE_API_ENDPOINTS.get(data_center, MPARTICLE_API_ENDPOINTS["us"])
        self.enable_batching = enable_batching
        self.batch_size = batch_size
        
        # Initialize optimizations
        self.rate_limiter = RateLimiter(max_requests=100, time_window=60)
        self.circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        
        # Setup optimized session with connection pooling
        self.session = self._create_optimized_session()
        
        # Batch management
        self.pending_events = []
        self.batch_lock = threading.Lock()
        
        logger.info(f"Initialized OptimizedMParticleClient with batching={'enabled' if enable_batching else 'disabled'}")
    
    def _create_optimized_session(self) -> requests.Session:
        """Create a session with connection pooling and retry configuration"""
        session = requests.Session()
        session.auth = (self.api_key, self.api_secret)
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        
        # Configure connection pooling
        adapter = HTTPAdapter(
            pool_connections=20,      # Number of connection pools
            pool_maxsize=100,         # Max connections per pool
            max_retries=retry_strategy
        )
        
        session.mount('https://', adapter)
        session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'QSR-Coupon-Integration/2.0'
        })
        
        return session
    
    def send_event(self, event: Dict[str, Any]) -> bool:
        """Send a single event or add to batch"""
        if self.enable_batching:
            return self._add_to_batch(event)
        else:
            return self._send_single_event(event)
    
    def send_events_batch(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """Send multiple events in optimized batches"""
        if not events:
            return {"success": 0, "failed": 0}
        
        results = {"success": 0, "failed": 0}
        
        # Process in batches
        for i in range(0, len(events), self.batch_size):
            batch = events[i:i + self.batch_size]
            
            if len(batch) == 1:
                # Single event
                if self._send_single_event(batch[0]):
                    results["success"] += 1
                else:
                    results["failed"] += 1
            else:
                # Batch multiple events
                if self._send_batch_events(batch):
                    results["success"] += len(batch)
                else:
                    results["failed"] += len(batch)
        
        return results
    
    def _add_to_batch(self, event: Dict[str, Any]) -> bool:
        """Add event to pending batch"""
        with self.batch_lock:
            self.pending_events.append(event)
            
            # Auto-flush when batch is full
            if len(self.pending_events) >= self.batch_size:
                return self._flush_batch()
        
        return True  # Event queued successfully
    
    def flush_pending_events(self) -> bool:
        """Flush any remaining events in the batch"""
        with self.batch_lock:
            if self.pending_events:
                return self._flush_batch()
        return True
    
    def _flush_batch(self) -> bool:
        """Send pending events as a batch"""
        if not self.pending_events:
            return True
        
        events_to_send = self.pending_events.copy()
        self.pending_events.clear()
        
        logger.debug(f"Flushing batch of {len(events_to_send)} events")
        
        if len(events_to_send) == 1:
            return self._send_single_event(events_to_send[0])
        else:
            return self._send_batch_events(events_to_send)
    
    def _send_single_event(self, event: Dict[str, Any]) -> bool:
        """Send a single event with all optimizations"""
        try:
            return self.circuit_breaker.call(self._make_api_request, event)
        except Exception as e:
            logger.error(f"Failed to send single event: {e}")
            return False
    
    def _send_batch_events(self, events: List[Dict[str, Any]]) -> bool:
        """Send multiple events in a single API call"""
        try:
            # Combine events into a single payload
            if not events:
                return True
            
            # Use the first event as template and combine all events
            base_event = events[0]
            combined_payload = {
                "schema_version": base_event.get("schema_version", 2),
                "environment": base_event.get("environment", "development"),
                "events": [],
                "device_info": base_event.get("device_info", {"platform": "web"})
            }
            
            # Combine all event data
            for event in events:
                if "events" in event and event["events"]:
                    combined_payload["events"].extend(event["events"])
            
            # Use user identities from first event (assuming batch is for same user)
            if "user_identities" in base_event:
                combined_payload["user_identities"] = base_event["user_identities"]
            
            return self.circuit_breaker.call(self._make_api_request, combined_payload)
            
        except Exception as e:
            logger.error(f"Failed to send batch events: {e}")
            return False
    
    def _make_api_request(self, payload: Dict[str, Any]) -> bool:
        """Make the actual API request with rate limiting"""
        # Apply rate limiting
        self.rate_limiter.acquire()
        
        try:
            response = self.session.post(
                self.api_url,
                json=payload,
                timeout=30
            )
            
            if 200 <= response.status_code < 300:
                logger.debug(f"Successfully sent to mParticle (HTTP {response.status_code})")
                return True
            else:
                logger.error(f"API error: HTTP {response.status_code}, {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception: {e}")
            raise
    
    def close(self):
        """Clean up resources"""
        # Flush any pending events
        self.flush_pending_events()
        
        # Close session
        if hasattr(self, 'session'):
            self.session.close()


# Backward compatibility functions
def send_event_to_mparticle(
    event: Dict[str, Any], 
    api_key: str, 
    api_secret: str, 
    max_retries: int = 5,
    data_center: str = "us"
) -> bool:
    """
    Legacy function for backward compatibility.
    Creates a temporary client for single event sending.
    """
    client = OptimizedMParticleClient(
        api_key=api_key,
        api_secret=api_secret,
        data_center=data_center,
        enable_batching=False
    )
    
    try:
        return client.send_event(event)
    finally:
        client.close()


def send_events_batch_to_mparticle(
    events: List[Dict[str, Any]], 
    api_key: str, 
    api_secret: str, 
    data_center: str = "us",
    batch_size: int = 10
) -> Dict[str, int]:
    """
    Optimized batch sending function.
    """
    client = OptimizedMParticleClient(
        api_key=api_key,
        api_secret=api_secret,
        data_center=data_center,
        enable_batching=True,
        batch_size=batch_size
    )
    
    try:
        return client.send_events_batch(events)
    finally:
        client.close()
