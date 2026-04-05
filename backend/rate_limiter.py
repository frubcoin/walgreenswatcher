"""Rate limiter to prevent Walgreens from blocking requests"""
import time
import random
from typing import Callable, Any
from functools import wraps
from config import RATE_LIMIT_DELAY, MAX_RETRIES, RETRY_BACKOFF

class RateLimiter:
    def __init__(self, delay: float = RATE_LIMIT_DELAY):
        self.delay = delay
        self.last_request_time = 0
    
    def wait(self) -> None:
        """Wait appropriate time before next request with jitter"""
        elapsed = time.time() - self.last_request_time
        wait_time = max(0, self.delay - elapsed)
        # Add jitter (±20%) to avoid predictable patterns
        jitter = wait_time * random.uniform(0.8, 1.2)
        if jitter > 0:
            time.sleep(jitter)
        self.last_request_time = time.time()
    
    def reset(self) -> None:
        """Reset the rate limiter"""
        self.last_request_time = 0

# Global rate limiter instance
_global_limiter = RateLimiter()

def rate_limited(func: Callable) -> Callable:
    """Decorator to rate limit function calls"""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        for attempt in range(MAX_RETRIES):
            try:
                _global_limiter.wait()
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    backoff_time = RETRY_BACKOFF ** attempt
                    print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {backoff_time:.1f}s...")
                    time.sleep(backoff_time)
                else:
                    raise
        return None
    return wrapper

def get_rate_limiter() -> RateLimiter:
    """Get the global rate limiter instance"""
    return _global_limiter
