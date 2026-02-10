# it/utils/wait_helpers.py
import time
import logging

logger = logging.getLogger("fustor_test")

def wait_for_condition(predicate, timeout: float, poll_interval: float = 0.5, fail_msg: str = ""):
    """
    Universal polling helper for asynchronous conditions.
    """
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(poll_interval)
    
    msg = fail_msg or f"Condition not met within {timeout}s"
    raise AssertionError(msg)
