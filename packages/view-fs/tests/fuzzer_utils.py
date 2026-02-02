import logging
import random
import time
from typing import List, Dict, Any, Optional
from fustor_core.event import MessageSource, EventType

logger = logging.getLogger(__name__)

class EventFuzzer:
    """
    Simple random event generator for property-based testing.
    Replaces hypothesis for environments where it's not installed.
    """
    
    def __init__(self, seed: Optional[int] = None):
        self.seed = seed or int(time.time())
        self.rng = random.Random(self.seed)
        self.paths = [f"/data/file_{i}.txt" for i in range(5)] # Small set to force collisions
        self.current_time = 1000.0

    def generate_events(self, count: int = 50, max_rows_per_event: int = 5) -> List[Dict[str, Any]]:
        events = []
        events_generated = 0
        
        while events_generated < count:
            self.current_time += self.rng.uniform(0.1, 10.0)
            
            # 80% chance of Realtime, 10% Audit, 10% Snapshot
            source_roll = self.rng.random()
            if source_roll < 0.8:
                source = MessageSource.REALTIME
            elif source_roll < 0.9:
                source = MessageSource.AUDIT
            else:
                source = MessageSource.SNAPSHOT
            
            # Number of rows in this event
            num_rows = self.rng.randint(1, max_rows_per_event)
            rows = []
            
            # Event Type (shared for the whole event for simplicity, though drivers might mix them)
            type_roll = self.rng.random()
            if type_roll < 0.7:
                evt_type = EventType.UPDATE
            else:
                evt_type = EventType.DELETE

            for _ in range(num_rows):
                path = self.rng.choice(self.paths)
                
                # Mtime logic: Realtime usually newer, others might be stale
                is_stale = (source != MessageSource.REALTIME) and (self.rng.random() < 0.3)
                
                if is_stale:
                    mtime = self.current_time - self.rng.uniform(50.0, 200.0)
                else:
                    mtime = self.current_time
                
                rows.append({
                    "path": path,
                    "modified_time": mtime,
                    "size": self.rng.randint(0, 10000)
                })
            
            # Event logical time (index)
            # If stale, we simulate "Late Arrival" by backdating the index to match mtime
            # This ensures that "Stale Content" appears as "Old Event" to the Arbitrator's Tombstone logic.
            if is_stale:
                event_time = rows[0]["modified_time"] + 0.1
            else:
                event_time = self.current_time + self.rng.uniform(0.0, 0.5)
            
            event = {
                "event_type": evt_type,
                "rows": rows,
                "message_source": source,
                "index": int(event_time * 1000) # Agent logical time in ms
            }
            events.append(event)
            events_generated += 1
            
        return events

    def log_seed(self):
        logger.info(f"Fuzzer Seed: {self.seed}")
        # Keep print for direct terminal feedback during debugging
        print(f"Fuzzer Seed: {self.seed}")
