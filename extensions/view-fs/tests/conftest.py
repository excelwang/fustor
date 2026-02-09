
import random
import time
from typing import List, Dict, Any, Optional
from fustor_core.event import MessageSource, EventType

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

    def generate_events(self, count: int = 50) -> List[Dict[str, Any]]:
        events = []
        for _ in range(count):
            self.current_time += self.rng.uniform(0.1, 10.0)
            
            # 80% chance of Realtime, 10% Audit, 10% Snapshot
            source_roll = self.rng.random()
            if source_roll < 0.8:
                source = MessageSource.REALTIME
            elif source_roll < 0.9:
                source = MessageSource.AUDIT
            else:
                source = MessageSource.SNAPSHOT
            
            # Event Type
            type_roll = self.rng.random()
            if type_roll < 0.7:
                evt_type = EventType.UPDATE # Covers INSERT/UPDATE
            else:
                evt_type = EventType.DELETE
            
            path = self.rng.choice(self.paths)
            
            # Mtime logic: Realtime usually newer, others might be stale
            # Stale probability: 30% for non-realtime
            is_stale = (source != MessageSource.REALTIME) and (self.rng.random() < 0.3)
            
            if is_stale:
                mtime = self.current_time - self.rng.uniform(50.0, 200.0)
            else:
                mtime = self.current_time
                
            row = {
                "path": path,
                "modified_time": mtime,
                "size": self.rng.randint(0, 10000)
            }
            
            event = {
                "event_type": evt_type,
                "rows": [row],
                "message_source": source,
                "index": int(self.current_time * 1000) # Agent logical time
            }
            events.append(event)
            
        return events

    def log_seed(self):
        print(f"Fuzzer Seed: {self.seed}")
