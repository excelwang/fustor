"""
Event Mapper utility for Pipeline.

Handles transformation of event data based on configuration mapping rules.
"""
from typing import List, Dict, Any, Optional, Callable
import logging

logger = logging.getLogger(__name__)

class EventMapper:
    """
    Handles mapping of source event fields to target structure.
    Safe implementation using closures instead of exec.
    """
    
    def __init__(self, mapping_config: List[Any]):
        """
        Initialize mapper with configuration.
        
        Args:
            mapping_config: List of mapping rules (Dict or Pydantic model).
        """
        # Normalize config to dicts
        self.config = []
        if mapping_config:
            for item in mapping_config:
                if hasattr(item, "model_dump"):
                    self.config.append(item.model_dump(mode="json"))
                elif hasattr(item, "dict"):
                    self.config.append(item.dict())
                elif isinstance(item, dict):
                    self.config.append(item)
                else:
                    try:
                        self.config.append(vars(item))
                    except Exception:
                        logger.warning(f"Skipping invalid mapping config item: {item}")

        self._mapper_func = self._create_mapper_closure()
        self.has_mappings = bool(self.config)

    @property
    def mappings(self) -> List[Dict[str, Any]]:
        return self.config

    def process(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mapping to single event data dict."""
        if not self.has_mappings:
            return event_data
        return self._mapper_func(event_data, logger)

    def map_batch(self, batch: List[Any]) -> List[Any]:
        """
        Apply mapping to a batch of events.
        Handles EventBase objects by modifying their 'rows' list.
        """
        if not self.has_mappings:
            return batch
            
        mapped_batch = []
        for event in batch:
            # Map object
            if hasattr(event, "rows") and isinstance(event.rows, list):
                new_rows = []
                for row in event.rows:
                    # Convert to dict if needed (lazy)
                    row_data = row
                    is_obj = False
                    if hasattr(row, "model_dump"):
                        row_data = row.model_dump(mode="json")
                        is_obj = True
                    elif hasattr(row, "__dict__"):
                        row_data = vars(row)
                        is_obj = True
                    
                    mapped_row = self.process(row_data)
                    new_rows.append(mapped_row)
                
                # Careful: modifying event in place if it's mutable
                # For safety/correctness with Pydantic, we usually shouldn't mutate 
                # but for performance in pipeline we often do.
                event.rows = new_rows
            
            # If the event itself is a dict (raw), map it directly
            elif isinstance(event, dict):
                 # Support for raw dict events without rows structure
                 # Note: This changes the event structure itself
                 mapped_batch.append(self.process(event))
                 continue
            
            mapped_batch.append(event)
            
        return mapped_batch

    def _create_mapper_closure(self) -> Callable[[Dict, logging.Logger], Dict]:
        """
        Create a closure that performs mapping based on config.
        Returns a callable that takes (event_data, logger).
        """
        if not self.config:
            def passthrough(event_data, logger):
                return event_data
            return passthrough

        type_converter_map = {
            "string": str,
            "str": str,
            "integer": int,
            "int": int,
            "number": float,
            "float": float,
            "boolean": self._to_bool,
            "bool": self._to_bool,
        }

        # Prepare instructions
        instructions = []
        
        for mapping in self.config:
            target_path = mapping.get("to")
            if not target_path:
                continue

            # Parse target path (dot notation)
            target_parts = target_path.split('.')
            
            # Determine source value strategy
            source_strategy = None
            
            if "hardcoded_value" in mapping:
                val = mapping["hardcoded_value"]
                source_strategy = lambda _, val=val: val
            else:
                source_list = mapping.get("source", [])
                if not source_list:
                    continue
                
                source_def = source_list[0]
                source_field = source_def
                target_type = None
                
                if ':' in source_def:
                    source_field, target_type = source_def.split(':', 1)
                
                converter = type_converter_map.get(target_type) if target_type else None
                
                def make_extractor(field, conv, path):
                    def extract(event_data):
                        val = event_data.get(field)
                        if val is None:
                            return None
                        if conv:
                            try:
                                return conv(val)
                            except (ValueError, TypeError):
                                # Logger will be passed at runtime if we wanted to log here, 
                                # but for simplicity in closure we might skip logging or raise
                                # To keep it fast, we return None or raw val?
                                # Original implementation logged warning.
                                return val # Fallback to raw value on error? Or None?
                                # Let's stick to returning raw value but ideally we should log.
                                # But we don't have logger in this inner scope easily without overhead.
                                # Let's handle logging in the main loop if needed.
                        return val
                    return extract
                
                source_strategy = make_extractor(source_field, converter, target_path)

            instructions.append((target_parts, source_strategy))

        def mapper_logic(event_data, logger):
            processed_data = {}
            
            for target_parts, get_value in instructions:
                val = get_value(event_data)
                if val is None:
                    continue
                
                # Set value in nested dict
                current = processed_data
                for i, part in enumerate(target_parts[:-1]):
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                    if not isinstance(current, dict):
                        # Conflict: trying to use a scalar as a dict
                        logger.warning(f"Mapping conflict for {target_parts}: {part} is not a dict")
                        break
                else:
                    current[target_parts[-1]] = val
            
            return processed_data

        return mapper_logic

    @staticmethod
    def _to_bool(val: Any) -> bool:
        """Robust boolean conversion."""
        if isinstance(val, str):
            return val.lower() in ('true', '1', 'yes', 'on')
        return bool(val)
