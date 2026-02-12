"""
Event Mapper utility for Pipe.

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
                    # In V2, mapping items are often already dicts
                    self.config.append(item)
                else:
                    try:
                        # Fallback for other objects
                        self.config.append(vars(item))
                    except Exception:
                        logger.warning(f"Skipping invalid mapping config item: {item}")

        self._mapper_func = self._create_mapper_closure()
        self.has_mappings = len(self.config) > 0

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
        Handles EventBase objects by modifying their 'rows' list and updating 'fields'.
        """
        if not self.has_mappings:
            return batch
            
        mapped_batch = []
        for event in batch:
            # Map object with rows
            if hasattr(event, "rows") and isinstance(event.rows, list):
                new_rows = []
                for row in event.rows:
                    # Robust conversion: if it's a Pydantic model, use model_dump
                    # to ensure we have a plain dict that we can mutate/pop fields from.
                    row_data = row
                    if hasattr(row, "model_dump"):
                        row_data = row.model_dump()
                    elif hasattr(row, "dict"):
                        row_data = row.dict()
                    elif not isinstance(row, dict):
                        # Last resort for other objects
                        row_data = vars(row).copy() if hasattr(row, "__dict__") else dict(row)
                    
                    mapped_row = self.process(row_data)
                    new_rows.append(mapped_row)
                
                # Update event with transformed rows
                event.rows = new_rows
                if new_rows:
                    # Update fields list to match remapped row keys
                    event.fields = list(new_rows[0].keys())
            
            # If the event itself is a dict (raw), map it directly
            elif isinstance(event, dict):
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
        # Track all source fields that should be removed from original data
        all_source_to_remove = set()
        # Track all target paths to avoid accidental deletion if source == target
        all_targets = set()
        
        for mapping in self.config:
            target_path = mapping.get("to")
            if not target_path:
                continue
            
            all_targets.add(target_path)

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
                
                # If target name is different from the stripped source field name, we 'move' it
                if source_field != target_path:
                    all_source_to_remove.add(source_field)

                converter = type_converter_map.get(target_type) if target_type else None
                
                def make_extractor(field, conv):
                    def extract(event_data):
                        # Handle both dict and object
                        if hasattr(event_data, "get"):
                            val = event_data.get(field)
                        else:
                            val = getattr(event_data, field, None)
                            
                        if val is None:
                            return None
                        if conv:
                            try:
                                return conv(val)
                            except (ValueError, TypeError):
                                return val 
                        return val
                    return extract
                
                source_strategy = make_extractor(source_field, converter)

            instructions.append((target_parts, source_strategy, target_path))

        # Filter out fields that are BOTH a source-to-remove and a target
        final_to_remove = all_source_to_remove - all_targets


        def mapper_logic(event_data, logger):
            processed_data = {}
            
            for target_parts, get_value, target_path in instructions:
                val = get_value(event_data)
                if val is None:
                    continue
                
                current = processed_data
                skip_mapping = False
                
                # Navigate to parent
                for part in target_parts[:-1]:
                    if part in current:
                        if not isinstance(current[part], dict):
                            # Conflict: Path segment is already a scalar value
                            logger.warning(f"Mapping conflict: cannot traverse '{part}' (value: {current[part]}) to set '{target_path}'")
                            skip_mapping = True
                            break
                    else:
                        current[part] = {}
                    
                    current = current[part]
                
                if skip_mapping:
                    continue
                    
                # Set value
                current[target_parts[-1]] = val
                
            return processed_data

        return mapper_logic

    @staticmethod
    def _to_bool(val: Any) -> bool:
        """Robust boolean conversion."""
        if isinstance(val, str):
            return val.lower() in ('true', '1', 'yes', 'on')
        return bool(val)
