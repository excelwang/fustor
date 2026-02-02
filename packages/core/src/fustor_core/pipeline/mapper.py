# packages/core/src/fustor_core/pipeline/mapper.py
"""
Field Mapper for Fustor Pipelines.

Handles renaming and transforming fields in event rows according to 
the fields_mapping configuration.
"""
import logging
from typing import Any, Dict, List, Optional, Callable, Set

from fustor_core.models.config import FieldMapping
from fustor_core.event import EventBase

logger = logging.getLogger("fustor_core.pipeline.mapper")

class EventMapper:
    """
    Handles mapping of fields in event rows.
    
    Supports:
    - Field renaming (e.g., file_path -> path)
    - Type conversion (string, integer, number)
    - Nested object creation
    """
    
    def __init__(self, fields_mapping: List[FieldMapping]):
        self.mappings = fields_mapping
        self._mapper_fn: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
        
        if self.mappings:
            self._compile_mapper()
            
    def _compile_mapper(self):
        """
        Build an efficient mapping function from the configuration.
        """
        # Group mappings by endpoint name (e.g., 'files.path' -> endpoint 'files')
        endpoints: Dict[str, Dict[str, Any]] = {}
        
        type_converters = {
            "string": str,
            "integer": int,
            "number": float,
            "float": float,
        }
        
        # We pre-process the mappings to speed up execution
        compiled_rules = []
        
        for mapping in self.mappings:
            if '.' not in mapping.to:
                continue
                
            endpoint_name, target_field_name = mapping.to.split('.', 1)
            
            # Identify source field and type
            if not mapping.source:
                continue
                
            source_parts = mapping.source[0].split(':')
            source_field_name = source_parts[0].split('.')[-1]
            source_type = source_parts[1] if len(source_parts) > 1 else None
            
            converter = type_converters.get(source_type) if source_type else None
            
            compiled_rules.append({
                "endpoint": endpoint_name,
                "target": target_field_name,
                "source": source_field_name,
                "converter": converter,
                "source_type": source_type
            })
            
        def mapper_fn(row: Dict[str, Any]) -> Dict[str, Any]:
            processed_data: Dict[str, Dict[str, Any]] = {}
            
            for rule in compiled_rules:
                endpoint = rule["endpoint"]
                if endpoint not in processed_data:
                    processed_data[endpoint] = {}
                    
                val = row.get(rule["source"])
                if val is not None:
                    if rule["converter"]:
                        try:
                            processed_data[endpoint][rule["target"]] = rule["converter"](val)
                        except (ValueError, TypeError):
                            logger.warning(
                                f"Failed to convert value {val} to {rule['source_type']} "
                                f"for field {rule['target']}"
                            )
                    else:
                        processed_data[endpoint][rule["target"]] = val
            
            # If the output only has one endpoint (most common), flatten it?
            # Actually, Fustor expects { "endpoint_name": { ... } } or just { ... }?
            # The Master branch returned processed_data which was { "files": { ... } }
            # But the 'rows' in FSInsertEvent expect FSRow objects or dicts matching them.
            
            # TODO: Verify if we should return the nested dict or flatten it.
            # In Master, the pusher expected nested dicts if the schema had dots.
            return processed_data

        self._mapper_fn = mapper_fn

    def map_event(self, event: EventBase) -> EventBase:
        """
        Apply mapping to all rows in an event.
        
        Args:
            event: The event to transform
            
        Returns:
            The event with transformed rows (modifies in place or returns new one)
        """
        if not self._mapper_fn or not hasattr(event, "rows"):
            return event
            
        new_rows = []
        for row in event.rows:
            # Handle row being a Pydantic model or a dict
            row_dict = row.model_dump() if hasattr(row, "model_dump") else (row.dict() if hasattr(row, "dict") else dict(row))
            
            mapped_result = self._mapper_fn(row_dict)
            
            # If mapping produced nested data like {"files": {...}}, we might need 
            # to pick the right endpoint or flatten it.
            # For FS schema, we usually map to 'files.xxx'
            if "files" in mapped_result:
                 # FS specific flattening for now to match FSInsertEvent expectations
                 # but this should be more generic.
                 new_rows.append(mapped_result["files"])
            else:
                 # Fallback: if multiple endpoints or other name, keep as is
                 new_rows.append(mapped_result)
                 
        # Update event rows
        if self.mappings:
            logger.info(f"DEBUG_MAPPER: {event.event_type} {event.table} rows={len(new_rows)}")
            if len(event.rows) > 0:
                logger.info(f"DEBUG_RAW_FIRST_ROW: {event.rows[0]}")
            if new_rows: 
                logger.info(f"DEBUG_MAP_ROW_FIRST: {new_rows[0]}")
        event.rows = new_rows
        return event

    def map_batch(self, batch: List[Any]) -> List[Any]:
        """
        Apply mapping to a list of events.
        """
        if not self._mapper_fn:
            return batch
            
        return [self.map_event(e) if isinstance(e, EventBase) else e for e in batch]
