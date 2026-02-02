"""
Event Mapper utility for Pipeline.

Handles transformation of event data based on configuration mapping rules.
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class EventMapper:
    """
    Handles mapping of source event fields to target structure.
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

        self._compiled_mapper = self._compile_mapper_function()
        self.has_mappings = bool(self.config)

    @property
    def mappings(self) -> List[Dict[str, Any]]:
        return self.config

    def process(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply mapping to single event data dict."""
        if not self.has_mappings:
            return event_data
        return self._compiled_mapper(event_data, logger)

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
                # Assuming event.rows is a list we can assign to.
                event.rows = new_rows
            
            # If the event itself is a dict (raw), map it directly?
            # Usually events in pipeline are EventBase. 
            # If flat dicts are supported:
            elif isinstance(event, dict):
                 # Fallback for raw dict events without rows?
                 # Assuming V2 pipeline uses EventBase mainly.
                 pass
            
            mapped_batch.append(event)
            
        return mapped_batch

    def _compile_mapper_function(self):
        """
        Compile a fast mapper function from configuration.
        Returns a callable that takes (event_data, logger).
        """
        if not self.config:
            def passthrough(event_data, logger):
                return event_data
            return passthrough

        type_converter_map = {
            "string": "str",
            "integer": "int",
            "number": "float",
            "boolean": "bool",
        }

        code_lines = [
            "def fast_mapper(event_data, logger):",
            "    processed_data = {}",
        ]

        # Pre-create top-level buckets for dot notation
        endpoint_names = {m.get("to", "").split('.', 1)[0] for m in self.config if '.' in m.get("to", "")}
        for name in endpoint_names:
            code_lines.append(f"    processed_data['{name}'] = {{}}")

        for mapping in self.config:
            target_path = mapping.get("to", "")
            source_list = mapping.get("source", [])
            
            if not target_path:
                continue

            if '.' in target_path:
                endpoint_name, target_field_name = target_path.split('.', 1)
                target_access = f"processed_data['{endpoint_name}']['{target_field_name}']"
            else:
                target_access = f"processed_data['{target_path}']"

            if not source_list and not mapping.get("hardcoded_value"):
                 continue

            if not source_list:
                 continue

            source_def = source_list[0]
            if ':' in source_def:
                source_field, target_type = source_def.split(':', 1)
            else:
                source_field, target_type = source_def, None

            code_lines.append(f"    val = event_data.get('{source_field}')")
            code_lines.append(f"    if val is not None:")
            
            if target_type and target_type in type_converter_map:
                converter = type_converter_map[target_type]
                code_lines.append(f"        try:")
                code_lines.append(f"            {target_access} = {converter}(val)")
                code_lines.append(f"        except (ValueError, TypeError):")
                code_lines.append(f"            logger.warning(f'Failed to convert {{val}} to {target_type} for {target_path}')")
            else:
                code_lines.append(f"        {target_access} = val")

        code_lines.append("    return processed_data")
        
        function_code = "\n".join(code_lines)
        
        local_namespace = {}
        try:
            exec(function_code, globals(), local_namespace)
            return local_namespace['fast_mapper']
        except Exception as e:
            logger.error(f"Failed to compile mapper: {e}", exc_info=True)
            def identity(data, log): return data
            return identity
