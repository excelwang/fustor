"""
Configuration Validation Utility
"""
import logging
from typing import List, Dict, Any, Tuple
from fustor_agent.config.unified import AgentConfigLoader, agent_config
from fustor_core.models.config import SourceConfig, SenderConfig, GlobalLoggingConfig


logger = logging.getLogger("fustor_agent.validator")

class ConfigValidator:
    """
    Validates agent configuration for consistency and completeness.
    """
    
    def __init__(self, loader: AgentConfigLoader = agent_config):
        self.loader = loader

    def validate(self, auto_reload: bool = True) -> Tuple[bool, List[str]]:
        """
        Run all validation checks.
        
        Args:
            auto_reload: Whether to reload config from disk before validation.
                       Set to False when validating in-memory config dicts.
        
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            if auto_reload:
                # Force reload to ensure freshness
                self.loader.reload()
        except Exception as e:
            errors.append(f"Failed to load configuration files: {e}")
            return False, errors

        # 0. Validate Global Settings
        if not getattr(self.loader, "agent_id", None):
            errors.append("Global 'agent_id' is missing in configuration. It is required for Multi-FS identification.")

        # 1. Validate Sources
        sources = self.loader.get_all_sources()
        for s_id, s_cfg in sources.items():
            if not s_cfg.driver:
                errors.append(f"Source '{s_id}' missing 'driver' field")
            if not s_cfg.uri:
                errors.append(f"Source '{s_id}' missing 'uri' field")

        # 2. Validate Senders
        senders = self.loader.get_all_senders()
        for s_id, s_cfg in senders.items():
            if not s_cfg.driver:
                errors.append(f"Sender '{s_id}' missing 'driver' field")
            if not s_cfg.uri:
                errors.append(f"Sender '{s_id}' missing 'uri' field")

        # 3. Validate Pipes (Cross-references and Uniqueness)
        pipes = self.loader.get_all_pipes()
        seen_pairs: Dict[Tuple[str, str], str] = {} # (source, sender) -> pipe_id

        for p_id, p_cfg in pipes.items():
            # Check Source Ref
            if not p_cfg.source:
                errors.append(f"Pipe '{p_id}' missing 'source' reference")
            elif p_cfg.source not in sources:
                errors.append(f"Pipe '{p_id}' references unknown source '{p_cfg.source}'")

            # Check Sender Ref
            if not p_cfg.sender:
                errors.append(f"Pipe '{p_id}' missing 'sender' reference")
            elif p_cfg.sender not in senders:
                errors.append(f"Pipe '{p_id}' references unknown sender '{p_cfg.sender}'")
            
            # Check Uniqueness of (source, sender) pair
            if p_cfg.source and p_cfg.sender:
                pair = (p_cfg.source, p_cfg.sender)
                if pair in seen_pairs:
                    errors.append(
                        f"Redundant configuration: Pipe '{p_id}' uses the same (source, sender) pair "
                        f"as Pipe '{seen_pairs[pair]}'. This is forbidden to prevent data conflicts."
                    )
                else:
                    seen_pairs[pair] = p_id

        if not sources and not senders and not pipes:
             pass

        return len(errors) == 0, errors

    def validate_config(self, config_dict: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a configuration dictionary using the same strict rules as disk loading.
        
        This method simulates loading the config into a temporary loader instance
        to leverage Pydantic model validation and cross-reference checks.
        """
        errors = []
        if not config_dict:
            return True, []

        try:
            # 1. Structural Validation via Pydantic (Generic Adapter)
            temp_loader = AgentConfigLoader(config_dir=None)
            data = config_dict
            
            if "logging" in data:
                 GlobalLoggingConfig.model_validate(data["logging"])
            
            if "fs_scan_workers" in data:
                temp_loader.fs_scan_workers = int(data["fs_scan_workers"])
            
            # Use provided agent_id or a dummy for validation context
            if "agent_id" in data:
                temp_loader.agent_id = str(data["agent_id"]).strip()
            else:
                temp_loader.agent_id = "validation-dummy-id"
            
            # Sources
            for src_id, src_data in data.get("sources", {}).items():
                temp_loader._sources[src_id] = SourceConfig(**src_data)
            
            # Senders
            for sender_id, sender_data in data.get("senders", {}).items():
                temp_loader._senders[sender_id] = SenderConfig(**sender_data)
            
            # Pipes
            from fustor_agent.config.unified import AgentPipeConfig
            pipes_data = data.get("pipes", {})
            if not isinstance(pipes_data, dict):
                errors.append("'pipes' section must be a dictionary")
                return False, errors
                
            for pipe_id, pipe_data in pipes_data.items():
                temp_loader._pipes[pipe_id] = AgentPipeConfig(**pipe_data)
            
            temp_loader._loaded = True
            
            # 2. Semantic Validation (Cross-references, Uniqueness)
            temp_validator = ConfigValidator(loader=temp_loader)
            is_valid, semantic_errors = temp_validator.validate(auto_reload=False)
            
            if not is_valid:
                errors.extend(semantic_errors)
                
        except Exception as e:
            errors.append(f"Validation failed: {e}")
        
        return len(errors) == 0, errors
