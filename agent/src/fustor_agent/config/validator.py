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
        
        Leverages UnifiedAgentConfig for Pydantic structural validation and 
        ConfigValidator.validate for semantic checks.
        """
        errors = []
        if not config_dict:
            return True, []

        try:
            # 1. Structural Validation via Pydantic
            from fustor_agent.config.unified import UnifiedAgentConfig
            
            # Pydantic handles structural validation. defaults (like agent_id being Optional) are handled here.
            unified_cfg = UnifiedAgentConfig.model_validate(config_dict)
            
            # 2. Build temp loader for semantic check
            temp_loader = AgentConfigLoader(config_dir=None)
            temp_loader.logging = unified_cfg.logging
            temp_loader.fs_scan_workers = unified_cfg.fs_scan_workers
            
            # Convert Dict[str, dict] to expected Dict[str, ConfigModel]
            for src_id, src_data in unified_cfg.sources.items():
                temp_loader._sources[src_id] = SourceConfig(**src_data)
            for sender_id, sender_data in unified_cfg.senders.items():
                temp_loader._senders[sender_id] = SenderConfig(**sender_data)

            temp_loader._pipes = unified_cfg.pipes
            temp_loader._loaded = True
            
            # 3. Semantic Validation (Cross-references, Uniqueness)
            temp_validator = ConfigValidator(loader=temp_loader)
            is_valid, semantic_errors = temp_validator.validate(auto_reload=False)
            
            if not is_valid:
                errors.extend(semantic_errors)
                
        except Exception as e:
            errors.append(f"Validation failed: {e}")
        
        return len(errors) == 0, errors
