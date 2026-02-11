"""
Configuration Validation Utility
"""
import logging
from typing import List, Dict, Any, Tuple
from fustor_agent.config.unified import AgentConfigLoader, agent_config


logger = logging.getLogger("fustor_agent.validator")

class ConfigValidator:
    """
    Validates agent configuration for consistency and completeness.
    """
    
    def __init__(self, loader: AgentConfigLoader = agent_config):
        self.loader = loader

    def validate(self) -> Tuple[bool, List[str]]:
        """
        Run all validation checks.
        
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        
        try:
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

        # 3. Validate Pipes (Cross-references)
        pipes = self.loader.get_all_pipes()
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

        if not sources and not senders and not pipes:
             # Just a warning context, maybe not an error if intentional, 
             # but usually means empty config dir
             pass

        return len(errors) == 0, errors
