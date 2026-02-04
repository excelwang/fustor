import os
import uuid
import logging

def get_or_generate_agent_id(config_dir: str, logger: logging.Logger) -> str:
    """
    Loads an existing agent ID from the config directory or generates a new one if not found.
    Persists the generated ID to a file.
    """
    agent_id_path = os.path.join(config_dir, 'agent.id')
    try:
        with open(agent_id_path, 'r', encoding='utf-8') as f:
            agent_id = f.read().strip()
            if not agent_id:
                raise FileNotFoundError # Treat empty file as not found
        logger.info(f"Loaded existing Agent ID: {agent_id}")
        return agent_id
    except FileNotFoundError:
        agent_id = str(uuid.uuid4())
        logger.info(f"No existing Agent ID found. Generated new ID: {agent_id}")
        try:
            with open(agent_id_path, 'w', encoding='utf-8') as f:
                f.write(agent_id)
        except IOError as e:
            logger.error(f"FATAL: Could not write Agent ID to file {agent_id_path}: {e}")
            raise
        return agent_id
