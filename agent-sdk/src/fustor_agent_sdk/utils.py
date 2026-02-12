import os
import uuid
import logging
import socket

def get_or_generate_agent_id(config_dir: str, logger: logging.Logger) -> str:
    """
    Loads an agent ID.
    Priority:
    1. Existing agent.id file
    2. Generate new ID (IP-based)
    """
    agent_id_path = os.path.join(config_dir, 'agent.id')
    
    # 1. Check File
    try:
        with open(agent_id_path, 'r', encoding='utf-8') as f:
            agent_id = f.read().strip()
        if not agent_id:
                raise FileNotFoundError # Treat empty file as not found
        logger.info(f"Loaded existing Agent ID: {agent_id}")
        return agent_id
    except FileNotFoundError:
        # 3. Auto-generate IP-based ID
        # User requested IP-based ID
        ip = "127.0.0.1"
        try:
            # UDP socket doesn't establish connection, just checking routing table
            # to determine which interface would be used for public/LAN traffic.
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                # Use a non-routable IP in standard private ranges or public DNS
                # 10.255.255.255 is a good broadcast target that forces interface selection
                s.connect(('10.255.255.255', 1))
                ip = s.getsockname()[0]
            finally:
                s.close()
        except Exception:
            pass

        # Sanitize IP (replace dots with dashes) to be ID-safe
        ip_sanitized = ip.replace('.', '-')
        
        short_uuid = str(uuid.uuid4())[:8]
        agent_id = f"{ip_sanitized}-{short_uuid}"
        logger.info(f"No existing Agent ID found. Generated new ID: {agent_id}")
        try:
            with open(agent_id_path, 'w', encoding='utf-8') as f:
                f.write(agent_id)
        except IOError as e:
            logger.error(f"FATAL: Could not write Agent ID to file {agent_id_path}: {e}")
            raise
        return agent_id
