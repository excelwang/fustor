# Dockerfile for NFS Client with optional Fustor Agent
FROM python:3.11-slim

WORKDIR /app

# Install NFS client and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    nfs-common \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire monorepo (context is project root)
COPY agent /app/agent
COPY packages /app/packages

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor Agent and dependencies (using correct package names)
RUN pip install --no-cache-dir \
    /app/packages/fustor_common \
    /app/packages/fustor_event_model \
    /app/packages/fustor_agent_sdk \
    /app/packages/fustor_fusion_sdk \
    /app/packages/source_fs \
    /app/packages/pusher_fusion \
    /app/agent

# Create directories
RUN mkdir -p /mnt/shared /data/agent

# Copy entrypoint script
COPY fusion/tests/integration/containers/nfs-client/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
