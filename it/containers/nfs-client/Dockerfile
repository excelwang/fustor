# Dockerfile for NFS Client with optional Fustor Agent
FROM python:3.11-slim

WORKDIR /app

# Install NFS client and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    nfs-common \
    curl \
    procps \
    faketime \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire monorepo (context is project root)
COPY agent /app/agent
COPY packages /app/packages

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor Agent and dependencies (using correct package names)
RUN pip install --no-cache-dir \
    httpx pydantic pyyaml \
    /app/packages/core \
    /app/packages/agent-sdk \
    /app/packages/fusion-sdk \
    /app/packages/schema-fs \
    /app/packages/source-fs \
    /app/packages/pusher-fusion \
    /app/agent



# Create directories
RUN mkdir -p /mnt/shared /data/agent

# Copy entrypoint script
COPY it/containers/nfs-client/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
