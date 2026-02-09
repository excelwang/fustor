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
COPY core /app/core
COPY agent-sdk /app/agent-sdk
COPY fusion-sdk /app/fusion-sdk
COPY extensions /app/extensions

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor Agent and dependencies (all pip deps declared in package pyproject.toml)
RUN pip install --no-cache-dir \
    python-dotenv \
    /app/core \
    /app/agent-sdk \
    /app/fusion-sdk \
    /app/extensions/schema-fs \
    /app/extensions/source-fs \
    /app/extensions/sender-http \
    /app/extensions/sender-echo \
    /app/agent



# Create directories
RUN mkdir -p /mnt/shared /data/agent

# Copy entrypoint script
COPY it/containers/nfs-client/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
