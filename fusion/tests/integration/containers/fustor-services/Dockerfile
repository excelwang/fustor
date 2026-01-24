# Dockerfile for Fustor Registry and Fusion services
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire monorepo (context is project root)
COPY registry /app/registry
COPY fusion /app/fusion
COPY packages /app/packages

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor packages (using correct package names)
RUN pip install --no-cache-dir \
    /app/packages/fustor_common \
    /app/packages/fustor_event_model \
    /app/packages/fustor_registry_client \
    /app/registry \
    /app/fusion

# Create data directories
RUN mkdir -p /data/registry /data/fusion

# Default command (override in docker-compose)
CMD ["fustor-registry"]
