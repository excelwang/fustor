# Dockerfile for Fustor Fusion service
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    faketime \
    && rm -rf /var/lib/apt/lists/*

# Copy monorepo components (context is project root)
COPY fusion /app/fusion
COPY packages /app/packages

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor packages (all pip dependencies declared in package pyproject.toml)
RUN pip install --no-cache-dir \
    /app/packages/fustor-core \
    /app/packages/fustor-fusion-sdk \
    /app/packages/fustor-schema-fs \
    /app/packages/fustor-view-fs \
    /app/packages/fustor-receiver-http \
    /app/fusion


# Create data directory
RUN mkdir -p /data/fusion

# Default command 
CMD ["fustor-fusion", "start"]
