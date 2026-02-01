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

# Install Fustor packages
RUN pip install --no-cache-dir \
    fastapi uvicorn pydantic pyyaml \
    /app/packages/core \
    /app/packages/fusion-sdk \
    /app/packages/schema-fs \
    /app/packages/view-fs \
    /app/fusion


# Create data directory
RUN mkdir -p /data/fusion

# Default command 
CMD ["fustor-fusion", "start"]
