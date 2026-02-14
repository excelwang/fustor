# Dockerfile for Fustor Fusion service
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    faketime \
    gettext-base \
    && rm -rf /var/lib/apt/lists/*

# Copy monorepo components (context is project root)
COPY fusion /app/fusion
COPY core /app/core
COPY fusion-sdk /app/fusion-sdk
COPY extensions /app/extensions

# Set fake version for setuptools-scm (since .git is not copied)
ENV SETUPTOOLS_SCM_PRETEND_VERSION=0.1.0

# Install Fustor packages (all pip dependencies declared in package pyproject.toml)
RUN pip install --no-cache-dir \
    /app/core \
    /app/fusion-sdk \
    /app/extensions/schema-fs \
    /app/extensions/view-fs \
    /app/extensions/receiver-http \
    /app/extensions/view-fs-forest \
    /app/fusion


# Create data directory
RUN mkdir -p /data/fusion

# Copy and set up entrypoint
COPY tests/e2e/containers/fustor-services/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]

# Default command 
CMD ["fustor-fusion", "start"]
