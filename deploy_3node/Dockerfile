FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv using pip
RUN pip install uv

WORKDIR /app

# Install Fustor components from PyPI
# According to DEPLOYMENT_FS.md, these are the required packages:
RUN uv pip install --system \
    fustor-fusion==0.8.9 \
    fustor-view-fs==0.8.9 \
    fustor-agent==0.8.9 \
    fustor-source-fs==0.8.9 \
    fustor-sender-http==0.8.9 \
    fustor-receiver-http==0.8.9

# Create necessary directories
RUN mkdir -p /root/.fustor/fusion-config \
    && mkdir -p /root/.fustor/agent-config \
    && mkdir -p /root/.fustor/logs

# Default command
CMD ["bash"]
