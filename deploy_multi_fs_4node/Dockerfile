FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv using pip
RUN pip install uv

WORKDIR /app

# Copy the entire project to build from source
COPY . .

# Install Fustor components from source
RUN uv pip install --system \
    ./core \
    ./agent-sdk \
    ./management-ui ./fusion-sdk \
    ./management-ui ./fusion \
    ./agent \
    ./benchmark \
    ./extensions/view-fs \
    ./extensions/view-multi-fs \
    ./extensions/source-fs \
    ./extensions/sender-http \
    ./extensions/receiver-http

# Create necessary directories
RUN mkdir -p /root/.fustor/fusion-config \
    && mkdir -p /root/.fustor/agent-config \
    && mkdir -p /root/.fustor/logs

# Default command
CMD ["bash"]
