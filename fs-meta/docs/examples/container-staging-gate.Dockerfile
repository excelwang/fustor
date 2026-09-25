FROM rust:bookworm

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        git \
        iproute2 \
        jq \
        kmod \
        libssl-dev \
        nfs-common \
        nfs-kernel-server \
        pkg-config \
        procps \
        rpcbind \
        sudo \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

RUN printf 'root ALL=(ALL) NOPASSWD:ALL\n' > /etc/sudoers.d/root-nopasswd \
    && chmod 0440 /etc/sudoers.d/root-nopasswd

WORKDIR /work/fustor
