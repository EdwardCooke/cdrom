FROM mcr.microsoft.com/devcontainers/go:1.27

RUN curl -1sLf 'https://dl.cloudsmith.io/public/gitguardian/ggshield/setup.deb.sh' | bash
RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    gnupg \
    lsb-release \
    ggshield \
    vim \
    protobuf-compiler

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Set up the user
# The base image already has a 'vscode' user
