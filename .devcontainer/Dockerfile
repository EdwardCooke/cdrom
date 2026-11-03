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
RUN ln -sf /home/vscode/.vscode-server /workspace/cdrom/.local/vscode-server

# Set up the user
# The base image already has a 'vscode' user
