FROM mcr.microsoft.com/vscode/devcontainers/base:buster

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen && locale-gen

RUN apt-get update \
    && apt-get install --no-install-recommends -y  locales curl xz-utils vim  ca-certificates && apt-get clean && rm -rf /var/lib/apt/lists/*

# create non-root user and group and add it sudoers
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=${USER_UID}
RUN echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/devcontainer && \
    chmod 0440 /etc/sudoers.d/devcontainer

RUN mkdir -p /etc/nix
COPY nix.conf /etc/nix/nix.conf

ARG NIX_INSTALL_SCRIPT=https://nixos.org/nix/install
# RUN curl -L ${NIX_INSTALL_SCRIPT} | sudo -u ${USERNAME} NIX_INSTALLER_NO_MODIFY_PROFILE=1 sh
RUN curl -L ${NIX_INSTALL_SCRIPT} | sudo -u ${USERNAME} sh -s -- --no-daemon

COPY profile.sh /etc/profile.d/devcontainer.sh

RUN mkdir -p /nix
RUN chown -R vscode:vscode /nix
RUN chmod 755 /nix
ENV USER=vscode

VOLUME /nix

