#!/bin/bash

PROJECT_DIR=${PROJECT_DIR:-/workspace}

if [ -e $HOME/.nix-profile/etc/profile.d/nix.sh ] ; then
  . $HOME/.nix-profile/etc/profile.d/nix.sh
fi

if [ -e $HOME/.nix-profile ] ; then
  XDG_DATA_DIRS="$XDG_DATA_DIRS:$HOME/.nix-profile/share"
fi
XDG_DATA_DIRS="$XDG_DATA_DIRS:/usr/share"

if shopt -q login_shell; then
  pushd "${PROJECT_DIR}"
  eval "$(nix print-dev-env --profile "${PROJECT_DIR}/.devcontainer/.profile")"
  popd
fi

