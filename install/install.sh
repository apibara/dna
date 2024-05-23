#!/usr/bin/env bash
# shellcheck shell=dash

set -euo pipefail


DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}"
APIBARA_ROOT_DIR="${APIBARA_ROOT_DIR:-$DATA_DIR/apibara}"
APIBARA_REPO="${APIBARA_REPO:-apibara/dna}"

main() {
    say "installing Apibara CLI to ${APIBARA_ROOT_DIR}"
    need_cmd curl
    need_cmd jq
    need_cmd gzip

    get_arch || exit 1

    local _arch="$RETVAL"
    assert_nz "$_arch" "arch"

    local _release_tag
    _release_tag=$(
        curl -s "https://api.github.com/repos/${APIBARA_REPO}/releases?per_page=100" \
        | jq -r '.[] | select((.prerelease==false) and (.tag_name | startswith("cli"))) | .tag_name' \
        | head -n 1
    )
    assert_nz "$_release_tag" "release tag"

    local _release_version="${_release_tag#cli/v}"
    assert_nz "$_release_version" "release version"
    say "installing CLI version $_release_version for $_arch"

    local _release_url="https://github.com/apibara/dna/releases/download/$_release_tag/cli-$_arch.gz"
    local _bin_dir="${APIBARA_ROOT_DIR}/bin"
    mkdir -p "$_bin_dir"
    ensure curl -Ls "$_release_url" > "$_bin_dir/apibara.gz"
    ensure gzip -f -d "$_bin_dir/apibara.gz"
    ensure chmod +x "$_bin_dir/apibara"

    say "checking installation"
    ensure "$_bin_dir/apibara" --version

    say "adding installation to PATH"
    local _profile _shell
    case "$SHELL" in
        */bash)
            _profile="$HOME/.bashrc"
            _shell="bash"
            ;;
        */zsh)
            _profile="${ZDOTDIR:-$HOME}/.zshenv"
            _shell="zsh"
            ;;

        */fish)
            _profile="$HOME/.config/fish/config.fish"
            _shell="fish"
            ;;
        *)
            err "could not detect shell. Add '$_bin_dir' to your PATH"
    esac

    say "detected your shell as $_shell."

    # Add only if not already in PATH
    # shellcheck disable=SC2035
    if test ":$PATH:" != *":$_bin_dir:"*; then
        ensure echo "# Added by the Apibara installer" >> "$_profile"
        ensure echo "export PATH=\"\$PATH:$_bin_dir\"" >> "$_profile"
    fi

    say "added the installation to your PATH. Run 'source $_profile' or start a new terminal to use apibara"
    say ""
    say "Documentation: https://www.apibara.com/docs"
    say "GitHub: https://github.com/apibara"
    say "Twitter: https://www.twitter.com/apibara_web3"
    say "Discord: https://discord.gg/m7B92CNFNt"

}

get_arch() {
    local _ostype _cputype _arch
    _ostype="$(uname -s)"
    _cputype="$(uname -m)"

    case "$_ostype" in
        Linux)
            _ostype=linux
            ;;
        Darwin)
            _ostype=macos
            ;;
        *)
            err "unrecognized OS type: $_ostype"
            ;;
    esac

    case "$_cputype" in
        aarch64 | arm64)
            _cputype=aarch64
            ;;
        x86_64 | x86-64 | x64 | amd64)
            _cputype=x86_64
            ;;
        *)
            err "unsupported CPU type: $_cputype"
            ;;
    esac

    _arch="${_cputype}-${_ostype}"

    RETVAL="$_arch"
}

say() {
    printf 'apibara-installer: %s\n' "$1"
    need_cmd uname
}

err() {
    say "$1" >&2
    exit 1
}

need_cmd() {
    if ! check_cmd "$1"; then
        err "command '$1' is required but not available"
    fi
}

check_cmd() {
    command -v "$1" > /dev/null 2>&1
}

assert_nz() {
    if [ -z "$1" ]; then
        err "assert_nz failed: $2"
    fi
}

ensure() {
    if ! "$@"; then
        err "command failed: $*"
    fi
}

main "$@" || exit 1
