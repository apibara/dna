{ pkgs }:
let
  # Parse the GITHUB_REF environment variable and set the output target and version.
  extractVersionFromTag = pkgs.writeScriptBin "extract-version-from-tag" ''
    echo "Ref: $GITHUB_REF"

    if [[ "''${GITHUB_REF:-}" != "refs/tags/"* ]]; then
      echo "Not a tag"
      exit 0
    fi

    tag=''${GITHUB_REF#refs/tags/}
    target=$(echo $tag | cut -d'/' -f1)
    version=$(echo $tag | cut -d'/' -f2)

    if [[ -z "''${target}" ]]; then
      echo "Target is not set"
      exit 1
    fi

    if [[ -z "''${version}" ]]; then
      echo "Version is not set"
      exit 1
    fi

    major=$(${pkgs.semver-tool}/bin/semver get major ''${version})
    minor=$(${pkgs.semver-tool}/bin/semver get minor ''${version})
    patch=$(${pkgs.semver-tool}/bin/semver get patch ''${version})
    prerel=$(${pkgs.semver-tool}/bin/semver get prerel ''${version})

    echo "major=''${major}"
    echo "minor=''${minor}"
    echo "patch=''${patch}"
    echo "prerel=''${prerel}"

    echo "target=''${target}" >> "$GITHUB_OUTPUT"
    echo "major=''${major}" >> "$GITHUB_OUTPUT"
    echo "minor=''${minor}" >> "$GITHUB_OUTPUT"
    echo "patch=''${patch}" >> "$GITHUB_OUTPUT"
    echo "prerel=''${prerel}" >> "$GITHUB_OUTPUT"
  '';

  # Publish docker images to Quay.io
  publishDockerImage = pkgs.writeScriptBin "publish-docker-image" ''
    set -euo pipefail

    function dry_run() {
      if [[ "''${DRY_RUN:-false}" == "true" ]]; then
        echo "[dry-run] $*"
      else
        "$@"
      fi
    }

    if [[ -z "''${IMAGE_NAME:-}" ]]; then
      echo "IMAGE_NAME is not set"
      exit 1
    fi

    if [[ -z "''${IMAGE_VERSION_MAJOR:-}" ]]; then
      echo "IMAGE_VERSION_MAJOR is not set"
      exit 1
    fi

    if [[ -z "''${IMAGE_VERSION_MINOR:-}" ]]; then
      echo "IMAGE_VERSION_MINOR is not set"
      exit 1
    fi

    if [[ -z "''${IMAGE_VERSION_PATCH:-}" ]]; then
      echo "IMAGE_VERSION_PATCH is not set"
      exit 1
    fi

    if ! [ -f "''${IMAGE_ARCHIVE_x86_64}" ]; then
      echo "IMAGE_ARCHIVE_x86_64 image does not exist"
      exit 1
    fi

    if ! [ -f "''${IMAGE_ARCHIVE_aarch64}" ]; then
      echo "IMAGE_ARCHIVE_aarch64 image does not exist"
      exit 1
    fi

    skopeo="${pkgs.skopeo}/bin/skopeo"
    buildah="${pkgs.buildah}/bin/buildah"
    echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json

    echo "::group::Logging in to Quay.io"
    echo "::add-mask::''${QUAY_USERNAME}"
    echo "::add-mask::''${QUAY_PASSWORD}"

    skopeo login -u="''${QUAY_USERNAME}" -p="''${QUAY_PASSWORD}" quay.io
    echo "::endgroup::"

    base="quay.io/apibara/''${IMAGE_NAME}"
    version="''${IMAGE_VERSION_MAJOR}.''${IMAGE_VERSION_MINOR}.''${IMAGE_VERSION_PATCH}"
    if ! [[ -z "''${IMAGE_VERSION_PREREL:-}" ]]; then
      version="''${version}-''${IMAGE_VERSION_PREREL}"
    fi

    echo "::group::Publishing image ''${IMAGE_NAME}:''${version}"
    dry_run skopeo copy "docker-archive:''${IMAGE_ARCHIVE_x86_64}" "docker://''${base}:''${version}-x86_64"
    dry_run skopeo copy "docker-archive:''${IMAGE_ARCHIVE_aarch64}" "docker://''${base}:''${version}-aarch64"
    echo "::endgroup::"

    images=("''${base}:''${version}-x86_64" "''${base}:''${version}-aarch64")

    echo "::group::Create manifest ''${base}:''${version}"
    manifest="''${base}:''${version}"
    dry_run buildah manifest create "''${manifest}" "''${images[@]}"
    echo "::endgroup::"

    if ! [[ -z "''${IMAGE_VERSION_PREREL:-}" ]]; then
      tag="''${IMAGE_VERSION_MAJOR}.''${IMAGE_VERSION_MINOR}.''${IMAGE_VERSION_PATCH}-''${IMAGE_VERSION_PREREL}"
      echo "::group::Push manifest ''${base}:''${tag}"
      dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
      echo "::endgroup::"
    fi

    tag="''${IMAGE_VERSION_MAJOR}.''${IMAGE_VERSION_MINOR}.''${IMAGE_VERSION_PATCH}"
    echo "::group::Push manifest ''${base}:''${tag}"
    dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
    echo "::endgroup::"

    tag="''${IMAGE_VERSION_MAJOR}.''${IMAGE_VERSION_MINOR}"
    echo "::group::Push manifest ''${base}:''${tag}"
    dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
    echo "::endgroup::"

    tag="''${IMAGE_VERSION_MAJOR}"
    echo "::group::Push manifest ''${base}:''${tag}"
    dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
    echo "::endgroup::"

    tag="latest"
    echo "::group::Push manifest ''${base}:''${tag}"
    dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
    echo "::endgroup::"
  '';
in
{
  shell.ci = pkgs.mkShell {
    buildInputs = [
      extractVersionFromTag
      publishDockerImage
    ];
  };
}
