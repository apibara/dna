{ pkgs, tests, ... }:
let
  ci-test = pkgs.writeShellApplication {
    name = "ci-test";
    runtimeInputs = [ tests ];
    text = ''
      echo "--- Running unit tests"
      for testBin in ${tests}/bin/*; do
        echo "Running ''${testBin}"
        ''${testBin}
      done
    '';
  };

  ci-e2e-test = pkgs.writeShellApplication {
    name = "ci-e2e-test";
    runtimeInputs = [ tests ];
    text = ''
      echo "--- Running e2e tests"
      for testBin in ${tests}/bin/*; do
        echo "Running ''${testBin}"
        ''${testBin} --ignored
      done
    '';
  };

  ci-publish-image = pkgs.writeShellApplication {
    name = "ci-publish-image";
    runtimeInputs = with pkgs; [
      docker
      semver-tool
    ];
    text = ''
      function dry_run() {
        if [[ "''${DRY_RUN:-false}" == "true" ]]; then
          echo "[dry-run] $*"
        else
          "$@"
        fi
      }

      filename=$1
      name=$2
      base="quay.io/apibara/''${name}"
      image="''${base}:''${BUILDKITE_COMMIT:-latest}"

      echo "--- Loading docker image from ''${filename}"
      docker image load -i "''${filename}"
      echo "--- Tagging image ''${image}"
      docker image tag "apibara-''${name}:latest" "''${image}"
      echo "--- Pushing image ''${image}"
      dry_run docker push "''${image}"

      if [[ "''${BUILDKITE_TAG:-}" == ''${name}/v* ]]; then
        version=''${BUILDKITE_TAG#"''${name}/v"}
        if [[ $(semver validate "''${version}") != "valid" ]]; then
          echo "Invalid version ''${version}"
          exit 1
        fi

        # Tag and push image v X.Y.Z
        echo "--- Tagging release ''${base}:''${version}"
        docker image tag "apibara-''${name}:latest" "''${base}:''${version}"
        echo "--- Pushing release ''${base}:''${version}"
        dry_run docker push "''${base}:''${version}"

        # Tag and push image v X.Y
        tag="$(semver get major "''${version}").$(semver get minor "''${version}")"
        echo "--- Tagging release ''${base}:''${tag}"
        docker image tag "apibara-''${name}:latest" "''${base}:''${tag}"
        echo "--- Pushing release ''${base}:''${tag}"
        dry_run docker push "''${base}:''${tag}"

        # Tag and push image v X
        tag="$(semver get major "''${version}")"
        echo "--- Tagging release ''${base}:''${tag}"
        docker image tag "apibara-''${name}:latest" "''${base}:''${tag}"
        echo "--- Pushing release ''${base}:''${tag}"
        dry_run docker push "''${base}:''${tag}"
      fi
    '';
  };
in
{
  shell.ci = pkgs.mkShell {
    buildInputs = [
      ci-test
      ci-e2e-test
      ci-publish-image
    ];
  };
}
