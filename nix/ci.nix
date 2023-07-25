{ pkgs, tests, binaries, ... }:
/* CI/CD related scripts and pipelines.

  This file is used to dynamically generate the Buildkite pipeline (see `pipeline` down below).

  Arguments:

   - `tests`: the tests derivation created by native.nix -> build.nix.
   - `binaries`: a list of binaries in the crate. This is generated from the `crates` definition in flake.nix.
 */
let
  /* Run all unit tests.
  */
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

  /* Run all integration tests.

    This tests take longer to run so they're run separately.
  */
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

  /* Publish a docker image to the registry.

    If the `BUILDKITE_COMMIT` env variable is set, it will be used to tag the image,
    otherwise the `latest` tag is used.

    If the `BUILDKITE_TAG` env variable is set, the image will be versioned. The tag
    is expected to be in the format `$name/vX.Y.Z` where `$name` is the name of the
    image and `X.Y.Z` is a valid semver version.
    The script will then tag the image four times:
    - `quay.io/apibara/$name:$version`
    - `quay.io/apibara/$name:$major.$minor`
    - `quay.io/apibara/$name:$major`
    - `quay.io/apibara/$name:latest`

    Example:
      BUILDKITE_TAG=operator/v1.2.3
      BUILDKITE_COMMIT=ffff

      The image will be tagged as:
       - quay.io/apibara/operator:ffff
       - quay.io/apibara/operator:1.2.3
       - quay.io/apibara/operator:1.2
       - quay.io/apibara/operator:1
       - quay.io/apibara/operator:latest

    Arguments:

     - `filename`: the filename of the docker image to load.
     - `name`: the name of the image to publish.
  */
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

        # Tag and push image latest
        tag="latest"
        echo "--- Tagging release ''${base}:''${tag}"
        docker image tag "apibara-''${name}:latest" "''${base}:''${tag}"
        echo "--- Pushing release ''${base}:''${tag}"
        dry_run docker push "''${base}:''${tag}"
      fi
    '';
  };

  pipeline = {
    default = {
      steps = [
        {
          label = ":nix: Checks";
          command = "nix flake check";
        }
        {
          wait = { };
        }
        {
          label = ":rust: Build tests";
          command = "nix build .#tests";
        }
        {
          wait = { };
        }
        {
          label = ":test_tube: Run tests";
          commands = [
            "nix develop .#ci -c ci-test"
            "nix develop .#ci -c ci-e2e-test"
          ];
        }
        {
          wait = { };
        }
        {
          label = ":rust: Build crate";
          command = "nix build .#all-crates";
        }
        {
          wait = { };
        }
        {
          group = ":rust: Build binaries";
          steps = [
            {
              label = ":rust: Build binary {{ matrix.binary }}";
              command = "nix build .#{{ matrix.binary }}";
              matrix = {
                setup = {
                  binary = binaries;
                };
              };
            }
          ];
        }
        {
          wait = { };
        }
        {
          group = ":rust: Build images";
          steps = [
            {
              label = ":rust: Build image {{ matrix.binary }}";
              commands = [
                "nix build .#{{ matrix.binary }}-image"
                "cp result {{ matrix.binary }}-image-amd64.tar.gz"
                "buildkite-agent artifact upload {{ matrix.binary }}-image-amd64.tar.gz"
              ];
              matrix = {
                setup = {
                  binary = binaries;
                };
              };
            }
          ];
        }
        {
          wait = { };
        }
        {
          label = ":pipeline:";
          "if" = "build.branch == 'main'";
          command = "nix eval --json .#pipeline.x86_64-linux.release | buildkite-agent pipeline upload --no-interpolation";
        }
      ];
    };

    release = {
      env = {
        # Set to "true" to skip pushing images to the registry.
        DRY_RUN = "false";
      };
      steps = [
        {
          group = ":quay: Publish images";
          steps = [
            {
              label = ":quay: Publish image {{ matrix.binary }}";
              commands = [
                "buildkite-agent artifact download {{ matrix.binary }}-image-amd64.tar.gz ."
                "nix develop .#ci -c ci-publish-image {{ matrix.binary }}-image-amd64.tar.gz {{ matrix.binary }}"
              ];
              matrix = {
                setup = {
                  binary = binaries;
                };
              };
              plugins = [
                {
                  "docker-login#v2.1.0" = {
                    username = "apibara+buildkite";
                    password-env = "APIBARA_DOCKER_LOGIN_PASSWORD";
                    server = "quay.io";
                  };
                }
              ];
            }
          ];
        }
      ];
    };
  };
in
{
  inherit pipeline;

  shell.ci = pkgs.mkShell {
    buildInputs = [
      ci-test
      ci-e2e-test
      ci-publish-image
    ];
  };
}
