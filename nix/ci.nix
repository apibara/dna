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

  /* Prepares the image to be uploaded to Buildkite.

    Notice that this script expects the image to be in `./result`.
  */
  ci-prepare-image = pkgs.writeShellApplication {
    name = "ci-prepare-image";
    runtimeInputs = with pkgs; [
      docker
    ];
    text = ''
      function dry_run() {
        if [[ "''${DRY_RUN:-false}" == "true" ]]; then
          echo "[dry-run] $*"
        else
          "$@"
        fi
      }

      name=$1
      arch=$2

      echo "--- Loading docker image"
      docker image load -i ./result

      tagged="''${name}:latest-''${arch}"
      echo "--- Tagging image ''${tagged}"
      docker image tag "apibara-''${name}:latest" "''${tagged}"

      filename="''${name}-''${arch}-image.tar.gz"
      echo "--- Saving image to ''${filename}"
      docker image save -o "''${filename}" "''${tagged}"
    '';
  };

  /* Publish a docker image to the registry, with support for multiple architectures.

    If the `BUILDKITE_TAG` env variable is not set, the script will do nothing.

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

      The image will be tagged as:
       - quay.io/apibara/operator:1.2.3-x86_64
       - quay.io/apibara/operator:1.2.3-aarch64
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

      name=$1
      shift
      archs=( "$@" )

      if [ ''${#archs[@]} -eq 0 ]; then
        echo "No architectures specified"
        exit 1
      fi

      if [ -z "''${BUILDKITE_TAG:-}" ]; then
        echo "No tag specified"
        exit 0
      fi

      if [[ "''${BUILDKITE_TAG:-}" != ''${name}/v* ]]; then
        echo "Tag is for different image"
        exit 0
      fi

      version=''${BUILDKITE_TAG#"''${name}/v"}
      if [[ $(semver validate "''${version}") != "valid" ]]; then
        echo "Invalid version ''${version}"
        exit 1
      fi

      base="quay.io/apibara/''${name}"
      image="''${base}:''${version}"

      # First, load the images and push them to the registry
      images=()
      for arch in "''${archs[@]}"; do
        echo "--- Loading docker image ''${arch}"
        filename="''${name}-''${arch}-image.tar.gz"
        docker image load -i "''${filename}"

        sourceImage="''${name}:latest-''${arch}"
        destImage="''${image}-''${arch}"
        echo "--- Pushing image ''${sourceImage} to ''${destImage}"
        docker image tag "''${sourceImage}" "''${destImage}"
        dry_run docker push "''${destImage}"
        images+=("''${destImage}")
      done

      # Tag and push image v X.Y.Z
      echo "--- Tagging release ''${base}:''${version}"
      dry_run docker manifest create "''${base}:''${version}" "''${images[@]}"
      echo "--- Pushing release ''${base}:''${version}"
      dry_run docker manifest push --purge "''${base}:''${version}"

      # Tag and push image v X.Y
      tag="$(semver get major "''${version}").$(semver get minor "''${version}")"
      echo "--- Tagging release ''${base}:''${tag}"
      dry_run docker manifest create "''${base}:''${tag}" "''${images[@]}"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run docker manifest push --purge "''${base}:''${tag}"

      # Tag and push image v X
      tag="$(semver get major "''${version}")"
      echo "--- Tagging release ''${base}:''${tag}"
      dry_run docker manifest create "''${base}:''${tag}" "''${images[@]}"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run docker manifest push --purge "''${base}:''${tag}"

      # Tag and push image latest
      tag="latest"
      echo "--- Tagging release ''${base}:''${tag}"
      dry_run docker manifest create "''${base}:''${tag}" "''${images[@]}"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run docker manifest push --purge "''${base}:''${tag}"
    '';
  };

  /* Buildkite agents tags.

    The agents are tagged based on the OS and architecture they run on.
    If a steps doesn't have an `agents` attribute, it will run on the default agent (which could have any architecture).
  */
  agents = {
    x86_64-linux = {
      os = "linux";
      arch = "x86_64";
    };
    aarch64-linux = {
      os = "linux";
      arch = "aarch64";
      queue = "aarch64-linux";
    };
  };

  onAllAgents = f:
    pkgs.lib.mapAttrsToList
      (name: agent: f { inherit name agent; })
      agents;

  /* Buildkite pipelines

    Instantiate a pipeline by running `nix eval --json .#pipeline.<name> | buildkite-agent pipeline upload`.
  */
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
      ] ++ (onAllAgents ({ name, agent }:
        {
          label = ":rust: Build crate ${name}";
          command = "nix build .#all-crates";
          agents = agent;
        })
      ) ++ [
        {
          wait = { };
        }
        {
          label = ":pipeline:";
          command = ''
            if [[ true || "''${BUILDKITE_BRANCH}" = "main" || -n "''${BUILDKITE_TAG}" ]]; then
              nix eval --json .#pipeline.x86_64-linux.release | buildkite-agent pipeline upload --no-interpolation;
            fi
          '';
        }
      ];
    };

    release = {
      env = {
        # Set to "true" to skip pushing images to the registry.
        DRY_RUN = "true";
      };
      steps =
        (onAllAgents ({ name, agent }:
          {
            group = ":rust: Build binaries ${name}";
            steps = [
              {
                label = ":rust: Build binary {{ matrix.binary }}";
                command = "nix build .#{{ matrix.binary }}";
                matrix = {
                  setup = {
                    binary = binaries;
                  };
                };
                agents = agent;
              }
            ];
          })
        ) ++ [
          {
            wait = { };
          }
        ] ++ (onAllAgents ({ name, agent }:
          {
            group = ":rust: Build images ${name}";
            steps = [
              {
                label = ":rust: Build image {{ matrix.binary }}";
                commands = [
                  "nix build .#{{ matrix.binary }}-image"
                  "nix develop .#ci -c ci-prepare-image {{ matrix.binary }} ${agent.arch}"
                  "buildkite-agent artifact upload {{ matrix.binary }}-${agent.arch}-image.tar.gz"
                ];
                matrix = {
                  setup = {
                    binary = binaries;
                  };
                };
                agents = agent;
              }
            ];
          })
        ) ++ (onAllAgents ({ name, agent }:
          {
            group = ":rust: Build linux binaries ${name}";
            steps = [
              {
                label = ":rust: Build binary {{ matrix.binary }}";
                commands = [
                  "nix build .#{{ matrix.binary }}-universal"
                ];
                matrix = {
                  setup = {
                    binary = binaries;
                  };
                };
                agents = agent;
              }
            ];
          })
        ) ++ [
          {
            wait = { };
          }
          {
            group = ":quay: Publish images";
            steps = [
              {
                label = ":quay: Publish image {{ matrix.binary }}";
                commands = (onAllAgents ({ agent, ... }:
                  "buildkite-agent artifact download {{ matrix.binary }}-${agent.arch}-image.tar.gz ."
                )) ++ [
                  (
                    let
                      archs = builtins.concatStringsSep " " (onAllAgents ({ agent, ... }: agent.arch));
                    in
                    "nix develop .#ci -c ci-publish-image {{ matrix.binary }} ${archs}"
                  )
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
      ci-prepare-image
      ci-publish-image
    ];
  };
}
