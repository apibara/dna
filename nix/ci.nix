{ pkgs, binaries, ... }:
/* CI/CD related scripts and pipelines.

  This file is used to dynamically generate the Buildkite pipeline (see `pipeline` down below).

  Arguments:

   - `tests`: the tests derivation.
   - `binaries`: a list of binaries in the crate. This is generated from the `crates` definition in flake.nix.
 */
let
  /* Prepares the image to be uploaded to Buildkite.

    Notice that this script expects the image to be in `./result`.
  */
  ci-prepare-image = pkgs.writeShellApplication {
    name = "ci-prepare-image";
    runtimeInputs = with pkgs; [
      buildah
      skopeo
    ];
    text = ''
      arch=$1
      name=$2

      filename="''${name}-''${arch}-image.tar.gz"
      echo "--- Copying image to ''${filename}"
      skopeo copy "docker-archive:result" "docker-archive:''${filename}"
    '';
  };


  /* Prepares the binary to be uploaded to Buildkite.

    Notice that this script expects the binary to be in `./result/bin`.
  */
  ci-prepare-binary = pkgs.writeShellApplication {
    name = "ci-prepare-binary";
    runtimeInputs = with pkgs; [ ];
    text = ''
      os=$1
      arch=$2
      name=$3

      filename="''${name}-''${arch}-''${os}.gz"
      echo "--- Compressing binary to ''${filename}"
      gzip -c ./result/bin/* > "''${filename}"
    '';
  };

  /* Publish a container image to the registry, with support for multiple architectures.

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

     - `filename`: the filename of the container image to load.
     - `name`: the name of the image to publish.
  */
  ci-publish-image = pkgs.writeShellApplication {
    name = "ci-publish-image";
    runtimeInputs = with pkgs; [
      skopeo
      buildah
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
        echo "--- Copying image ''${arch}"

        filename="''${name}-''${arch}-image.tar.gz"
        destImage="''${image}-''${arch}"

        echo "Copying ''${filename} to ''${destImage}"
        dry_run skopeo copy "docker-archive:''${filename}" "docker://''${destImage}"

        images+=("''${destImage}")
      done

      echo "--- Tagging release ''${base}:''${version}"
      manifest="''${base}:''${version}"
      dry_run buildah manifest create "''${manifest}" "''${images[@]}"

      # Tag and push image v X.Y.Z
      echo "--- Pushing release ''${base}:''${version}"
      dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${version}"

      # Tag and push image v X.Y
      tag="$(semver get major "''${version}").$(semver get minor "''${version}")"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"

      # Tag and push image v X
      tag="$(semver get major "''${version}")"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"

      # Tag and push image latest
      tag="latest"
      echo "--- Pushing release ''${base}:''${tag}"
      dry_run buildah manifest push --all "''${manifest}" "docker://''${base}:''${tag}"
    '';
  };

  /* Buildkite agents tags.

    The agents are tagged based on the OS and architecture they run on.
    If a steps doesn't have an `agents` attribute, it will run on the default agent (which could have any architecture).
  */
  agents = {
    x86_64-linux = {
      queue = "default";
      os = "linux";
      arch = "x86_64";
    };
    aarch64-linux = {
      queue = "aarch64-linux";
      os = "linux";
      arch = "aarch64";
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
          command = "nix flake check -L";
        }
        {
          wait = { };
        }
        {
          label = ":test_tube: Run unit tests";
          commands = [
            "nix build .#unit-tests -L"
          ];
        }
        {
          label = ":test_tube: Run e2e tests";
          commands = [
            "podman system service --time=0 unix:///var/run/docker.sock &"
            "nix build .#integration-tests -L"
          ];
        }
        {
          wait = { };
        }
      ] ++ (onAllAgents ({ name, agent }:
        {
          label = ":rust: Build crate ${name}";
          command = "nix build .#all-crates";
          agents = {
            queue = agent.queue;
          };
        })
      ) ++ [
        {
          wait = { };
        }
        {
          label = ":pipeline:";
          command = ''
            if [[ "''${BUILDKITE_BRANCH}" = "main" || -n "''${BUILDKITE_TAG}" ]]; then
              nix eval --json .#pipeline.x86_64-linux.release | buildkite-agent pipeline upload --no-interpolation;
            fi
          '';
        }
      ];
    };

    release = {
      env = {
        # Set to "true" to skip pushing images to the registry.
        DRY_RUN = "false";
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
                agents = {
                  queue = agent.queue;
                };
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
                  "nix develop .#ci -c ci-prepare-image ${agent.arch} {{ matrix.binary }}"
                  "buildkite-agent artifact upload {{ matrix.binary }}-${agent.arch}-image.tar.gz"
                ];
                matrix = {
                  setup = {
                    binary = binaries;
                  };
                };
                agents = {
                  queue = agent.queue;
                };
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
                  "nix build .#{{ matrix.binary }}"
                  "nix develop .#ci -c ci-prepare-binary linux ${agent.arch} {{ matrix.binary }}"
                  "buildkite-agent artifact upload {{ matrix.binary }}-${agent.arch}-linux.gz"
                ];
                matrix = {
                  setup = {
                    binary = binaries;
                  };
                };
                agents = {
                  queue = agent.queue;
                };
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
              }
            ];
          }
        ];
    };
  };
in
{
  inherit pipeline;

  shell = {
    ci = pkgs.mkShell {
      buildInputs = [
        ci-prepare-image
        ci-prepare-binary
        ci-publish-image
      ];
    };
  };
}
