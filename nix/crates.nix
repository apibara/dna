# Build the provided crates using crane.
{ pkgs, crane, workspaceDir, crates }:
let
  rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml).override {
    extensions = [ "rust-src" "rust-analyzer" ];
  };

  nightlyRustToolchain = pkgs.rust-bin.selectLatestNightlyWith (toolchain: toolchain.default);

  craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

  src = pkgs.lib.cleanSourceWith {
    src = craneLib.path workspaceDir;
    filter = path: type:
      (builtins.match ".*proto$" path != null) # include protobufs
      || (builtins.match ".*fbs$" path != null) # include flatbuffers
      || (builtins.match ".*js$" path != null) # include js (for deno runtime)
      || (craneLib.filterCargoSources path type); # include rust/cargo
  };

  buildArgs = ({
    nativeBuildInputs = with pkgs; [
      cargo-nextest
      cargo-flamegraph
      samply
      clang
      cmake
      llvmPackages.libclang.lib
      pkg-config
      protobuf
      flatbuffers
      rustToolchain
      openssl
      jq
      sqlite
    ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
      CoreFoundation
      CoreServices
      Security
      SystemConfiguration
    ]);

    # used by bindgen
    LIBCLANG_PATH = pkgs.lib.makeLibraryPath [
      pkgs.llvmPackages.libclang.lib
    ];
  });

  commonArgs = (buildArgs // {
    inherit src;
  });

  cargoArtifacts = craneLib.buildDepsOnly (commonArgs // {
    pname = "apibara";
    version = "0.0.0";
  });

  allCrates = craneLib.buildPackage (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
    doCheck = false;
  });

  cargoFmt = craneLib.cargoFmt (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
  });

  cargoClippy = craneLib.cargoClippy (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
  });

  unitTests = craneLib.cargoNextest (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
    cargoNextestExtraArgs = "-E 'kind(lib)'";
  });

  integrationTestsArchive = craneLib.mkCargoDerivation (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara-nextest-archive";
    version = "0.0.0";
    doCheck = true;

    nativeBuildInputs = (commonArgs.nativeBuildInputs or [ ]) ++ [ pkgs.cargo-nextest ];

    buildPhaseCargoCommand = ''
      mkdir -p $out
      cargo nextest --version
    '';

    # Work around Nextest bug: https://github.com/nextest-rs/nextest/issues/267
    preCheck = pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
      export DYLD_FALLBACK_LIBRARY_PATH=$(${pkgs.rustc}/bin/rustc --print sysroot)/lib
    '';

    checkPhaseCargoCommand = ''
      cargo nextest archive --cargo-profile $CARGO_PROFILE --archive-format tar-zst --archive-file $out/archive.tar.zst
    '';
  });

  /* Build a crate from a path, optionally overriding the binary name.

    Arguments:

     - `path`: create path, e.g. `./sinks/sink-postgres`.
     - `binaryName`: override the binary name. Defaults to the crate name.
   */
  buildCrate = { path, binaryName ? null }:
    let
      manifest = builtins.fromTOML (builtins.readFile (path + "/Cargo.toml"));
      pname = manifest.package.name;
      realBinaryName = if binaryName != null then binaryName else pname;
      binaryPath = "${allCrates}/bin/${realBinaryName}";
      version = manifest.package.version;
      bin = pkgs.stdenv.mkDerivation {
        name = "${pname}-${version}";
        buildInputs = [ allCrates ];
        phases = [ "installPhase" ];
        installPhase = ''
          mkdir -p $out/bin
          cp -r ${binaryPath} $out/bin
        '';
      };
    in
    {
      inherit pname version bin;
      binaryName = realBinaryName;
    };

  /* Dockerize a binary crate.

    Arguments:

     - `crate`: the crate.
     - `volumes`: volumes to mount in the container.
     - `ports`: ports to expose.
     - `description`: description of the image.
     - `binaryName`: override the entrypoint binary.
     - `extraBinaries`: extra binaries to copy to the image.
   */
  dockerizeCrateBin = { crate, volumes ? null, ports ? null, description ? null, binaryName ? null, extraBinaries ? [ ] }:
    pkgs.dockerTools.buildImage {
      name = crate.pname;
      # we're publishing images, so make it less confusing
      tag = "latest";
      created = "now";
      copyToRoot = with pkgs.dockerTools; [
        usrBinEnv
        binSh
        caCertificates
      ] ++ extraBinaries;
      config = {
        Entrypoint = [
          "${crate.bin}/bin/${if binaryName != null then binaryName else crate.pname}"
        ];
        Volumes = volumes;
        ExposedPorts = ports;
        Labels = ({
          "org.opencontainers.image.source" = "https://github.com/apibara/dna";
          "org.opencontainers.image.licenses" = "Apache-2.0";
        } // (if description != null then { "org.opencontainers.image.description" = description; } else { }));
      };
    };

  # NixOS binaries. They won't work on non-NixOS systems, but they're
  # used for testing.
  binaries = builtins.mapAttrs
    (_: crate: buildCrate {
      inherit (crate) path;
      binaryName = if crate ? binaryName then crate.binaryName else null;
    })
    crates;

  # Binaries for non-NixOS systems.
  binariesUniversal =
    let
      mkUniversal = name: value:
        {
          name = "${name}";
          value = pkgs.stdenv.mkDerivation {
            name = "${name}";
            buildInputs = [
              value.bin
              pkgs.patchelf
            ];
            phases = [ "installPhase" ];
            installPhase =
              if pkgs.stdenv.isLinux then
                let
                  interpreter =
                    if pkgs.system == "x86_64-linux" then
                      "/lib64/ld-linux-x86-64.so.2"
                    else
                      "/lib/ld-linux-aarch64.so.1";
                in
                ''
                  mkdir -p $out/bin
                  cp --no-preserve=mode ${value.bin}/bin/${value.binaryName} $out/bin
                  chmod +x $out/bin/${value.binaryName}
                  patchelf --set-interpreter ${interpreter} $out/bin/${value.binaryName}
                ''
              else
                ''
                  mkdir -p $out/bin
                  cp -r ${value.bin}/bin $out
                '';
          };
        };
    in
    pkgs.lib.attrsets.mapAttrs' mkUniversal binaries;

  binariesArchive =
    let
      mkArchive = name: value: {
        name = "${name}-archive";
        value = pkgs.stdenv.mkDerivation {
          name = "${name}-archive";
          buildInputs = [
            value
            pkgs.gzip
          ];
          phases = [ "installPhase" ];
          installPhase = ''
            mkdir -p $out
            gzip -c ${value}/bin/* > $out/${name}.gz
          '';
        };
      };
    in
    pkgs.lib.attrsets.mapAttrs' mkArchive binariesUniversal;

  # Helper to move the docker archive into a tar.gz file.
  mkDockerArchive = name: image: pkgs.stdenv.mkDerivation {
    name = "${name}";
    buildInputs = [
      pkgs.skopeo
    ];
    phases = [ "installPhase" ];
    installPhase = ''
      mkdir -p $out
      echo '{"default": [{"type": "insecureAcceptAnything"}]}' > /tmp/policy.json
      skopeo copy --policy=/tmp/policy.json --tmpdir=/tmp docker-archive:${image} docker-archive:$out/${name}.tar.gz
    '';
  };

  # Docker images.
  images =
    let
      crateNames = builtins.attrNames crates;
      images = map
        (name:
          let
            def = crates.${name};
            extraBinaries = map (bin: binaries.${bin}.bin) (if def ? extraBinaries then def.extraBinaries else [ ]);
            built = dockerizeCrateBin {
              description = def.description;
              binaryName = if def ? binaryName then def.binaryName else null;
              volumes = if def ? volumes then def.volumes else null;
              ports = if def ? ports then def.ports else null;
              crate = binaries.${name};
              extraBinaries = extraBinaries;
            };
          in
          {
            name = "${name}-image";
            value = mkDockerArchive name built;
          }
        )
        crateNames;
    in
    builtins.listToAttrs images;

  # Ubuntu-based Docker images.
  imagesUbuntu =
    let
      # Base image.
      # Args obtained with:
      # nix-prefetch-docker ubuntu 22.04
      ubuntu = pkgs.dockerTools.pullImage {
        imageName = "ubuntu";
        imageDigest = "sha256:77906da86b60585ce12215807090eb327e7386c8fafb5402369e421f44eff17e";
        sha256 = "0vgrb2lwxxxncw0k8kc0019zpg32bdq7d427lszzdnpdy98pfvbc";
        finalImageName = "ubuntu";
        finalImageTag = "22.04";
      };

      mkUbuntuImage = name: value: {
        name = "${name}-image-ubuntu";
        value =
          let
            image = pkgs.dockerTools.buildImage {
              name = name;
              # we're publishing images, so make it less confusing
              tag = "latest-ubuntu";
              created = "now";
              fromImage = ubuntu;
              copyToRoot = pkgs.buildEnv {
                name = "${name}-image-ubuntu-root";
                pathsToLink = [ "/bin" ];
                paths = [
                  value
                ];
              };
            };
          in
          mkDockerArchive name image;
      };
    in
    pkgs.lib.attrsets.mapAttrs' mkUbuntuImage binariesUniversal;
in
{
  checks = {
    inherit cargoFmt cargoClippy;
  };

  shell = {
    default = pkgs.mkShell (buildArgs // {
      inputsFrom = [
        allCrates
      ];
      buildInputs = with pkgs; [
        kubernetes-helm
        tokio-console
      ];
    });

    nightly = pkgs.mkShell (buildArgs // {
      nativeBuildInputs = with pkgs; [
        nightlyRustToolchain
        cargo-udeps
        clang
        cmake
        llvmPackages.libclang.lib
        pkg-config
        protobuf
        flatbuffers
        openssl
      ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
        CoreFoundation
        CoreServices
        Security
        SystemConfiguration
      ]);
    });

    # Integration tests require an internet connection, which is
    # not available inside the Nix build environment.
    # Use the generated nextest archive and run the tests outside of
    # the build environment.
    integration =
      let
        runTests = pkgs.writeScriptBin "run-integration-tests" ''
          echo "Using archive at ${integrationTestsArchive}"
          cargo nextest run \
            --archive-file ${integrationTestsArchive}/archive.tar.zst\
            --workspace-remap=. \
            -E 'kind(test)'
        '';
      in
      pkgs.mkShell (buildArgs // {
        inputsFrom = [
          integrationTestsArchive
        ];

        nativeBuildInputs = (integrationTestsArchive.nativeBuildInputs or [ ]) ++ [ runTests ];

        ARCHIVE_PATH = "${integrationTestsArchive}/archive.tar.zst";
      });

    ci =
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

          echo "target=''${target}" >> "$GITHUB_OUTPUT"
          echo "major=''${major}" >> "$GITHUB_OUTPUT"
          echo "minor=''${minor}" >> "$GITHUB_OUTPUT"
          echo "patch=''${patch}" >> "$GITHUB_OUTPUT"
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

          echo "::group::Publishing image ''${IMAGE_NAME}:''${version}"
          dry_run skopeo copy "docker-archive:''${IMAGE_ARCHIVE_x86_64}" "docker://''${base}:''${version}-x86_64"
          dry_run skopeo copy "docker-archive:''${IMAGE_ARCHIVE_aarch64}" "docker://''${base}:''${version}-aarch64"
          echo "::endgroup::"

          images=("''${base}:''${version}-x86_64" "''${base}:''${version}-aarch64")

          echo "::group::Create manifest ''${base}:''${version}"
          manifest="''${base}:''${version}"
          dry_run buildah manifest create "''${manifest}" "''${images[@]}"
          echo "::endgroup::"

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
      pkgs.mkShell {
        buildInputs = [
          extractVersionFromTag
          publishDockerImage
        ];
      };
  };

  binaries = builtins.attrNames binariesUniversal;

  packages = images // imagesUbuntu // binariesUniversal // binariesArchive // {
    all-crates = allCrates;
    unit-tests = unitTests;
    integration-tests-archive = integrationTestsArchive;
  };
}
