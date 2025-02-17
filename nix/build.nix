{ pkgs, crane, workspaceDir, crates }:
let
  rustToolchain = (pkgs.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml).override {
    extensions = [ "rust-src" "rust-analyzer" ];
  };

  craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

  src = pkgs.lib.cleanSourceWith {
    src = craneLib.path workspaceDir;
    filter = path: type:
      (builtins.match ".*proto$" path != null) # include protobufs
      || (craneLib.filterCargoSources path type); # include rust/cargo
  };

  buildArgs = ({
    nativeBuildInputs = with pkgs; [
      cargo-nextest
      cargo-flamegraph
      cargo-edit

      clang
      cmake
      llvmPackages.libclang.lib
      libllvm
      pkg-config
      protobuf
      rustToolchain
      openssl.dev
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

  # Shared cargo artifacts.
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

  # cargo fmt --check
  cargoFmt = craneLib.cargoFmt (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
  });

  # cargo clippy
  cargoClippy = craneLib.cargoClippy (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
  });

  # Build unit tests.
  unitTests = craneLib.cargoNextest (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara";
    version = "0.0.0";
    cargoNextestExtraArgs = "-E 'kind(lib)'";
  });

  # Create nextest archive.
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
      cargo nextest -P ci archive --cargo-profile $CARGO_PROFILE --workspace --archive-format tar-zst --archive-file $out/archive.tar.zst
    '';
  });

  /* Build a crate from a path, optionally overriding the binary name.

    Arguments:

     - `path`: create path, e.g. `./sinks/sink-postgres`.
   */
  buildCrate = { path }:
    let
      manifest = builtins.fromTOML (builtins.readFile (path + "/Cargo.toml"));
    in
    pkgs.stdenv.mkDerivation rec {
      pname = manifest.package.name;
      version = manifest.package.version;
      buildInputs = [ allCrates ];
      phases = [ "installPhase" ];
      installPhase = ''
        mkdir -p $out/bin
        cp -r ${allCrates}/bin/${pname} $out/bin
      '';
    };

  # NixOS binaries. They won't work on non-NixOS systems, but they're
  # used for testing.
  binariesNix = builtins.mapAttrs
    (_: crate: {
      out = buildCrate {
        inherit (crate) path;
      };
      meta = { inherit (crate) description ports; };
    })
    crates;

  # Binaries for non-NixOS systems.
  # Use patchelf to change the interpreter.
  binariesUniversal =
    let
      mkUniversal = name: crate:
        {
          inherit name;
          value = pkgs.stdenv.mkDerivation {
            name = "${name}";
            buildInputs = [
              pkgs.patchelf
              crate.out
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
                  for bin in ${crate.out}/bin/*; do
                    cp --no-preserve=mode $bin $out/bin
                  done
                  for bin in $out/bin/*; do
                    chmod +x $bin
                    patchelf --set-interpreter ${interpreter} $bin
                  done
                ''
              else
                ''
                  mkdir -p $out/bin
                  cp -r ${crate.out}/bin $out
                '';
          };
        };
    in
    pkgs.lib.attrsets.mapAttrs' mkUniversal binariesNix;

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

  images =
    let
      mkImage = name: crate: pkgs.dockerTools.buildImage {
        inherit name;
        # we're publishing images, so make it less confusing
        tag = "latest";
        created = "now";
        copyToRoot = with pkgs.dockerTools; [
          usrBinEnv
          binSh
          caCertificates
        ];
        config = {
          Entrypoint = [
            "${crate.out}/bin/apibara-${name}"
          ];
          ExposedPorts = crate.meta.ports;
          Labels = ({
            "org.opencontainers.image.source" = "https://github.com/apibara/dna";
            "org.opencontainers.image.licenses" = "Apache-2.0";
          } // (if crate.meta.description != null then { "org.opencontainers.image.description" = crate.meta.description; } else { }));
        };
      };

      mkDockerArchive = name: crate:
        let
          image = mkImage name crate;
        in
        {
          name = "${name}-image";
          value = pkgs.stdenv.mkDerivation {
            inherit name;
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
        };
    in
    pkgs.lib.attrsets.mapAttrs' mkDockerArchive binariesNix;
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
    });

    # Integration tests require an internet connection, which is
    # not available inside the Nix build environment.
    # Use the generated nextest archive and run the tests outside of
    # the build environment.
    integration =
      let
        runTests = pkgs.writeScriptBin "run-integration-tests" ''
          echo "Using archive at ${integrationTestsArchive}"
          cargo nextest -P ci run \
            --archive-file ${integrationTestsArchive}/archive.tar.zst\
            --workspace-remap=. \
            --test-threads=1 \
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
  };

  packages = binariesUniversal // binariesArchive // images // {
    all-crates = allCrates;
    unit-tests = unitTests;
    integration-tests-archive = integrationTestsArchive;
  };
}
