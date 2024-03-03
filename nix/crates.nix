# Build the provided crates using crane.
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
      || (builtins.match ".*js$" path != null) # include js (for deno runtime)
      || (craneLib.filterCargoSources path type); # include rust/cargo
  };

  buildArgs = ({
    nativeBuildInputs = with pkgs; [
      clang
      cmake
      llvmPackages.libclang.lib
      pkg-config
      protobuf
      rustToolchain
      openssl
      jq
    ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
      CoreFoundation
      CoreServices
      Security
      SystemConfiguration
    ]);

    buildInputs = with pkgs; [
      librusty_v8
    ];

    RUSTY_V8_ARCHIVE = "${pkgs.librusty_v8}/lib/librusty_v8.a";
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

  testBinaries = craneLib.buildPackage (commonArgs // {
    inherit cargoArtifacts;
    pname = "apibara-test";
    version = "0.0.0";
    cargoExtraArgs = "--tests";
    doCheck = false;

    installPhaseCommand = ''
      local dest="$out"
      local log="$cargoBuildLog"

      if ! [ -f "''${log}" ]; then
        echo "unable to install, cargo build log does not exist at: ''${log}"
        false
      fi

      echo "searching for tests to install from cargo build log at ''${log}"

      local logs
      logs=$(jq -R 'fromjson?' <"''${log}")

      local select_test='select(.reason == "compiler-artifact" and .profile.test == true)'
      local select_bins="''${select_test} | .executable | select(.!= null)"

      function installArtifacts() {
        local loc=''${1?:missing}
        mkdir -p "''${loc}"

        while IFS= read -r to_install; do
          echo "installing ''${to_install}"
          cp "''${to_install}" "''${loc}"
        done

        rmdir --ignore-fail-on-non-empty "''${loc}"
      }

      echo "''${logs}" | jq -r "''${select_bins}" | installArtifacts "''${dest}/bin"
      echo "searching for tests complete"
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
            value = built;
          }
        )
        crateNames;
    in
    builtins.listToAttrs images;

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

  # Binaries as packages.
  # binaryPackages = builtins.mapAttrs (_: crate: crate.bin) binaries;
in
rec {
  checks = {
    inherit cargoFmt cargoClippy;
  };

  shell = {
    default = pkgs.mkShell (buildArgs // {
      inputsFrom = [
        allCrates
      ];
    });
  };

  binaries = builtins.attrNames binariesUniversal;

  packages = images // binariesUniversal // {
    all-crates = allCrates;
    tests = testBinaries;
  };
}
