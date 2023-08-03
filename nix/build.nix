{ pkgs, crane, rustToolchain, workspaceDir, extraBuildArgs ? { } }:
let
  # script needed to install tests from cargo build log
  installTestsFromCargoBuildLogHook = pkgs.makeSetupHook
    { name = "installTestsFromCargoBuildLogHook"; } ./installTestsFromCargoBuildLogHook.sh;
  craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
in
rec {
  inherit (craneLib) cargoFmt cargoClippy buildPackage;

  src = pkgs.lib.cleanSourceWith {
    src = craneLib.path workspaceDir; # all sources
    filter = path: type:
      (builtins.match ".*proto$" path != null) # include protobufs
      || (builtins.match ".*js$" path != null) # include js (for deno runtime)
      || (craneLib.filterCargoSources path type); # include rust/cargo
  };

  buildArgs = ({
    nativeBuildInputs = with pkgs.pkgsBuildHost; [
      clang
      llvmPackages.libclang.lib
      pkg-config
      protobuf
      rustToolchain
    ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
      CoreFoundation
      CoreServices
      Security
    ]);

    buildInputs = with pkgs.pkgsHostHost; [
      librusty_v8
    ];

    RUSTY_V8_ARCHIVE = "${pkgs.pkgsHostHost.librusty_v8}/lib/librusty_v8.a";
    # used by bindgen
    LIBCLANG_PATH = pkgs.lib.makeLibraryPath [
      pkgs.pkgsBuildHost.llvmPackages.libclang.lib
    ];
  } // extraBuildArgs);

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

  buildCrateTests = args:
    craneLib.buildPackage (args // {
      doCheck = false;
      cargoExtraArgs = "--tests";

      nativeBuildInputs = [
        installTestsFromCargoBuildLogHook
      ] ++ (commonArgs.nativeBuildInputs or [ ]);

      # extract the test binary from the build log
      installPhaseCommand = ''
        if [ -n "$cargoBuildLog" -a -f "$cargoBuildLog" ]; then
          installTestsFromCargoBuildLog "$out" "$cargoBuildLog"
        else
          echo ${pkgs.lib.strings.escapeShellArg ''
            !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            $cargoBuildLog is either undefined or does not point to a valid file location!
            By default `buildPackage` will capture cargo's output and use it to determine which binaries
            should be installed (instead of just guessing based on what is present in cargo's target directory).
            If you are overriding the derivation with a custom build step, you have two options:
            1. override `installPhaseCommand` with the appropriate installation steps
            2. ensure that cargo's build log is captured in a file and point $cargoBuildLog at it
            At a minimum, the latter option can be achieved with running:
                cargoBuildLog=$(mktemp cargoBuildLogXXXX.json)
                cargo build --release --message-format json-render-diagnostics >"$cargoBuildLog"
          ''}

          false
        fi
      '';
    });

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

  /* Build all crates and dockerize them (if requested).

     Example:
       buildCrates [
        { path = ./my-crate; }
        { path = ./my-service; ports = { "8000/tcp" = { }; }; }
        { path = ./my-service; volumes = { "/data" = { }; }; }
        { path = ./my-other-crate; docker = false; }
       ];
   */
  buildCrates = crates:
    let
      binaries = builtins.mapAttrs
        (_: crate: buildCrate {
          inherit (crate) path;
          binaryName = if crate ? binaryName then crate.binaryName else null;
        })
        crates;
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
      binariesUniversal =
        let
          mkUniversal = name: value:
            {
              name = "${name}-universal";
              value = pkgs.stdenv.mkDerivation {
                name = "${name}-universal";
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
      binaryPackages = builtins.mapAttrs (_: crate: crate.bin) binaries;
      packages = binaryPackages // images // binariesUniversal // {
        all-crates = allCrates;
      };
    in
    {
      inherit binaryPackages images packages;
    };
}
