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
    pname = "apibara";
    version = "0.0.0";
    doCheck = false;
  });

  buildCrate = { crate }:
    let
      manifest = builtins.fromTOML (builtins.readFile (crate + "/Cargo.toml"));
      pname = manifest.package.name;
      version = manifest.package.version;
      bin = pkgs.stdenv.mkDerivation {
        name = "${pname}-${version}";
        buildInputs = [ allCrates ];
        phases = [ "installPhase" ];
        installPhase = ''
          mkdir -p $out/bin
          cp -r ${allCrates}/bin/${pname} $out/bin
        '';
      };
    in
    {
      inherit pname version bin;
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

  dockerizeCrateBin = { crate, volumes ? null, ports ? null, description ? null }:
    pkgs.dockerTools.buildImage {
      name = crate.pname;
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
          "${crate.bin}/bin/${crate.pname}"
        ];
        Volumes = volumes;
        ExposedPorts = ports;
        Labels = ({
          "org.opencontainers.image.source" = "https://github.com/apibara/dna";
          "org.opencontainers.image.licenses" = "Apache-2.0";
        } // (if description != null then { "org.opencontainers.image.description" = description; } else { }));
      };
    };

  buildAndDockerize = { path, docker ? true, ports ? null, volumes ? null, description ? null }:
    let
      crate = buildCrate { crate = path; };
      image =
        if docker then
          dockerizeCrateBin
            {
              crate = crate;
              ports = ports;
              volumes = volumes;
              description = description;
            } else null;
    in
    {
      inherit crate image;
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
      built = builtins.mapAttrs (_: crate: buildAndDockerize crate) crates;
      binaryPackages = builtins.mapAttrs (_: { crate, ... }: crate.bin) built;
      images =
        let
          crateNames = builtins.attrNames crates;
          maybeImages = map (name: { name = "${name}-image"; value = built.${name}.image; }) crateNames;
          images = builtins.filter (image: image.value != null) maybeImages;
        in
        (builtins.listToAttrs images);
      packages = binaryPackages // images // {
        all-crates = allCrates;
      };
    in
    {
      inherit binaryPackages images packages;
    };
}
