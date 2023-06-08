{
  description = "Apibara development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # setup overlay with stable rust.
        overlays = [
          (import rust-overlay)
          (import ./nix/overlay.nix)
        ];

        # update packages to use the current system and overlay.
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" ];
        };

        installTestsFromCargoBuildLogHook = pkgs.makeSetupHook
          { name = "installTestsFromCargoBuildLogHook"; } ./nix/installTestsFromCargoBuildLogHook.sh;

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        src = pkgs.lib.cleanSourceWith {
          src = craneLib.path ./.; # all sources
          filter = path: type:
            (builtins.match ".*proto$" path != null) # include protobufs
            || (builtins.match ".*js$" path != null) # include js (for deno runtime)
            || (craneLib.filterCargoSources path type); # include rust/cargo
        };

        buildArgs = {
          nativeBuildInputs = with pkgs; [
            clang
            pkg-config
            protobuf
          ] ++ pkgs.lib.optional stdenv.isDarwin (with pkgs.darwin.apple_sdk.frameworks; [
            CoreFoundation
            CoreServices
            Security
          ]);

          buildInputs = with pkgs; [
            rustToolchain
            librusty_v8
          ];

          RUSTY_V8_ARCHIVE = "${pkgs.librusty_v8}/lib/librusty_v8.a";
          LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages.libclang.lib ];
        };

        commonArgs = (buildArgs // {
          inherit src;
        });

        cargoArtifacts = craneLib.buildDepsOnly (commonArgs // {
          pname = "apibara";
          version = "0.0.0";
        });

        workspaceFmt = craneLib.cargoFmt (commonArgs // {
          inherit cargoArtifacts;
          pname = "apibara-fmt";
          version = "0.0.0";
        });

        workspaceClippy = craneLib.cargoClippy (commonArgs // {
          inherit cargoArtifacts;
          pname = "apibara-clippy";
          version = "0.0.0";
        });

        workspaceTest = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "apibara-clippy";
          version = "0.0.0";
          doCheck = false;
          cargoExtraArgs = "--tests";

          nativeBuildInputs = [
            installTestsFromCargoBuildLogHook
          ] ++ (commonArgs.nativeBuildInputs or []);

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

        buildCrate = { crate }:
          let
            manifest = builtins.fromTOML (builtins.readFile (crate + "/Cargo.toml"));
            pname = manifest.package.name;
            version = manifest.package.version;
            bin = craneLib.buildPackage (commonArgs // {
              inherit pname version cargoArtifacts;

              cargoExtraArgs = "--package ${pname}";
              doCheck = false;
            });
          in
          {
            inherit pname version bin;
          };

        dockerizeCrateBin = { crate, volumes ? null, ports ? null }:
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
            };
          };

        apibara = {
          sdk = buildCrate { crate = ./sdk; };
          starknet = buildCrate { crate = ./starknet; };
          sink-webhook = buildCrate { crate = ./sink-webhook; };
          sink-mongo = buildCrate { crate = ./sink-mongo; };
          sink-postgres = buildCrate { crate = ./sink-postgres; };
          sink-parquet = buildCrate { crate = ./sink-parquet; };
        };
      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # development shells. start with `nix develop`.
        devShells.default = pkgs.mkShell (buildArgs // { });

        checks = {
          inherit workspaceFmt workspaceClippy;
        };

        packages = {
          tests = workspaceTest;

          sdk = apibara.sdk.bin;
          starknet = apibara.starknet.bin;
          sink-webhook = apibara.sink-webhook.bin;
          sink-mongo = apibara.sink-mongo.bin;
          sink-postgres = apibara.sink-postgres.bin;
          sink-parquet = apibara.sink-parquet.bin;

          starknet-image = dockerizeCrateBin {
            crate = apibara.starknet;
            ports = {
              "7171/tcp" = { };
            };
          };

          sink-webhook-image = dockerizeCrateBin {
            crate = apibara.sink-webhook;
          };

          sink-mongo-image = dockerizeCrateBin {
            crate = apibara.sink-mongo;
          };

          sink-postgres-image = dockerizeCrateBin {
            crate = apibara.sink-postgres;
          };

          sink-parquet-image = dockerizeCrateBin {
            crate = apibara.sink-parquet;
          };
        };
      }
    );

  nixConfig = {
    extra-substituters = [ "https://apibara-public.cachix.org" ];
    extra-trusted-public-keys = [
      "apibara-public.cachix.org-1:FLOMNlARo9CcxtcLDblZlt2xhsu/pa/EddEH1cM3Vog="
    ];
  };
}
