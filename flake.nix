{
  description = "Apibara development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    cargo2nix.url = "github:cargo2nix/cargo2nix/release-0.11.0";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, cargo2nix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        # setup overlay with stable rust.
        overlays = [
          (import rust-overlay)
          (import ./nix/overlay.nix)
          cargo2nix.overlays.default
        ];

        # update packages to use the current system and overlay.
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rustPkgs = pkgs.rustBuilder.makePackageSet {
          packageFun = import ./Cargo.nix;
          rustToolchain = pkgs.rustVersion;
          packageOverrides = pkgs: pkgs.apibaraBuilder.overrides;
        };

        dockerizeCrateBin = { crate, volumes ? null, ports ? null }:
          pkgs.dockerTools.buildImage {
            name = crate.name;
            copyToRoot = with pkgs.dockerTools; [
              usrBinEnv
              binSh
              caCertificates
            ];
            config = {
              Cmd = [
                "${crate.bin}/bin/${crate.name}"
              ];
              Volumes = volumes;
              Ports = ports;
            };
          };
      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # development shells. start with `nix develop`.
        devShells = {
          default = rustPkgs.workspaceShell {
            LIBCLANG_PATH = pkgs.lib.makeLibraryPath [ pkgs.llvmPackages.libclang.lib ];
            nativeBuildInputs = with pkgs; [
              clang
              pkg-config
              llvmPackages.libclang
              protobuf
            ];
          };
        };

        packages = {
          # libs
          core-lib = rustPkgs.workspace.apibara-core { };
          node-lib = rustPkgs.workspace.apibara-node { };
          sdk-lib = rustPkgs.workspace.apibara-sdk { };

          # docker images
          starknet-image = dockerizeCrateBin {
            crate = (rustPkgs.workspace.apibara-starknet { });
            ports = {
              "7171/tcp" = { };
            };
          };
        };
      }
    );
}
