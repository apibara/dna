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
        overlays = [
          (import rust-overlay)
          (import ./nix/overlay.nix)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        crates = {
          sdk = {
            path = ./sdk;
            docker = false;
          };
          starknet = {
            path = ./starknet;
            ports = {
              "7171/tcp" = { };
            };
          };
          sink-webhook = { path = ./sink-webhook; };
          sink-mongo = { path = ./sink-mongo; };
          sink-postgres = { path = ./sink-postgres; };
          sink-parquet = { path = ./sink-parquet; };
        };

        native = pkgs.callPackage ./nix/native.nix {
          inherit crane crates;
          workspaceDir = ./.;
        };

        # cross = pkgs.callPackage ./nix/cross.nix {
        # inherit crane;
        # workspaceDir = ./.;
        # };
      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # development shells. start with `nix develop`.
        # devShells.default = pkgs.mkShell (buildArgs // { });

        checks = {
          inherit (native.checks) workspaceFmt workspaceClippy;
        };

        packages = (native.packages // { });
      }
    );

  nixConfig = {
    extra-substituters = [ "https://apibara-public.cachix.org" ];
    extra-trusted-public-keys = [
      "apibara-public.cachix.org-1:FLOMNlARo9CcxtcLDblZlt2xhsu/pa/EddEH1cM3Vog="
    ];
  };
}
