{
  description = "Apibara development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.11";
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

  outputs = { nixpkgs, rust-overlay, flake-utils, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        crates = {
          dna-evm = {
            description = "The EVM DNA server";
            path = ./evm;
            ports = {
              "7001/tcp" = { };
              "7007/tcp" = { };
            };
          };
          dna-starknet = {
            description = "The Starknet DNA server";
            path = ./starknet;
            ports = {
              "7001/tcp" = { };
              "7007/tcp" = { };
            };
          };
        };

        builtCrates = pkgs.callPackage ./nix/crates.nix {
          inherit crane crates;
          workspaceDir = ./.;
        };
      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # checks. run with `nix flake check`.
        checks = builtCrates.checks;

        # development shells. start with `nix develop`.
        devShells = builtCrates.shell;

        # all packages.
        # show them with `nix flake show`.
        # build with `nix build .#<name>`.
        packages = (builtCrates.packages // { });
      }
    );

  nixConfig = {
    extra-substituters = [ "https://apibara-public.cachix.org" ];
    extra-trusted-public-keys = [
      "apibara-public.cachix.org-1:FLOMNlARo9CcxtcLDblZlt2xhsu/pa/EddEH1cM3Vog="
    ];
  };
}
