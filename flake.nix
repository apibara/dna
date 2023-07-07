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
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, pre-commit-hooks, ... }:
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
            description = "The Starknet DNA server";
            path = ./starknet;
            ports = {
              "7171/tcp" = { };
            };
          };
          sink-webhook = {
            description = "Integration to connect onchain data to HTTP endpoints";
            path = ./sink-webhook;
            volumes = {
              "/data" = { };
            };
          };
          sink-mongo = {
            description = "Integration to populate a MongoDB collection with onchain data";
            path = ./sink-mongo;
            volumes = {
              "/data" = { };
            };
          };
          sink-postgres = {
            description = "Integration to populate a PostgreSQL table with onchain data";
            path = ./sink-postgres;
            volumes = {
              "/data" = { };
            };
          };
          sink-parquet = {
            description = "Integration to generate a Parquet dataset from onchain data";
            path = ./sink-parquet;
            volumes = {
              "/data" = { };
            };
          };
        };

        native = pkgs.callPackage ./nix/native.nix {
          inherit crane crates;
          pre-commit-hooks = pre-commit-hooks.lib.${system};
          workspaceDir = ./.;
        };

        aarch64Linux =
          let
            crossSystem = "aarch64-linux";
            crossPkgs = import nixpkgs {
              inherit overlays crossSystem;
              localSystem = system;
            };
          in
          pkgs.callPackage ./nix/cross.nix {
            inherit crane crates crossSystem;
            pkgs = crossPkgs;
            workspaceDir = ./.;
          };

        crosses = {
          "x86_64-linux" = aarch64Linux;
          "x86_64-darwin" = aarch64Linux;
          "aarch64-linux" = { packages = { }; };
          "aarch64-darwin" = { packages = { }; };
        };

        cross = crosses.${system};
      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # checks. run with `nix flake check`.
        checks = native.checks;

        # development shells. start with `nix develop`.
        devShells = native.shell;

        # all packages.
        # show them with `nix flake show`.
        # build with `nix build .#<name>`.
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
