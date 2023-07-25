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
          starknet = {
            description = "The Starknet DNA server";
            path = ./starknet;
            ports = {
              "7171/tcp" = { };
            };
          };
          operator = {
            description = "The Apibara Kubernetese Operator";
            path = ./operator;
            ports = {
              "8118/tcp" = { };
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

        ci = pkgs.callPackage ./nix/ci.nix {
          tests = native.packages.tests;
          binaries = native.binaries;
        };
      in
      {
        inherit (ci) pipeline;

        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # checks. run with `nix flake check`.
        checks = native.checks;

        # development shells. start with `nix develop`.
        devShells = (native.shell // ci.shell);

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
