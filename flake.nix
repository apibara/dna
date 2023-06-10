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

        native = pkgs.callPackage ./nix/native.nix {
          inherit rustToolchain crane;
          workspaceDir = ./.;
        };
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
