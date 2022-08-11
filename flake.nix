{
  description = "Apibara development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem(system:
      let
        overlays = [
          (import rust-overlay)
        ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in
        {
          devShells.default = pkgs.mkShell {
            buildInputs = with pkgs; [
              openssl
              pkg-config
              protobuf
              rust-bin.stable.latest.default
            ];
          };
        }
    );
}