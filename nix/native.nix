{ pkgs, crane, workspaceDir, crates }:
let
  rustToolchain = pkgs.rust-bin.stable.latest.default.override {
    extensions = [ "rust-src" ];
  };

  buildLib = import ./build.nix { inherit pkgs crane rustToolchain workspaceDir; };

  workspaceFmt = buildLib.cargoFmt (buildLib.commonArgs // {
    inherit (buildLib) cargoArtifacts;
    pname = "apibara-fmt";
    version = "0.0.0";
  });

  workspaceClippy = buildLib.cargoClippy (buildLib.commonArgs // {
    inherit (buildLib) cargoArtifacts;
    pname = "apibara-clippy";
    version = "0.0.0";
  });

  workspaceTest = buildLib.buildCrateTests (buildLib.commonArgs // {
    inherit (buildLib) cargoArtifacts;
    pname = "apibara-clippy";
    version = "0.0.0";
  });

  built = buildLib.buildCrates crates;
in
{
  checks = {
    inherit workspaceFmt workspaceClippy;
  };

  shell = pkgs.mkShell (buildLib.buildArgs // { });

  packages = (built.packages // {
    tests = workspaceTest;
  });
}
