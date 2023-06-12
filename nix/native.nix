{ pkgs, crane, workspaceDir, crates }:
let
  rustToolchain = pkgs.rust-bin.stable.latest.default.override {
    extensions = [ "rust-src" "rust-analyzer" ];
  };

  buildLib = import ./build.nix { inherit pkgs crane rustToolchain workspaceDir; };

  nightlyRustToolchain = pkgs.rust-bin.nightly.latest.default;
  nightlyBuildLib = import ./build.nix {
    inherit pkgs crane workspaceDir;
    rustToolchain = nightlyRustToolchain;
  };

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

  shell = {
    default = pkgs.mkShell (buildLib.buildArgs // { });

    nightly = pkgs.mkShell (nightlyBuildLib.buildArgs // {
      nativeBuildInputs = with pkgs; [
        cargo-udeps
      ] ++ nightlyBuildLib.buildArgs.nativeBuildInputs;
    });

    jupyter = pkgs.mkShell {
      nativeBuildInputs = with pkgs; [
        jupyter
        python310Packages.seaborn
        python310Packages.matplotlib
        python310Packages.pandas
        python310Packages.pyarrow
      ];
    };
  };

  packages = (built.packages // {
    tests = workspaceTest;
  });
}
