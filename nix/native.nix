{ pkgs, crane, pre-commit-hooks, workspaceDir, crates }:
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

  workspaceTest = buildLib.buildCrateTests (buildLib.commonArgs // {
    inherit (buildLib) cargoArtifacts;
    pname = "apibara-test";
    version = "0.0.0";
  });

  built = buildLib.buildCrates crates;
in
rec {
  checks = {
    pre-commit-check = pre-commit-hooks.run {
      src = ./.;
      hooks = {
        nixpkgs-fmt.enable = true;
        rustfmt.enable = true;
        clippy.enable = true;
        cargo-check.enable = true;
      };
      tools = {
        rustfmt = rustToolchain;
        clippy = rustToolchain;
        cargo = rustToolchain;
      };
    };
  };

  shell = rec {
    default = pkgs.mkShell (buildLib.buildArgs // {
      inherit (checks.pre-commit-check) shellHook;
    });

    nightly = pkgs.mkShell (nightlyBuildLib.buildArgs // {
      nativeBuildInputs = with pkgs; [
        cargo-udeps
      ] ++ nightlyBuildLib.buildArgs.nativeBuildInputs;
    });

    # demo used to quickly demo dna and integrations
    demo = pkgs.mkShell {
      nativeBuildInputs = with pkgs; [
        jupyter
        python310Packages.seaborn
        python310Packages.matplotlib
        python310Packages.pandas
        python310Packages.pyarrow

        built.packages.sink-webhook
        built.packages.sink-mongo
        built.packages.sink-postgres
        built.packages.sink-parquet
      ];
    };
  };

  packages = (built.packages // {
    tests = workspaceTest;
  });
}
