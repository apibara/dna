{ pkgs, crane, rustToolchain, workspaceDir }:
let
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

  apibara = {
    sdk = buildLib.buildCrate { crate = ../sdk; };
    starknet = buildLib.buildCrate { crate = ../starknet; };
    sink-webhook = buildLib.buildCrate { crate = ../sink-webhook; };
    sink-mongo = buildLib.buildCrate { crate = ../sink-mongo; };
    sink-postgres = buildLib.buildCrate { crate = ../sink-postgres; };
    sink-parquet = buildLib.buildCrate { crate = ../sink-parquet; };
  };
in
{
  checks = {
    inherit workspaceFmt workspaceClippy;
  };

  packages = {
    tests = workspaceTest;
    sdk = apibara.sdk.bin;
    starknet = apibara.starknet.bin;
    sink-webhook = apibara.sink-webhook.bin;
    sink-mongo = apibara.sink-mongo.bin;
    sink-postgres = apibara.sink-postgres.bin;
    sink-parquet = apibara.sink-parquet.bin;

    starknet-image = buildLib.dockerizeCrateBin {
      crate = apibara.starknet;
      ports = {
        "7171/tcp" = { };
      };
    };

    sink-webhook-image = buildLib.dockerizeCrateBin {
      crate = apibara.sink-webhook;
    };

    sink-mongo-image = buildLib.dockerizeCrateBin {
      crate = apibara.sink-mongo;
    };

    sink-postgres-image = buildLib.dockerizeCrateBin {
      crate = apibara.sink-postgres;
    };

    sink-parquet-image = buildLib.dockerizeCrateBin {
      crate = apibara.sink-parquet;
    };
  };
}
