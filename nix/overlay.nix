final: prev:

let
  makeOverride = final.rustBuilder.rustLib.makeOverride;

  mdbx-sys = makeOverride {
    name = "mdbx-sys";
    overrideAttrs = old: {
      nativeBuildInputs = old.nativeBuildInputs ++ [
        final.clang
        final.pkg-config
        final.llvmPackages.libclang
      ];
      LIBCLANG_PATH = final.lib.makeLibraryPath [ final.llvmPackages.libclang.lib ];
    };
  };

  overrides = final.rustBuilder.overrides.all ++ [
    mdbx-sys
  ];
in
{
  rustVersion = prev.rust-bin.stable.latest.default.override {
    extensions = [ "rust-src" ];
  };

  rustPlatform = prev.makeRustPlatform {
    cargo = final.rustVersion;
    rustc = final.rustVersion;
  };

  apibaraBuilder = {
    inherit overrides;
  };
}
