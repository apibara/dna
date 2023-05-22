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

  v8 = makeOverride {
    name = "v8";
    overrideAttrs = old: {
      naviteBuildInputs = old.buildInputs ++ [
        final.librusty_v8
      ];
      RUSTY_V8_ARCHIVE = "${final.librusty_v8}/lib/librusty_v8.a";
      # The build.rs script outputs the default path as the linker path.
      # We need to override it to point to the correct path.
      patchPhase = ''
        substituteInPlace build.rs \
        --replace \
        'println!("cargo:rustc-link-search={}", dir.display());' \
        'println!("cargo:rustc-link-search=${final.librusty_v8}/lib");'
      '';
    };
  };

  overrides = final.rustBuilder.overrides.all ++ [
    mdbx-sys
    v8
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

  # rusty_v8 downloads a prebuilt v8 from github, so we need to prefetch it
  # and pass it to the builder.
  librusty_v8 = prev.callPackage ./packages/librusty_v8.nix { };
}
