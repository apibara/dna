{ pkgs, crane, workspaceDir, crates, crossSystem }:
let
  rustToolchain = pkgs.pkgsBuildHost.rust-bin.stable.latest.default.override {
    targets = [ "aarch64-unknown-linux-gnu" ];
  };

  buildLib = import ./build.nix {
    inherit pkgs crane rustToolchain workspaceDir;
    extraBuildArgs = {
      CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER = "/kkkk"; # "${pkgs.stdenv.cc}/bin/${pkgs.stdenv.cc.targetPrefix}cc";
      # RUSTFLAGS = "-v -C linker=${pkgs.stdenv.cc}/bin/${pkgs.stdenv.cc.targetPrefix}cc";
      HOST_CC = "${pkgs.stdenv.cc.nativePrefix}cc";
      CARGO_BUILD_TARGET = "aarch64-unknown-linux-gnu";
      # Set C flags for Rust's bindgen program. Unlike ordinary C compilation,
      # bindgen does not invoke $CC directly. Instead it uses LLVM's libclang. To
      # make sure all necessary flags are included we need to look in a few
      # places.
      preBuild =
        let
          inherit (pkgs) stdenv lib;
        in
        ''
          export BINDGEN_EXTRA_CLANG_ARGS="
            $(< ${stdenv.cc}/nix-support/libc-crt1-cflags) \
            $(< ${stdenv.cc}/nix-support/libc-cflags) \
            $(< ${stdenv.cc}/nix-support/cc-cflags) \
            $(< ${stdenv.cc}/nix-support/libcxx-cxxflags) \
            ${lib.optionalString stdenv.cc.isClang "-idirafter ${stdenv.cc.cc}/lib/clang/${lib.getVersion stdenv.cc.cc}/include"} \
            ${lib.optionalString stdenv.cc.isGNU "-isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc} -isystem ${stdenv.cc.cc}/include/c++/${lib.getVersion stdenv.cc.cc}/${stdenv.hostPlatform.config} -idirafter ${stdenv.cc.cc}/lib/gcc/${stdenv.hostPlatform.config}/${lib.getVersion stdenv.cc.cc}/include"}
          "
        '';
    };
  };
  built = (buildLib.buildCrates crates).packages;
  packages =
    let
      names = builtins.attrNames built;
      packages = map (name: { name = "${name}-cross"; value = built.${name}; }) names;
    in
    (builtins.listToAttrs packages);
in
{
  inherit packages;
}
