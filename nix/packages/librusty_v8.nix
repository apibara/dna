{ rust, stdenv, lib, fetchurl }:

let
  arch = rust.toRustTarget stdenv.hostPlatform;
  fetch_librusty_v8 = args: fetchurl {
    name = "librusty_v8-${args.version}";
    url = "https://github.com/denoland/rusty_v8/releases/download/v${args.version}/librusty_v8_release_${arch}.a";
    sha256 = args.shas.${stdenv.hostPlatform.system};
    meta = { inherit (args) version; };
  };
in
stdenv.mkDerivation rec {
  pname = "librusty_v8";
  version = "0.81.0";

  src = fetch_librusty_v8 {
    inherit version;
    shas = {
      x86_64-linux = "sha256-e77LYm/sus7EY4eiRuEp6G25djDaT4wSD4FBCxy4vcE=";
      aarch64-linux = "sha256-wPfUcuT2Z2sy5nLf8xR3QjGQKk6OsM/45jnYv/Hw+Zs=";
      x86_64-darwin = "sha256-UbnRiywM7b7q3rITZzNeWAuKU+HXXAqVapQ9j5ND6go=";
      aarch64-darwin = "sha256-42d3VGBv5lW1InfzYfWr6Xj0GpyJ6GWswVNtUa8ID30=";
    };
  };

  dontUnpack = true;

  installPhase = ''
    mkdir -p $out/lib
    cp $src $out/lib/librusty_v8.a

    mkdir -p $out/lib/pkgconfig
    cat > $out/lib/pkgconfig/rusty_v8.pc << EOF
    Name: rusty_v8
    Description: V8 JavaScript Engine
    Version: ${version}
    Libs: -L $out/lib
    EOF
  '';

  meta = with lib; {
    description = "Rust bindings for the V8 JavaScript engine";
    homepage = "https://crates.io/crates/v8";
    license = licenses.mit;
    platforms = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
  };
}
