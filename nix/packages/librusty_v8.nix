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
  version = "0.71.2";

  src = fetch_librusty_v8 {
    version = "0.71.2";
    shas = {
      x86_64-linux = "sha256-QCqCJdNaOLXggIGQhLOV/NvbMebfA3g1iyiBtkPDS9A=";
      aarch64-linux = "sha256-r9/1086GQolGfc3iqIxZzTmoC7G/1vK4QG4Qvm0zUOo=";
      x86_64-darwin = "sha256-kMpB9F2sTxjqnplOznvv20fa7AhdnEeIb3yb4qa72e0=";
      aarch64-darwin = "sha256-jWjPJa3bWynoMUmccASggvu25NaHpn/AsTnC/JMyr8o=";
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
    description = "";
    homepage = "";
    license = licenses.mit;
    platforms = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
  };
}
