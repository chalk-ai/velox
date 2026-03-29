{
  description = "velox flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ ];
      };
    in
    {
      devShells.${system}.default =
        pkgs.mkShell.override
          {
            # TODO(aaronmondal): GCC 15 breaks due to stricter include handling. We
            # should patch failures rather than regressing the compiler here.
            stdenv = pkgs.gcc14Stdenv;
          }
          {
            nativeBuildInputs = with pkgs; [
              nixd
              nixfmt
              starpls
              bazel-buildtools

              bazel

              # TODO
              cmake
              vcpkg

              autoconf
              autoconf-archive
              automake
              gnumake
              gettext
              gperf
              libtool
              m4
              ninja
              pkg-config
              zip
              flex
              bison

              python313

              # Rust side
              cargo
              rustc
              openssl
              protobuf
              cyrus_sasl

              lld
              doppler

              # For engine
              uv
              postgresql.pg_config
              zstd

              arrow-cpp
            ];

            # So that vcpkg can resolve pkg-config properly.
            shellHook = ''
              echo "${pkgs.arrow-cpp}"
              # export CC=clang
              # export CXX=clang++
              # export USE_CLANG=true
              export LD="${pkgs.lld}/bin/lld"
              export PKG_CONFIG="${pkgs.pkg-config}/bin/pkg-config"
              export LD_LIBRARY_PATH="${pkgs.gcc14Stdenv.cc.cc.lib}/lib:${pkgs.python3}/lib"

              # Only for a Python issue with gcc14
              export CFLAGS="-Wno-error=incompatible-pointer-types $CFLAGS"
            '';
          };

      packages.${system} = {
        # TODO
        velox = pkgs.callPackage ./default.nix { };
      };
    };
}

# cd libchalk

# mkdir build

# cd build

# VCPKG_ROOT=/home/aaronmondal/chalk-ai/vcpkg cmake -DCMAKE_MAKE_PROGRAM=/nix/store/45npani18v2m7sbkrzrv2xyilyghrny9-gnumake-4.4.1/bin/make -DCMAKE_C_COMPILER=/nix/store/kisrcr13ayli6nqh99r4kwzbsqrcd8xs-gcc-wrapper-14.3.0/bin/gcc -DCMAKE_CXX_COMPILER=/nix/store/kisrcr13ayli6nqh99r4kwzbsqrcd8xs-gcc-wrapper-14.3.0/bin/g++ -DVCPKG_OVERLAY_PORTS=/home/aaronmondal/chalk-ai/chalk-private/libchalk/ports ..

# cmake --build . -j$(nproc)

# SITE_PACKAGES=$(python -c "import site; print(site.getsitepackages()[0])")

# cp libchalk.cpython-*.so "$SITE_PACKAGES/"
# cp libonnxruntime* "$SITE_PACKAGES/"
# cp libchalk_onnx_module.so "$SITE_PACKAGES/" 2>/dev/null || true
