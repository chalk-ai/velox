{ lib
, stdenv
, cmake
, ninja
, pkg-config
, python3

# Core dependencies
, boost
, folly
, fmt
, gflags
, glog
, double-conversion
, libevent
, libsodium
, openssl
, zlib
, bzip2
, lz4
, lzo
, zstd
, snappy
, re2
, protobuf
, simdjson
, fast-float
, xsimd
, icu
, thrift
, abseil-cpp
, nlohmann_json
, crc32c
, gtest
, geos
, backward-cpp
, libstemmer
, c-ares
, grpc

# Parser generators
, bison
, flex

# Arrow / Parquet
, arrow-cpp

# Cloud storage
, aws-sdk-cpp
, google-cloud-cpp
, azure-sdk-for-cpp

# HTTP
, curl
, proxygen ? null

# Optional
, pybind11 ? null
, doctest ? null

, libchalkGlobalFlags ? "-mavx2 -mfma -mavx -mf16c -mlzcnt -mbmi2"
}:

stdenv.mkDerivation rec {
  pname = "velox";
  version = "unstable-2025-03-26";

  src = lib.cleanSourceWith {
    src = lib.cleanSource ./.;
    filter = path: type:
      let baseName = baseNameOf (toString path);
      in !(baseName == "flake.nix"
        || baseName == "flake.lock"
        || baseName == "default.nix"
        || lib.hasSuffix ".nix" baseName);
  };

  nativeBuildInputs = [
    cmake
    ninja
    pkg-config
    python3
    bison
    flex
  ];

  buildInputs = [
    # Core
    boost
    folly
    fmt
    gflags
    glog
    double-conversion
    libevent
    libsodium
    openssl
    zlib
    bzip2
    lz4
    lzo
    zstd
    snappy
    re2
    protobuf
    simdjson
    fast-float
    xsimd
    icu
    thrift
    abseil-cpp
    nlohmann_json
    crc32c
    geos
    libstemmer
    c-ares
    grpc

    # Arrow / Parquet
    arrow-cpp

    backward-cpp

    # Cloud storage
    aws-sdk-cpp
    google-cloud-cpp
    azure-sdk-for-cpp.storage-files-datalake
    azure-sdk-for-cpp.identity

    # HTTP
    curl
  ]
  ++ lib.optional (proxygen != null) proxygen
  ++ lib.optional (pybind11 != null) pybind11
  ++ lib.optional (doctest != null) doctest;

  cmakeFlags = [
    # Dependency resolution
    "-DVELOX_DEPENDENCY_SOURCE=SYSTEM"

    # Testing / codegen / coverage
    "-DVELOX_BUILD_TESTING=OFF"
    "-DVELOX_CODEGEN_SUPPORT=OFF"
    "-DENABLE_TESTS=OFF"
    "-DENABLE_COVERAGE=OFF"
    "-DVELOX_ENABLE_EXAMPLES=OFF"

    # Core engine
    "-DVELOX_ENABLE_EXPRESSION=ON"
    "-DVELOX_ENABLE_EXEC=ON"
    "-DVELOX_ENABLE_AGGREGATES=ON"
    "-DVELOX_ENABLE_PARSE=ON"

    # Function sets
    "-DVELOX_ENABLE_PRESTO_FUNCTIONS=ON"
    "-DVELOX_ENABLE_SPARK_FUNCTIONS=ON"

    # File formats
    "-DVELOX_ENABLE_PARQUET=ON"
    "-DVELOX_ENABLE_ARROW=ON"

    # Connectors
    "-DVELOX_ENABLE_HIVE_CONNECTOR=ON"
    "-DVELOX_ENABLE_S3=ON"
    "-DVELOX_ENABLE_GCS=ON"
    "-DVELOX_ENABLE_ABFS=ON"
    "-DVELOX_ENABLE_HTTP=ON"

    # Explicitly disabled
    "-DVELOX_ENABLE_DUCKDB=OFF"
    "-DVELOX_ENABLE_SUBSTRAIT=OFF"
    "-DVELOX_ENABLE_TPCH_CONNECTOR=OFF"
    "-DVELOX_ENABLE_TPCDS_CONNECTOR=OFF"

    # Linkage / misc
    "-DVELOX_GFLAGS_TYPE=static"
    "-DWITH_ABSEIL=ON"
    "-DDOCTEST_WITH_MAIN_IN_STATIC_LIB=ON"
    "-DTARGET_SUPPORTS_SHARED_LIBS=ON"

    # Python / pybind
    "-DPYBIND11_FINDPYTHON=ON"
    "-DPython_VIRTUALENV=FIRST"
  ];

  preConfigure = ''
    export NIX_CFLAGS_COMPILE="$NIX_CFLAGS_COMPILE -DGLOG_USE_GLOG_EXPORT ${libchalkGlobalFlags}"

    # for dir in ${boost.dev}/lib/cmake/boost_*; do
    #   export CMAKE_PREFIX_PATH="$dir:$CMAKE_PREFIX_PATH"
    # done
  '';

  # TODO(aaronmondal): Might want this instead of the libchalkGlobalFlags in preConfigure.
  #
  # preBuild = ''
  #   export CPU_TARGET="avx2"
  # '';

  postPatch = ''
  substituteInPlace ./CMakeLists.txt \
      --replace-fail \
        "set(
    BOOST_INCLUDE_LIBRARIES
    atomic
    context
    date_time
    filesystem
    program_options
    regex
    system
    thread
  )" \
        "set(
    BOOST_INCLUDE_LIBRARIES
    atomic
    context
    date_time
    filesystem
    program_options
    regex
    thread
  )"

  # Relax versions
  sed -i 's/simdjson 3.9.3/simdjson/' CMakeLists.txt
  sed -i 's/google_cloud_cpp_storage CONFIG 2.37.0/google_cloud_cpp_storage CONFIG/' CMakeLists.txt
  sed -i 's/xsimd 10.0.0/xsimd/' CMakeLists.txt
  sed -i 's/fmt 9.0.0/fmt/' CMakeLists.txt
  sed -i 's/pybind11 2.10.0/pybind11/' CMakeLists.txt

  substituteInPlace velox/common/process/CMakeLists.txt \
    --replace-fail \
      "# BEGIN CHALK MODIFICATION
  # Fetch backward-cpp for stack trace support
  FetchContent_Declare(
    backward
    GIT_REPOSITORY https://github.com/bombela/backward-cpp
    GIT_TAG 0bfd0a07a61551413ccd2ab9a9099af3bad40681
    EXCLUDE_FROM_ALL)
  FetchContent_MakeAvailable(backward)" \
        "# BEGIN CHALK MODIFICATION
  # Stub target for backward-cpp
  if(NOT TARGET Backward::Interface)
    add_library(backward_interface INTERFACE)
    add_library(Backward::Interface ALIAS backward_interface)
  endif()"

  # Remove DuckDB-dependent QueryPlanner from parse module
  substituteInPlace velox/parse/CMakeLists.txt \
    --replace-fail \
      "velox_add_library(velox_parse_parser ExpressionsParser.cpp QueryPlanner.cpp)
  velox_link_libraries(velox_parse_parser velox_parse_expression velox_type velox_duckdb_parser)" \
      "velox_add_library(velox_parse_parser ExpressionsParser.cpp)
  velox_link_libraries(velox_parse_parser velox_parse_expression velox_type)"
  '';

  postInstall = ''
    # xxhash is vendored in-tree and not part of any CMake target,
    # so it has no install rules. Install it manually.
    mkdir -p $out/include/velox/external/xxhash
    cp $src/velox/external/xxhash/*.h $out/include/velox/external/xxhash/
  '';

  meta = with lib; {
    description = "A composable and fully extensible C++ execution engine library";
    homepage = "https://github.com/facebookincubator/velox";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
