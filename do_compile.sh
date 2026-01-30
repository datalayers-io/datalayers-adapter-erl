#!/usr/bin/env bash

BUILD_SCRIPT="crates/datalayers-adapter-nif/build.rs"
NIF_PATH="priv/libdatalayers_nif.so"

if ./do_prebuilt.sh; then
  exit 0
else
  echo "No prebuilt artifacts, building from source"
fi

# touch the build.rs to force cargo to rerun build script and generate libpath file
touch "${BUILD_SCRIPT}"

cargo build --release

# Should always be `.so`, OTP on macos won't load `.dylib` files.
cp $(cat ./libpath) "${NIF_PATH}"

if [ "${BUILD_RELEASE:-}" = 1 ]; then
  PKGNAME="$(./pkgname.sh)"
  if [ -z "$PKGNAME" ]; then
    echo "unable_to_resolve_release_package_name"
    exit 1
  fi
  mkdir -p _packages
  TARGET="_packages/${PKGNAME}"
  gzip -c "${NIF_PATH}" > "$TARGET"
  # use openssl but not sha256sum command because in some macos env it does not exist
  if command -v openssl; then
    openssl dgst -sha256 "${TARGET}" | cut -d ' ' -f 2  > "${TARGET}.sha256"
  else
    sha256sum "${TARGET}"  | cut -d ' ' -f 1 > "${TARGET}.sha256"
  fi
fi
