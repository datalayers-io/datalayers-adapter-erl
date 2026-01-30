#!/usr/bin/env bash

set -euo pipefail

NIF_PATH="priv/libdatalayers_nif.so"

if [ "${BUILD_RELEASE:-}" = 1 -o "${FORCE_BUILD:-}" = 1 ]; then
  # never download when building a new release
  exit 1
fi

if [ ! -f "${NIF_PATH}" ]; then
  PKGNAME="$(./pkgname.sh)"
  if [ -n "$PKGNAME" ]; then
    if ./download.sh "$PKGNAME"; then
      exit 0
    else
      exit 1
    fi
  fi
  exit 2
else
  echo "nif ${NIF_PATH} already exists; not recompiling"
  echo "run with FORCE_BUILD=1 to force recompilation"
fi
