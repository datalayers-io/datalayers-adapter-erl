#!/usr/bin/env bash

set -euo pipefail

NIF_PATH="priv/libdatalayers_nif.so"
STAMP_PATH="priv/.nif-source-stamp"

if [ "${BUILD_RELEASE:-}" = 1 -o "${FORCE_BUILD:-}" = 1 ]; then
  # never download when building a new release
  exit 1
fi

if [ ! -f "${NIF_PATH}" ]; then
  PKGNAME="$(./pkgname.sh)"
  if [ -n "$PKGNAME" ]; then
    if ./download.sh "$PKGNAME"; then
      ./nif-source-stamp.sh > "${STAMP_PATH}"
      exit 0
    else
      exit 1
    fi
  fi
  exit 2
elif [ -f "${STAMP_PATH}" ] && [ "$(cat "${STAMP_PATH}")" = "$(./nif-source-stamp.sh)" ]; then
  echo "nif ${NIF_PATH} is up to date; not recompiling"
  echo "run with FORCE_BUILD=1 to force recompilation"
else
  echo "nif ${NIF_PATH} was built from different sources; rebuilding"
  exit 2
fi
