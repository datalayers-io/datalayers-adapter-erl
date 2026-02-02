#!/bin/sh

set -eu

PKGNAME="${1:-$(./pkgname.sh)}"
TAG="$(git describe --tags | head -1)"
REPO="${REPO:-datalayers-io/datalayers-adapter-erl}"
URL="https://github.com/${REPO}/releases/download/$TAG/$PKGNAME"
NIF_SO_NAME="libdatalayers_nif"
NIF_PATH="priv/${NIF_SO_NAME}.so"
ERL_NIF_MODULE="datalayers_nif"
ERL_NIF_MODULE_SRC="src/${ERL_NIF_MODULE}.erl"

mkdir -p _packages
echo "Attempting to download pre-built package from ${URL}"
if [ ! -f "_packages/${PKGNAME}" ]; then
    curl -f -L --no-progress-meter -o "_packages/${PKGNAME}" "${URL}"
fi

if [ ! -f "_packages/${PKGNAME}.sha256" ]; then
    curl -f -L --no-progress-meter -o "_packages/${PKGNAME}.sha256" "${URL}.sha256"
fi

if [ "$(uname -s)" = "Darwin" ]; then
    # macOS
    echo "$(cat "_packages/${PKGNAME}.sha256")  _packages/${PKGNAME}" | shasum -a 256 -c || exit 1
else
    # Linux and other Unix-like systems
    echo "$(cat "_packages/${PKGNAME}.sha256")  _packages/${PKGNAME}" | sha256sum -c || exit 1
fi

mkdir -p priv
gzip -c -d "_packages/${PKGNAME}" > "${NIF_PATH}"

# Sanity check
erlc "${ERL_NIF_MODULE_SRC}"
trap "rm -f ${ERL_NIF_MODULE}.beam" EXIT
if erl -noshell -eval "[_|_]=${ERL_NIF_MODULE}:module_info(), halt(0)"; then
    echo "sanity check ok"
    exit 0
else
    echo "sanity check failed"
    exit 1
fi
