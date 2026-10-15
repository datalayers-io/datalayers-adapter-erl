#!/usr/bin/env bash
# Prints a stamp covering everything the native library is built from.
#
# `do_prebuilt.sh` uses it to notice that a cached `priv/libdatalayers_nif.so`
# predates the current sources. Reusing such a library loads a NIF whose exports
# no longer match the Erlang modules, which fails at `on_load` time. The stub
# module counts as a source here: it declares the NIF ABI the library is loaded
# against, so a stub-only change must invalidate the cached library too.
#
# Files that a clean checkout does not have (Cargo.lock is not tracked) are
# skipped: nothing here may fail the caller, because the prebuilt-package path
# runs this before it knows whether Rust is even installed.

set -euo pipefail

for file in \
    crates/datalayers-adapter-nif/src/*.rs \
    crates/datalayers-adapter-nif/Cargo.toml \
    crates/datalayers-adapter-nif/build.rs \
    src/datalayers_nif.erl \
    Cargo.toml \
    Cargo.lock; do
    if [ -f "${file}" ]; then
        cat "${file}"
    fi
done | cksum
