#!/bin/bash -e

if [[ ! -d "rust-rocksdb" ]]; then
    git clone git@github.com:rust-rocksdb/rust-rocksdb.git
fi

cd ./rust-rocksdb

unset ROCKSDB_LIB_DIR
unset ROCKSDB_STATIC

# TODO: pass features as args
cargo clean
cargo build --release --no-default-features --features snappy

# TODO: copy the librocksdb.a to the root directory
rocksdb_archive="$(find ./target/ -type f -name librocksdb.a)"
rocksdb_lib_dir="$(dirname $rocksdb_archive)"

echo "export ROCKSDB_LIB_DIR=\"$(readlink -f $(dirname $rocksdb_archive))\""
echo "export ROCKSDB_STATIC=1"
