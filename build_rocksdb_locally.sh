#!/bin/bash -e

if [[ ! -d "rust-rocksdb" ]]; then
    git clone git@github.com:rust-rocksdb/rust-rocksdb.git
fi

cd ./rust-rocksdb

unset ROCKSDB_LIB_DIR
unset ROCKSDB_STATIC

local_rocksdb_archive="$PWD/librocksdb.a"

if [[ ! -f "$local_rocksdb_archive" ]]; then
    # TODO: pass features as args
    cargo clean
    cargo build --release --no-default-features --features snappy

    found_rocksdb_archive="$(find ./target/ -type f -name librocksdb.a)"
    cp "$found_rocksdb_archive" "$local_rocksdb_archive"
fi


echo "export ROCKSDB_LIB_DIR=\"$(readlink -f $PWD)\""
echo "export ROCKSDB_STATIC=1"
