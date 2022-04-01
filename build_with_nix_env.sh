#!/bin/bash

export ROCKSDB_PATH=$(nix path-info nixpkgs#rocksdb)
export ROCKSDB_INCLUDE_DIR="$ROCKSDB_PATH/include"
export ROCKSDB_LIB_DIR="$ROCKSDB_PATH/lib"
export ROCKSDB_STATIC="1"

