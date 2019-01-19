#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="small"
valgrind --leak-check=full ${DIR}/build/release/joinProgram -i ./workloads/"$DATA_DIR"/small.init -w ./workloads/"$DATA_DIR"/small.work
