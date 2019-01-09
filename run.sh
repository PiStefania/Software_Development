#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="verySmall"
${DIR}/build/release/joinProgram -i ./workloads/"$DATA_DIR"/small.init -w ./workloads/"$DATA_DIR"/small.work
