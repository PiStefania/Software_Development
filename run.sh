#!/bin/bash

args=("$@")
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
DATA_DIR="${args[0]}"
statistic="${args[1]}"
statisticTool="${args[2]}"
logFile="${args[3]}"


#$statistic $statisticTool $logFile ${DIR}/build/release/joinProgram -i ./workloads/"$DATA_DIR"/small.init -w ./workloads/"$DATA_DIR"/small.work

DATA_DIR="small"
${DIR}/build/release/joinProgram -i ./workloads/"$DATA_DIR"/small.init -w ./workloads/"$DATA_DIR"/small.work

#$statistic $statisticTool $logFile ${DIR}/build/release/joinProgram -i ./workloads/"$DATA_DIR"/small.init -w ./workloads/"$DATA_DIR"/small.work

