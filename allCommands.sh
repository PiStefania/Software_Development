#!/bin/bash

# compile program
echo "---------COMPILE PROGRAM---------"
./compile.sh
mkdir statistics

# run run.sh with valgrind --tool=callgrind
echo "---------with callgrind-------------"
./run.sh small valgrind --tool=callgrind --callgrind-out-file=statistics/callgrindSmall &>> statistics/smallLogs.txt

# run run.sh with valgrind --tool=massif
echo "---------with massif-------------"
./run.sh small valgrind --tool=massif --massif-out-file=statistics/massifSmall &>> statistics/smallLogs.txt

# run harness
echo "---------RUN HARNESS FOR ALL---------"
./runTestharness.sh workloads/small &>> statistics/HarnessLogs.txt
