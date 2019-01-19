#!/bin/bash

# compile program
echo "---------COMPILE PROGRAM---------"
./compile.sh

echo "---------RUN TEST SMALL FOLDER"
# run run.sh with time
echo "---------with time-------------"
./run.sh testSmall time	&>> statistics/testSmallLogs.txt

# run run.sh with valgrind --leak-check=full
echo "---------with leak check-------------"
./run.sh testSmall valgrind --leak-check=full &>> statistics/testSmallLogs.txt

# run run.sh with valgrind --tool=callgrind
echo "---------with callgrind-------------"
./run.sh testSmall valgrind --tool=callgrind --callgrind-out-file=statistics/callgrindTestSmall &>> statistics/testSmallLogs.txt

# run run.sh with valgrind --tool=massif
echo "---------with massif-------------"
./run.sh testSmall valgrind --tool=massif --massif-out-file=statistics/massifTestSmall &>> statistics/testSmallLogs.txt

echo "---------RUN VERY SMALL FOLDER"
# run run.sh with time
echo "---------with time-------------"
./run.sh verySmall time &>> statistics/verySmallLogs.txt

# run run.sh with valgrind --leak-check=full
echo "---------with leak check-------------"
./run.sh verySmall valgrind --leak-check=full &>> statistics/verySmallLogs.txt

# run run.sh with valgrind --tool=callgrind
echo "---------with callgrind-------------"
./run.sh verySmall valgrind --tool=callgrind --callgrind-out-file=statistics/callgrindVerySmall &>> statistics/verySmallLogs.txt

# run run.sh with valgrind --tool=massif
echo "---------with massif-------------"
./run.sh verySmall valgrind --tool=massif --massif-out-file=statistics/massifVerySmall &>> statistics/verySmallLogs.txt

echo "---------RUN TEST SMALL FOLDER"
# run run.sh with time
echo "---------with time-------------"
./run.sh small time &>> statistics/smallLogs.txt

# run run.sh with valgrind --leak-check=full
echo "---------with leak check-------------"
./run.sh small valgrind --leak-check=full &>> statistics/smallLogs.txt

# run run.sh with valgrind --tool=callgrind
echo "---------with callgrind-------------"
./run.sh small valgrind --tool=callgrind --callgrind-out-file=statistics/callgrindSmall &>> statistics/smallLogs.txt

# run run.sh with valgrind --tool=massif
echo "---------with massif-------------"
./run.sh small valgrind --tool=massif --massif-out-file=statistics/massifSmall &>> statistics/smallLogs.txt

# run harness
echo "---------RUN HARNESS FOR ALL---------"
./runTestharness.sh workloads/testSmall &>> statistics/HarnessLogs.txt
./runTestharness.sh workloads/verySmall &>> statistics/HarnessLogs.txt
./runTestharness.sh workloads/small &>> statistics/HarnessLogs.txt
