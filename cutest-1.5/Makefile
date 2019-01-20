CC = gcc -pthread -g
FILESTEST = AllTests.c CuTest.c CuTestRadixHashJoin.c CuTestQueryMethods.c CuTestRelationMethods.c CuTestImplementation.c CuTestRowIdArrayMethods.c
FILESMAIN = ../src/radixHashJoin.c ../src/queryMethods.c ../src/relationMethods.c ../src/implementation.c ../src/rowIdArrayMethods.c ../src/statisticsMethods.c ../src/threadPool.c
OBJECTSTEST = AllTests.o CuTest.o CuTestRadixHashJoin.o CuTestQueryMethods.o CuTestRelationMethods.o CuTestImplementation.o CuTestRowIdArrayMethods.o
OBJECTSMAIN = radixHashJoin.o queryMethods.o relationMethods.o implementation.o rowIdArrayMethods.o statisticsMethods.o threadPool.o
OUT = test
HEADERSTEST = CuTest.h CuTestRadixHashJoin.h CuTestQueryMethods.h CuTestRelationMethods.h CuTestImplementation.h CuTestRowIdArrayMethods.h
HEADERSMAIN = ../include/radixHashJoin.h ../include/queryMethods.h ../include/relationMethods.h ../include/implementation.h ../include/rowIdArrayMethods.h ../include/statisticsMethods.h ../include/threadPool.h

all: $(OBJECTS) $(HEADERSTEST) $(HEADERSMAIN)
	$(CC) -o $(OUT) $(FILESTEST) $(FILESMAIN) -lm
	make clean_some

AllTests.o: AllTests.c
	$(CC) -c AllTests.c

CuTest.o: CuTest.c
	$(CC) -c CuTest.c

CuTestRadixHashJoin.o: CuTestRadixHashJoin.c
	$(CC) -c CuTestRadixHashJoin.c

CuTestQueryMethods.o: CuTestQueryMethods.c
	$(CC) -c CuTestQueryMethods.c

CuTestRelationMethods.o: CuTestRelationMethods.c
	$(CC) -c CuTestRelationMethods.c

CuTestImplementation.o: CuTestImplementation.c
	$(CC) -c CuTestImplementation.c

CuTestRowIdArrayMethods.o: CuTestRowIdArrayMethods.c
	$(CC) -c CuTestRowIdArrayMethods.c

radixHashJoin.o: ../src/radixHashJoin.c
	$(CC) -c ../src/radixHashJoin.c

queryMethods.o: ../src/queryMethods.c
	$(CC) -c ../src/queryMethods.c

relationMethods.o: ../src/relationMethods.c
	$(CC) -c ../src/relationMethods.c

implementation.o: ../src/implementation.c
	$(CC) -c ../src/implementation.c

rowIdArrayMethods.o: ../src/rowIdArrayMethods.c
	$(CC) -c ../src/rowIdArrayMethods.c

statisticsMethods.o: ../src/statisticsMethods.c
	$(CC) -c ../src/statisticsMethods.c

threadPool.o: ../src/threadPool.c
	$(CC) -c ../src/threadPool.c

clean_some:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN)

clean:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN) $(OUT)
