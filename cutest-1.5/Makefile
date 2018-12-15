CC = gcc -g
FILESTEST = AllTests.c CuTest.c CuTestRadixHashJoin.c CuTestQueryMethods.c CuTestRelationMethods.c CuTestImplementation.c
FILESMAIN = ../radixHashJoin.c ../queryMethods.c ../relationMethods.c ../implementation.c
OBJECTSTEST = AllTests.o CuTest.o CuTestRadixHashJoin.o CuTestQueryMethods.o CuTestRelationMethods.o CuTestImplementation.o
OBJECTSMAIN = radixHashJoin.o queryMethods.o relationMethods.o implementation.o
OUT = test
HEADERSTEST = CuTest.h CuTestRadixHashJoin.h CuTestQueryMethods.h CuTestRelationMethods.h CuTestImplementation.h
HEADERSMAIN = ../radixHashJoin.h ../queryMethods.h ../relationMethods.h ../implementation.h

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

radixHashJoin.o: ../radixHashJoin.c
	$(CC) -c ../radixHashJoin.c

queryMethods.o: ../queryMethods.c
	$(CC) -c ../queryMethods.c

relationMethods.o: ../relationMethods.c
	$(CC) -c ../relationMethods.c

implementation.o: ../implementation.c
	$(CC) -c ../implementation.c

clean_some:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN)

clean:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN) $(OUT)
