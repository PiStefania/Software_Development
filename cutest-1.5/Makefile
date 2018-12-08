CC = gcc -g
FILESTEST = AllTests.c CuTest.c CuTestAuxMethods.c CuTestRadixHashJoin.c CuTestQueryMethods.c CuTestRelationMethods.c CuTestImplementation.c CuTestMetadataColumn.c
FILESMAIN = ../auxMethods.c ../radixHashJoin.c ../queryMethods.c ../relationMethods.c ../implementation.c ../metadataColumn.c
OBJECTSTEST = AllTests.o CuTest.o CuTestAuxMethods.o CuTestRadixHashJoin.o CuTestQueryMethods.o CuTestRelationMethods.o CuTestImplementation.o CuTestMetadataColumn.o
OBJECTSMAIN = auxMethods.o radixHashJoin.o queryMethods.o relationMethods.o implementation.o metadataColumn.o
OUT = test
HEADERSTEST = CuTest.h CuTestAuxMethods.h CuTestRadixHashJoin.h CuTestQueryMethods.h CuTestRelationMethods.h CuTestImplementation.h CuTestMetadataColumn.h
HEADERSMAIN = ../auxMethods.h ../radixHashJoin.h ../queryMethods.h ../relationMethods.h ../implementation.h ../metadataColumn.h 

all: $(OBJECTS) $(HEADERSTEST) $(HEADERSMAIN)
	$(CC) -o $(OUT) $(FILESTEST) $(FILESMAIN) -lm
	make clean_some

AllTests.o: AllTests.c
	$(CC) -c AllTests.c

CuTest.o: CuTest.c
	$(CC) -c CuTest.c

CuTestAuxMethods.o: CuTestAuxMethods.c
	$(CC) -c CuTestAuxMethods.c

CuTestRadixHashJoin.o: CuTestRadixHashJoin.c
	$(CC) -c CuTestRadixHashJoin.c

auxMethods.o: ../auxMethods.c
	$(CC) -c ../auxMethods.c

radixHashJoin.o: ../radixHashJoin.c
	$(CC) -c ../radixHashJoin.c

queryMethods.o: ../queryMethods.c
	$(CC) -c ../queryMethods.c

relationMethods.o: ../relationMethods.c
	$(CC) -c ../relationMethods.c

implementation.o: ../implementation.c
	$(CC) -c ../implementation.c

metadataColumn.o: ../metadataColumn.c
	$(CC) -c ../metadataColumn.c

clean_some:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN)

clean:
	rm -f $(OBJECTSTEST) $(OBJECTSMAIN) $(OUT)
