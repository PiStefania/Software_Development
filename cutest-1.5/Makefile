CC = gcc -g
FILES = AllTests.c CuTest.c CuTestAuxMethods.c CuTestRadixHashJoin.c ../auxMethods.c ../radixHashJoin.c
OBJECTS = AllTests.o CuTest.o CuTestAuxMethods.o CuTestRadixHashJoin.o auxMethods.o radixHashJoin.o
OUT = test
HEADERS = CuTest.h CuTestAuxMethods.h CuTestRadixHashJoin.h ../auxMethods.h ../radixHashJoin.h

all: $(OBJECTS) $(HEADERS)
	$(CC) -o $(OUT) $(FILES) -lm
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

clean_some:
	rm -f $(OBJECTS)

clean:
	rm -f $(OBJECTS) $(OUT)
