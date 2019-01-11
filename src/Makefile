CC = gcc -Wall -O3 -g -pthread
FILES = main.c radixHashJoin.c queryMethods.c relationMethods.c rowIdArrayMethods.c statisticsMethods.c implementation.c threadPool.c
OBJECTS = main.o radixHashJoin.o queryMethods.o relationMethods.o rowIdArrayMethods.o statisticsMethods.o implementation.o threadPool.o
OUT = joinProgram

all: $(OBJECTS)
	$(CC) -o $(OUT) $(FILES) -lm
	make clean_some

main.o: main.c
	$(CC) -c main.c

radixHashJoin.o: radixHashJoin.c
	$(CC) -c radixHashJoin.c

queryMethods.o: queryMethods.c
	$(CC) -c queryMethods.c

relationMethods.o: relationMethods.c
	$(CC) -c relationMethods.c

rowIdArrayMethods.o: rowIdArrayMethods.c
	$(CC) -c rowIdArrayMethods.c

statisticsMethods.o: statisticsMethods.c
	$(CC) -c statisticsMethods.c

implementation.o: implementation.c
	$(CC) -c implementation.c

threadPool.o: threadPool.c
	$(CC) -c threadPool.c

clean_some:
	rm -f $(OBJECTS)

clean:
	rm -f $(OBJECTS) $(OUT)
