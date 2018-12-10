CC = gcc -Wall -g
FILES = main.c radixHashJoin.c auxMethods.c queryMethods.c relationMethods.c implementation.c 
OBJECTS = main.o radixHashJoin.o auxMethods.o queryMethods.o relationMethods.o implementation.o 
OUT = joinProgram
HEADERS = radixHashJoin.h auxMethods.h queryMethods.h relationMethods.h implementation.h 

all: $(OBJECTS) $(HEADERS)
	$(CC) -o $(OUT) $(FILES)
	make clean_some

main.o: main.c
	$(CC) -c main.c

radixHashJoin.o: radixHashJoin.c
	$(CC) -c radixHashJoin.c

auxMethods.o: auxMethods.c
	$(CC) -c auxMethods.c

queryMethods.o: queryMethods.c
	$(CC) -c queryMethods.c

relationMethods.o: relationMethods.c
	$(CC) -c relationMethods.c

implementation.o: implementation.c
	$(CC) -c implementation.c

clean_some:
	rm -f $(OBJECTS)

clean:
	rm -f $(OBJECTS) $(OUT)
