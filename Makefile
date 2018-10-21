CC = gcc -g
FILES = main.c radixHashJoin.c auxMethods.c
OBJECTS = main.o radixHashJoin.o auxMethods.o
OUT = joinProgram
HEADERS = radixHashJoin.h auxMethods.h

all: $(OBJECTS) $(HEADERS)
	$(CC) -o $(OUT) $(FILES)
	make clean

main.o: main.c
	$(CC) -c main.c
	
radixHashJoin.o: radixHashJoin.c
	$(CC) -c radixHashJoin.c
	
auxMethods.o: auxMethods.c
	$(CC) -c auxMethods.c
	
clean:
	rm -f $(OBJECTS)