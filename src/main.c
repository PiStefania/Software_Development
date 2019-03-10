#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include "../include/radixHashJoin.h"
#include "../include/relationMethods.h"
#include "../include/implementation.h"
#include "../include/threadPool.h"


int main(int argc, char* argv[]){
	// Create thread Pool
	threadPool* thPool = initializeThreadPool(THREADS);
	if (PRINT) printf("------------------------------------------------------\n");
	if (PRINT) printf("START OF MAIN PROGRAM\n");
	// Check arguments
	char* init = NULL;
	char* work = NULL;

	for (int i=0; i<argc; i++) {
		if (strcmp(argv[i],"-i") == 0)
			init = argv[i+1];
		if (strcmp(argv[i],"-w") == 0)
			work = argv[i+1];
	}

	// Read binary data from init file
	FILE* initFile = NULL;
	if (init != NULL) {
		initFile = fopen(init, "r");
	}
	relationsInfo* initRelations = NULL;
	int num_of_initRelations = 0;
	initRelations = getRelationsData(initFile, init, &num_of_initRelations);
	if (initRelations == NULL) {
		printf("INIT FAILED\n");
		deleteRelationsData(initRelations, &num_of_initRelations);
		// Delete threadPool
		destroyThreadPool(&thPool);
		return -1;
	}

	if (PRINT) printf("--------------------------------------------------\n");
	if (PRINT) printf("   Reading data files done! Now read queries!!\n");
	if (PRINT) printf("--------------------------------------------------\n");

	// Wait 1 sec
	sleep(1);

	// Get query lines
	FILE* workFile = NULL;
	if(work != NULL){
		workFile = fopen(work,"r");
	}
	int query = queriesImplementation(workFile, initRelations, num_of_initRelations, thPool);
	if (!query) printf(" WORK FAILED\n");

	deleteRelationsData(initRelations, &num_of_initRelations);
	// Delete threadPool
	destroyThreadPool(&thPool);
	return 0;
}
