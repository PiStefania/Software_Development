#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "auxMethods.h"
#include "radixHashJoin.h"

#define RANGE 17							// Range of values for arrays initialization (1 to RANGE)
#define R_ROWS 5							// Number of rows for R array
#define S_ROWS 3							// Number of rows for S array
#define COLUMNS 2							// Colums of arrays (for this program, only 2)


int main(int argc, char* argv[]){
	printf("START OF MAIN PROGRAM\n");
	// Check the first hash functionality
	int32_t out = hashFunction1(148);
	printf("HASH: %d\n",out);

	// Create randomly filled arrays
	srand(time(NULL));
	int32_t** R = malloc(R_ROWS * sizeof(int*));
	for (int i = 0; i < R_ROWS; i++) {
		R[i] = malloc(COLUMNS * sizeof(int));
	}
  for (int i = 0; i < R_ROWS; i++) {
    for (int j = 0; j < COLUMNS; j++) {
      R[i][j] = (rand() % RANGE) + 1;
    }
  }
	/*
	R[0][0] = 1;
	R[0][1] = 3;
	R[1][0] = 1;
	R[1][1] = 4;
	R[2][0] = 2;
	R[2][1] = 5;
	*/
	int32_t** S = malloc(S_ROWS * sizeof(int*));
	for (int i = 0; i < S_ROWS; i++) {
		S[i] = malloc(COLUMNS * sizeof(int));
	}
	for (int i = 0; i < S_ROWS; i++) {
    for (int j = 0; j < COLUMNS; j++) {
      S[i][j] = (rand() % RANGE) + 1;
    }
  }
	/*
	S[0][0] = 1;
	S[0][1] = 3;
	S[1][0] = 2;
	S[1][1] = 3;
	*/

	//get col
	int* column = getColumnOfArray(R, R_ROWS, 0);
	printf("COL\n");
	for (int i = 0; i < R_ROWS; i++) {
		printf("elem: %d\n",column[i]);
	}

	//create relation
	printf("---RELATION FROM 1 FIELD---\n");
	relation* rel = createRelation(column, R_ROWS);
	printRelation(rel);

	//create relation with buckets
	printf("---RELATION WITH BUCKETS---\n");
	relation* RNotOrdered = createBucketsRelation(rel);
	printRelation(RNotOrdered);

	//create histogram
	printf("---HIST---\n");
	relation* Hist = createHistogram(RNotOrdered);
	printRelation(Hist);

	//create Psum
	printf("---PSUM---\n");
	relation* Psum = createPsum(Hist);
	printRelation(Psum);

	//create ordered R
	printf("---RORDERED---\n");
	relation* ROrdered = createROrdered(RNotOrdered, Hist, Psum);
	printRelation(ROrdered);

	// Delete all structure created by allocating memory dynamically
	for (int i = 0; i < R_ROWS; i++) {
		free(R[i]);
	}
	free(R);
	for (int i = 0; i < S_ROWS; i++) {
		free(S[i]);
	}
	free(S);
	free(column);
	deleteRelation(&rel);
	deleteRelation(&RNotOrdered);
	deleteRelation(&Hist);
	deleteRelation(&Psum);
	deleteRelation(&ROrdered);

	return 0;
}
