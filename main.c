#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "auxMethods.h"
#include "radixHashJoin.h"

#define RANGE 17							// Range of values for arrays initialization (1 to RANGE)
#define R_ROWS 5							// Number of rows for R array
#define S_ROWS 3							// Number of rows for S array
#define COLUMNS 1							// Colums of arrays (for this program, only 1)


int main(int argc, char* argv[]){
	printf("------------------------------------------------------\n");
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

	// Get the requested column (for this implementation we need just one)
	// That's why COLUMNS is set to 1 above (R, S have only one field, e.g: a)
	int* Rcolumn = getColumnOfArray(R, R_ROWS, 0);
	printf("R_COL\n");
	for (int i = 0; i < R_ROWS; i++) {
		printf("Relem: %d\n",Rcolumn[i]);
	}

	//create relation
	printf("---RELATION FROM 1 FIELD - R---\n");
	relation* Rrel = createRelation(Rcolumn, R_ROWS);
	printRelation(Rrel);

	//create relation with buckets
	printf("---RELATION WITH BUCKETS - R---\n");
	relation* RNotOrdered = createBucketsRelation(Rrel);
	printRelation(RNotOrdered);

	//create histogram
	printf("---HIST - R---\n");
	relation* RHist = createHistogram(RNotOrdered);
	printRelation(RHist);

	//create Psum
	printf("---PSUM - R---\n");
	relation* RPsum = createPsum(RHist);
	printRelation(RPsum);

	//create ordered R
	printf("---REORDERED - R---\n");
	relation* ROrdered = createROrdered(RNotOrdered, RHist, RPsum);
	printRelation(ROrdered);

	printf("------------------------------------------------------\n");

	// Now the same procedure for S array
	int* Scolumn = getColumnOfArray(S, S_ROWS, 0);
	printf("S_COL\n");
	for (int i = 0; i < S_ROWS; i++) {
		printf("Selem: %d\n", Scolumn[i]);
	}

	//create relation
	printf("---RELATION FROM 1 FIELD - S---\n");
	relation* Srel = createRelation(Scolumn, S_ROWS);
	printRelation(Srel);

	//create relation with buckets
	printf("---RELATION WITH BUCKETS - S---\n");
	relation* SNotOrdered = createBucketsRelation(Srel);
	printRelation(SNotOrdered);

	//create histogram
	printf("---HIST - S---\n");
	relation* SHist = createHistogram(SNotOrdered);
	printRelation(SHist);

	//create Psum
	printf("---PSUM - S---\n");
	relation* SPsum = createPsum(SHist);
	printRelation(SPsum);

	//create ordered R
	printf("---REORDERED - S---\n");
	relation* SOrdered = createROrdered(SNotOrdered, SHist, SPsum);
	printRelation(SOrdered);


	// Delete all structure created by allocating memory dynamically
	for (int i = 0; i < R_ROWS; i++) {
		free(R[i]);
	}
	free(R);
	for (int i = 0; i < S_ROWS; i++) {
		free(S[i]);
	}
	free(S);

	free(Rcolumn);
	deleteRelation(&Rrel);
	deleteRelation(&RNotOrdered);
	deleteRelation(&RHist);
	deleteRelation(&RPsum);
	deleteRelation(&ROrdered);

	free(Scolumn);
	deleteRelation(&Srel);
	deleteRelation(&SNotOrdered);
	deleteRelation(&SHist);
	deleteRelation(&SPsum);
	deleteRelation(&SOrdered);

	printf("------------------------------------------------------\n");

	return 0;
}
