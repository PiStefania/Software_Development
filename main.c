#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "auxMethods.h"
#include "radixHashJoin.h"


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

	int32_t** S = malloc(S_ROWS * sizeof(int*));
	for (int i = 0; i < S_ROWS; i++) {
		S[i] = malloc(COLUMNS * sizeof(int));
	}
	for (int i = 0; i < S_ROWS; i++) {
	    for (int j = 0; j < COLUMNS; j++) {
	      	S[i][j] = (rand() % RANGE) + 1;
	    }
  	}

	// Get the requested column (for this implementation we need just one)
	// That's why COLUMNS is set to 1 above (R, S have only one field, e.g: a)
	int* Rcolumn = getColumnOfArray(R, R_ROWS, 0);
	if (PRINT) {
		printf("R_COL\n");
		for (int i = 0; i < R_ROWS; i++) {
			printf("Relem: %d\n",Rcolumn[i]);
		}
	}

	//create relation
	printf("---RELATION FROM 1 FIELD - R---\n");
	relation* Rrel = createRelation(Rcolumn, R_ROWS);
	if (PRINT) printRelation(Rrel);

	//create histogram
	printf("---HIST - R---\n");
	relation* RHist = createHistogram(Rrel);
	if (PRINT) printRelation(RHist);

	//create Psum
	printf("---PSUM - R---\n");
	relation* RPsum = createPsum(RHist);
	if (PRINT) printRelation(RPsum);

	//create ordered R
	printf("---REORDERED - R---\n");
	relation* ROrdered = createROrdered(Rrel, RHist, RPsum);
	if (PRINT) printRelation(ROrdered);

	printf("------------------------------------------------------\n");

	// Now the same procedure for S array
	int* Scolumn = getColumnOfArray(S, S_ROWS, 0);
	if (PRINT) {
		printf("S_COL\n");
		for (int i = 0; i < S_ROWS; i++) {
			printf("Selem: %d\n", Scolumn[i]);
		}
	}

	//create relation
	printf("---RELATION FROM 1 FIELD - S---\n");
	relation* Srel = createRelation(Scolumn, S_ROWS);
	if (PRINT) printRelation(Srel);

	//create histogram
	printf("---HIST - S---\n");
	relation* SHist = createHistogram(Srel);
	if (PRINT) printRelation(SHist);

	//create Psum
	printf("---PSUM - S---\n");
	relation* SPsum = createPsum(SHist);
	if (PRINT) printRelation(SPsum);

	//create ordered R
	printf("---REORDERED - S---\n");
	relation* SOrdered = createROrdered(Srel, SHist, SPsum);
	if (PRINT) printRelation(SOrdered);


	// Create the list of joined values
	printf("---Create List---\n");
	result* ResultList = createList();
	// Index (in the smallest bucket of the 2 arrays for each hash1 value), compare and join by bucket
	if (indexCompareJoin(ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum)) {
		printf("Error\n");
	}
	if (PRINT) printList(ResultList);
	printf("---Delete List---\n");
	deleteList(&ResultList);


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
	deleteRelation(&RHist);
	deleteRelation(&RPsum);
	deleteRelation(&ROrdered);

	free(Scolumn);
	deleteRelation(&Srel);
	deleteRelation(&SHist);
	deleteRelation(&SPsum);
	deleteRelation(&SOrdered);

	printf("------------------------------------------------------\n");

	return 0;
}
