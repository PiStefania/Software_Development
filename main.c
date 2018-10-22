#include <stdio.h>
#include <stdlib.h>
#include "auxMethods.h"
#include "radixHashJoin.h"

int main(int argc, char* argv[]){
	printf("START OF MAIN PROGRAM\n");
	//ex
	int32_t out = hashFunction1(148);
	printf("HASH: %d\n",out);
	
	//arrays
	int32_t** R = malloc(3*sizeof(int*));
	for(int i=0;i<3;i++){
		R[i] = malloc(2*sizeof(int));
	}
	R[0][0] = 1;
	R[0][1] = 3;
	R[1][0] = 1;
	R[1][1] = 4;
	R[2][0] = 2;
	R[2][1] = 5;
	int32_t** S = malloc(2*sizeof(int*));
	for(int i=0;i<2;i++){
		S[i] = malloc(2*sizeof(int));
	}
	S[0][0] = 1;
	S[0][1] = 3;
	S[1][0] = 2;
	S[1][1] = 3;

	//get col
	int* column = getColumnOfArray(R, 3, 0);
	printf("COL\n");
	for(int i=0;i<3;i++){
		printf("elem: %d\n",column[i]);
	}
	
	//create relation
	printf("---RELATION FROM 1 FIELD---\n");
	relation* rel = createRelation(column, 3);
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
	relation* ROrdered = createROrdered(RNotOrdered,Psum);
	printRelation(ROrdered);
	
	//free
	for(int i=0;i<3;i++){
		free(R[i]);
	}
	free(R);
	for(int i=0;i<2;i++){
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