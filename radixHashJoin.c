#include <stdio.h>
#include <stdlib.h>
#include "radixHashJoin.h"

#define BUCKETS 8

//H1 for bucket selection, get last 3 bits
int32_t hashFunction1(int32_t value){
	return value & 0x7;
}

//create relation array for field
relation* createRelation(int* col, int noOfElems){
	relation* rel = malloc(sizeof(relation));
	rel->tuples = malloc(noOfElems*sizeof(tuple));
	for(int i=0;i<noOfElems;i++){
		rel->tuples[i].rowId = i+1;
		rel->tuples[i].value = col[i];
	}
	rel->num_tuples = noOfElems;
	return rel;
}
	

void deleteRelation(relation** rel){
	free((*rel)->tuples);
	(*rel)->tuples = NULL;
	free(*rel);
	*rel = NULL;
}

void printRelation(relation* rel){
	printf("Relation has: %d tuples\n",rel->num_tuples);
	for(int i=0;i<rel->num_tuples;i++){
		printf("Row with id: %d and value: %d\n",rel->tuples[i].rowId,rel->tuples[i].value);
	}
}

relation* createBucketsRelation(relation* rel){
	relation* R = malloc(sizeof(relation));
	R->tuples = malloc(rel->num_tuples*sizeof(tuple));
	R->num_tuples = rel->num_tuples;
	for(int i=0;i<R->num_tuples;i++){
		//check with hash, no of bucket
		R->tuples[i].rowId = hashFunction1(rel->tuples[i].value);
		R->tuples[i].value = rel->tuples[i].value;
	}
	return R;
}

relation* createHistogram(relation* R){
	relation* Hist = malloc(sizeof(relation));
	Hist->num_tuples = BUCKETS;
	Hist->tuples = malloc(BUCKETS * sizeof(tuple));
	//initialize Hist
	for(int i=0;i<Hist->num_tuples;i++){
		Hist->tuples[i].rowId = i+1;
		Hist->tuples[i].value = 0;
	}
	//populate Hist
	for(int i=0;i<R->num_tuples;i++){
		int bucket = R->tuples[i].rowId;
		Hist->tuples[bucket-1].value++;
	}
	return Hist;
}

relation* createPsum(relation* Hist){
	relation* Psum = malloc(sizeof(relation));
	Psum->num_tuples = BUCKETS;
	Psum->tuples = malloc(BUCKETS * sizeof(tuple));
	int32_t sum = 0;
	for(int i=0;i<Psum->num_tuples;i++){
		Psum->tuples[i].rowId = Hist->tuples[i].rowId;
		Psum->tuples[i].value = sum;
		sum += Hist->tuples[i].value;
	}
	return Psum;
}

relation* createROrdered(relation* R, relation* Psum){
	relation* ROrdered = malloc(sizeof(relation));
	ROrdered->num_tuples = R->num_tuples;
	ROrdered->tuples = malloc(ROrdered->num_tuples*sizeof(tuple));
	int32_t bucket = 1;
	for(int k=0;k<Psum->num_tuples-1;k++){
		//get "floor" of psum
		int32_t init = Psum->tuples[k].value;
		//get "ceil" of psum
		int32_t end = Psum->tuples[k+1].value;
		for(int i=init;i<end;i++){
			for(int j=0;j<R->num_tuples;j++){
				if(R->tuples[j].rowId == bucket){
					ROrdered->tuples[i].rowId = R->tuples[j].rowId;
					ROrdered->tuples[i].value = R->tuples[j].value;
				}
			}	
		}
		bucket++;
	}
	return ROrdered;
}

