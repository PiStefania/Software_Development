#include <stdio.h>
#include <stdlib.h>
#include "radixHashJoin.h"

#define BUCKETS 8				// Number of buckets is 2^n, where n = num of last bits for hashing

//H1 for bucket selection, get last 3 bits
int32_t hashFunction1(int32_t value){
	return value & 0x7;
}

//create relation array for field
relation* createRelation(int* col, int noOfElems){
	relation* rel = malloc(sizeof(relation));
	rel->tuples = malloc(noOfElems*sizeof(tuple));
	for(int i=0;i<noOfElems;i++){
		rel->tuples[i].key = i;
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
	printf("Relation has %d tuples\n",rel->num_tuples);
	for(int i=0;i<rel->num_tuples;i++){
		printf("Row with key: %d and value: %d\n",rel->tuples[i].key,rel->tuples[i].value);
	}
}

// Create the first unordered array (R)
// Here, tuples.key field indicates the hash value of each rel.value element
relation* createBucketsRelation(relation* rel){
	relation* R = malloc(sizeof(relation));
	R->tuples = malloc(rel->num_tuples*sizeof(tuple));
	R->num_tuples = rel->num_tuples;
	for(int i=0;i<R->num_tuples;i++){
		//check with hash, no of bucket
		R->tuples[i].key = hashFunction1(rel->tuples[i].value);
		R->tuples[i].value = rel->tuples[i].value;
	}
	return R;
}

// Create the Histogram, where we store the number of elements corresponding with each hash value
// The size of tuples array is the number of buckets (one position for each hash value)
relation* createHistogram(relation* R){
	relation* Hist = malloc(sizeof(relation));
	Hist->num_tuples = BUCKETS;
	Hist->tuples = malloc(BUCKETS * sizeof(tuple));
	//initialize Hist
	for(int i=0;i<Hist->num_tuples;i++){
		Hist->tuples[i].key = i;
		Hist->tuples[i].value = 0;
	}
	//populate Hist
	for(int i=0;i<R->num_tuples;i++){
		int bucket = R->tuples[i].key;
		Hist->tuples[bucket].value++;
	}
	return Hist;
}

// Create Psum just like histogram, but for different purpose
// We calculate the position of the first element from each bucket in the new ordered array
relation* createPsum(relation* Hist){
	relation* Psum = malloc(sizeof(relation));
	Psum->num_tuples = BUCKETS;
	Psum->tuples = malloc(BUCKETS * sizeof(tuple));
	int32_t sum = 0;
	for(int i=0;i<Psum->num_tuples;i++){
		Psum->tuples[i].key = Hist->tuples[i].key;
		if (Hist->tuples[i].value == 0)
			Psum->tuples[i].value = -1;				// If there is no item for a hash value, we write -1
		else
			Psum->tuples[i].value = sum;
		sum += Hist->tuples[i].value;
	}
	return Psum;
}

// Create the new ordered array (R')
relation* createROrdered(relation* R, relation* Hist, relation* Psum){
	// Allocate a relation array same as R
	relation* ROrdered = malloc(sizeof(relation));
	ROrdered->num_tuples = R->num_tuples;
	ROrdered->tuples = malloc(ROrdered->num_tuples * sizeof(tuple));

	// Create a copy of Histogram, so as to use it while filling in the new ordered array
	// Whenever we copy an element from old array to the new one, reduce the counter of its hist id (in RemainHist)
	// In this way, we read the old array only one time - complexity O(n) (n = R->num_tuples)
	// We copy all the elements one by one, in the proper position of the new array
	relation* RemainHist = malloc(sizeof(relation));
	RemainHist->num_tuples = Hist->num_tuples;
	RemainHist->tuples = malloc(RemainHist->num_tuples * sizeof(tuple));
	for (int i = 0; i < Hist->num_tuples; i++) {
		RemainHist->tuples[i].key = Hist->tuples[i].key;
		RemainHist->tuples[i].value = Hist->tuples[i].value;
	}
	// Now copy the elements of old array to the new one by buckets (ordered)
	for (int i = 0; i < R->num_tuples; i++) {
		int32_t hashId = R->tuples[i].key;
		int offset = Hist->tuples[hashId].value - RemainHist->tuples[hashId].value;		// Total hash items - hash items left
		int ElementNewPosition = Psum->tuples[hashId].value + offset;									// Position = bucket's position + offset
		RemainHist->tuples[hashId].value--;
		ROrdered->tuples[ElementNewPosition].key = R->tuples[i].key; 							// Copy the element form old to new array
		ROrdered->tuples[ElementNewPosition].value = R->tuples[i].value;
	}
	// Delete the RemainHist Array
	deleteRelation(&RemainHist);
	return ROrdered;
}
