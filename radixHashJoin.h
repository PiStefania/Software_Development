#ifndef _RADIX_HASH_JOIN_H_
#define _RADIX_HASH_JOIN_H_

#include <stdint.h>

#define ARRAYSIZE ((1024 * 1024) / 64)

typedef struct node resultNode;

//Type definition for a tuple
typedef struct tuple{
	int32_t key;
	int32_t value;
}tuple;

//Type definition for a relation.
//It consists of an array of tuples and a size of the relation.
typedef struct relation{
	tuple *tuples;
	uint32_t num_tuples;
}relation;

typedef struct resultElement{
	int32_t key1;
	int32_t key2;
}resultElement;

struct node{
	struct node* next;
	resultElement array[ARRAYSIZE];
	int num_of_elems; //max == 16384
}node;

typedef struct result{
	resultNode* head;
}result;

resultNode * createNode();

result * createList();

int insertToList(result * list, int32_t rowID1, int32_t rowID2);

//Radix Hash Join
result* RadixHashJoin(relation *relR, relation *relS);

//H1 Function - return no of bucket
int32_t hashFunction1(int32_t value);

//create relation for field
relation* createRelation(int* col, int noOfElems);

//delete specific relation
void deleteRelation(relation** rel);

//print relation
void printRelation(relation* rel);

//create relation with buckets
relation* createBucketsRelation(relation* rel);

//create Histogram from R
relation* createHistogram(relation* R);

//create Psum
relation* createPsum(relation* Hist);

//create relation with ordered buckets
relation* createROrdered(relation* R, relation* Hist, relation* Psum);

#endif
