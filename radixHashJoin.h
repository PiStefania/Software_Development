#ifndef _RADIX_HASH_JOIN_H_
#define _RADIX_HASH_JOIN_H_

#include <stdint.h>

#define ARRAYSIZE ((1014 * 1024) / 64)

typedef struct node resultNode;

//Type definition for a tuple
typedef struct tuple{
	int32_t rowId;
	int32_t value;
}tuple;

//Type definition for a relation.
//It consists of an array of tuples and a size of the relation.
typedef struct relation{
	tuple *tuples;
	uint32_t num_tuples;
}relation;

typedef struct resultElement{
	int32_t rowId1;
	int32_t rowId2;
}resultElement;

struct node{
	struct node* next;
	resultElement array[ARRAYSIZE];
}node;

typedef struct result{
	resultNode* head;
}result;

//Radix Hash Join
result* RadixHashJoin(relation *relR, relation *relS);

#endif

