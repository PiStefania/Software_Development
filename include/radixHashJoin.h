#ifndef _RADIX_HASH_JOIN_H_
#define _RADIX_HASH_JOIN_H_

#include <stdint.h>
#include "relationMethods.h"

/*#define BUCKETS 4							// Number of buckets is 2^n, where n = num of last bits for hashing
#define HEXBUCKETS 0x3						// From decimal to hex, for proper hashing (use of logical &)
#define HASH2 8								// Second hash number for indexing in buckets
#define HEXHASH2 0x7*/
// For 8 BUCKETS
#define BUCKETS 8
#define HEXBUCKETS 0x7
#define HASH2 16
#define HEXHASH2 0xF
#define ARRAYSIZE 1000	//((1024 * 1024) / 128)

/*#define RANGE 17							// Range of values for arrays initialization (1 to RANGE)
#define R_ROWS 7							// Number of rows for R array
#define S_ROWS 5							// Number of rows for S array
#define COLUMNS 1							// Colums of arrays (for this program, only 1)*/

#define PRINT 0				// Print results and arrays throughout process (suggested for debbuging and with small data)


typedef struct node resultNode;

//Type definition for a tuple
typedef struct tuple {
	int rArrayRow;
	uint64_t rowId;
	uint64_t value;
}tuple;

//Type definition for a relation.
//It consists of an array of tuples and a size of the relation.
typedef struct relation{
	tuple *tuples;
	uint32_t num_tuples;
}relation;

typedef struct resultElement{
	int rArrayRow1;
	uint32_t rowId1;
	int rArrayRow2;
	uint32_t rowId2;
}resultElement;

struct node{
	struct node* next;
	resultElement array[ARRAYSIZE];
	int num_of_elems; //max == 16384
}node;

typedef struct result{
	resultNode* head;
}result;

// For job scheduler - threads
// Struct of args of createHistogramThread function
typedef struct histArgs{
	relation** Hist;
	relation* R;
}histArgs;


// Not used
typedef struct rOrderedArgs{
	relation* R;
	relation* Hist;
	relation* Psum;
}rOrderedArgs;

// Struct of args of indexCompareJoinThread function
typedef struct indexCompareJoinArgs{
	result* ResultList;
	relation* ROrdered;
	relation* RHist;
	relation* RPsum;
	relation* SOrdered;
	relation* SHist;
	relation* SPsum;
	int currentBucket;
} indexCompareJoinArgs;


resultNode * createNode();
result * createList();
int insertToList(result** list, tuple RItemTuple, tuple SItemTuple);
void printList(result * list);
void deleteList(result ** list);

//Radix Hash Join
result* RadixHashJoin(relation *relR, relation *relS);

//Hash Functions - return no of bucket
int hashFunction1(uint64_t value);
int hashFunction2(uint64_t value);

//create relation for field
relation* createRelation(uint64_t* col, uint64_t* rowIds, uint64_t noOfElems);
//delete specific relation
void deleteRelation(relation** rel);
//print relation
void printRelation(relation* rel);

//create Histogram from R
relation* createHistogram(relation* R);

//create Psum
relation* createPsum(relation* Hist);

//create relation with ordered buckets
relation* createROrdered(relation* R, relation* Hist, relation* Psum);

//create indexes for each bucket in R array, compare the items of S with R's and finally join the same values (return in the list rowIds)
int indexCompareJoin(result* ResultList, relation* ROrdered, relation* RHist, relation* RPsum, relation* SOrdered, relation* SHist, relation* SPsum);

// Threads
void createHistogramThread(histArgs* args);
void indexCompareJoinThread(indexCompareJoinArgs* args);

#endif
