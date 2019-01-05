#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"
#include "threadPool.h"

typedef struct rowIdNode {
    uint32_t rowId;
    char isEmptyList;
    struct rowIdNode* next;
} rowIdNode;


typedef struct rowIdsList {
    int relationId;
    int num_of_rowIds;
    rowIdNode* rowIds;
} rowIdsList;

typedef struct intermediate{
	result* ResultList;
	int leftRelation;
	int rightRelation;
	tuple* foundIdsLeft;
	tuple* foundIdsRight;
	int capacityLeft;
	int capacityRight;
}intermediate;


int queriesImplementation(FILE* file, relationsInfo* initRelations, threadPool* thPool);

int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate, intermediate* inter, threadPool* thPool);
int updatePredicates(predicate** predicates, rowIdsList* rList, int currentPredicate, int side, intermediate** intermediateStructs, int noJoins, intermediate* currentIntermediate);
int checkSameId(tuple* foundIds, uint32_t rowId, int capacity, int checkFlag);

rowIdNode* createRowIdList();
int insertIntoRowIdList(rowIdNode** list, int rowId);
void deleteRowIdList(rowIdNode** list);
void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations);
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity, int checkFlag);
int existsInrList(rowIdsList* rList, int position, int rowId);

#endif
