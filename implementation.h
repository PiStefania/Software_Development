#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"


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
}intermediate;


int queriesImplementation(FILE* file, relationsInfo* initRelations);

int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate, intermediate* inter);
int updatePredicates(predicate** predicates, rowIdsList* rList, int currentPredicate, int side, intermediate** intermediateStructs, int noJoins);

rowIdNode* createRowIdList();
int insertIntoRowIdList(rowIdNode** list, int rowId);
void deleteRowIdList(rowIdNode** list);
void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations);
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type);
int existsInrList(rowIdsList* rList, int position, int rowId);

#endif
