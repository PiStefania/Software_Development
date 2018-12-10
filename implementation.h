#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"

// TODO: alter rowIdsList to an array of ids
typedef struct rowIdNode {
    int32_t rowId;
    char isEmptyList;
    struct rowIdNode* next;
} rowIdNode;


typedef struct rowIdsList {
    int relationId;
    int num_of_rowIds;
    rowIdNode* rowIds;
} rowIdsList;


int queriesImplementation(FILE* file, relationsInfo* initRelations);

int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate);

rowIdNode* createRowIdList();
int insertIntoRowIdList(rowIdNode* list, int rowId);
void deleteRowIdList(rowIdNode* list);
void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations);
uint64_t* setRowIdsToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn1);
int existsInrList(rowIdsList* rList, int position, int rowId);

#endif
