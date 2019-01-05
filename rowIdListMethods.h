#ifndef _ROW_ID_LIST_METHODS_H_
#define _ROW_ID_LIST_METHODS_H_

#include "relationMethods.h"
#include "queryMethods.h"
#include "radixHashJoin.h"


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


int checkSameId(tuple* foundIds, uint32_t rowId, int capacity, char checkFlag);
rowIdNode* createRowIdList();
int insertIntoRowIdList(rowIdNode** list, int rowId);
void deleteRowIdList(rowIdNode** list);
void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations);
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity);
void searchOutdatedPredicates(predicate** predicates, tuple* projections, char *outdatedPredicates, int currentPredicate, int predicatesSize, int projectionsSize);


#endif
