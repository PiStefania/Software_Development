#ifndef _ROW_ID_LIST_METHODS_H_
#define _ROW_ID_LIST_METHODS_H_

#include "relationMethods.h"
#include "queryMethods.h"
#include "radixHashJoin.h"

#define DEFAULT_ROWS 5

typedef struct rowIdsArray{
    int relationId;
    int num_of_rowIds;
    int position;				// current capacity - where to insert rowId
    uint32_t* rowIds;
} rowIdsArray;


int checkSameId(tuple* foundIds, uint32_t rowId, int capacity, char checkFlag);
rowIdsArray* createRowIdsArray(int relationId);
int insertIntoRowIdsArray(rowIdsArray* rArray, int rowId);
void doubleRowIdsArray(rowIdsArray* rArray);
void deleteRowIdsArray(rowIdsArray** rArray);
void printRowIdsArray(rowIdsArray* rArray, int noOfRelations);
uint64_t* setRowIdsValuesToArray(rowIdsArray** rArray, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity);
void searchOutdatedPredicates(predicate** predicates, tuple* projections, char *outdatedPredicates, int currentPredicate, int predicatesSize, int projectionsSize);


#endif
