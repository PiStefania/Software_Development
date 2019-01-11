#ifndef _ROW_ID_LIST_METHODS_H_
#define _ROW_ID_LIST_METHODS_H_

#include "relationMethods.h"
#include "radixHashJoin.h"

#define DEFAULT_ROWS 100


typedef struct rowIdsArray{
    int relationId;
    int num_of_rowIds;
    int position;				// current capacity - where to insert rowId
    uint64_t* rowIds;
} rowIdsArray;


rowIdsArray* createRowIdsArray(int relationId);
int insertIntoRowIdsArray(rowIdsArray* rArray, int rowId);
void doubleRowIdsArray(rowIdsArray* rArray);
void deleteRowIdsArray(rowIdsArray** rArray);
void printRowIdsArray(rowIdsArray* rArray, int noOfRelations);
relation* createRelationFromRarray(rowIdsArray* rArray, relationsInfo* initRelations, int relationId, int relColumn);

#endif
