#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "relationMethods.h"


typedef struct rowIdNode {
    int32_t rowId;
    char isEmptyList;
    struct rowIdNode* next;
} rowIdNode;


typedef struct rowIdsList {
    int relationId1;
    int relationId2;
    rowIdNode* rowIds;
} rowIdsList;


int queriesImplementation(FILE* file, relationsInfo* initRelations);

rowIdNode* createRowIdList();
int insertIntoRowIdList(rowIdNode* list, int rowId);
void deleteRowIdList(rowIdNode* list);

#endif
