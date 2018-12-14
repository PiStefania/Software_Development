#ifndef _RELATION_METHODS_H_
#define _RELATION_METHODS_H_

#include <stdint.h>

typedef struct metadataCol {
    uint32_t num_of_rows;
    uint32_t min, max;
    uint32_t discrete_values;
} metadataCol;

typedef struct relationsInfo {
    char* relName;
    uint64_t num_of_columns;
    uint64_t num_of_rows;
    uint64_t** Rarray;
    metadataCol * MDCols; //metadata about each column goes here
} relationsInfo;

typedef struct stringNode {
    char isEmptyList;               // 1 true, 0 false
    char* name;
    struct stringNode* next;
} stringNode;


relationsInfo* getRelationsData(FILE* file, int* num_of_initRelations);
void deleteRelationsData(relationsInfo* initRelations, int num_of_initRelations);

// Functions for handling the list in which data files' names are stored
stringNode* createNameList();
int insertIntoNameList(stringNode* nameList, char* name);
char* findNameByIndex(stringNode* nameList, int index);
void deleteNameList(stringNode* nameList);

#endif
