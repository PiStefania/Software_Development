#ifndef _RELATION_METHODS_H_
#define _RELATION_METHODS_H_

#include <stdint.h>
#include "auxMethods.h"


typedef struct relationsInfo {
    char relName[STRINGLEN];
    uint64_t num_of_columns;
    uint64_t num_of_rows;
    uint64_t** Rarray;
} relationsInfo;


relationsInfo* getRelationsData(FILE* file, int* num_of_initRelations);
void deleteRelationsData(relationsInfo* initRelations, int num_of_initRelations);


#endif
