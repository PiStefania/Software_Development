#ifndef _QUERY_METHODS_H_
#define _QUERY_METHODS_H_

#include "radixHashJoin.h"

int getQueryLines(FILE* file);
int* getRelationsFromLine(char* relationsStr, int* relationsSize);
tuple* getProjectionsFromLine(char* projectionsStr, int* projectionsSize);

#endif