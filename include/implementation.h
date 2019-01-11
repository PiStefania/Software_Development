#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"
#include "rowIdArrayMethods.h"
#include "threadPool.h"
#include "radixHashJoin.h"


int queriesImplementation(FILE* file, relationsInfo* initRelations, int num_of_initRelations, threadPool* thPool);
int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsArray** rArray, int currentPredicate, threadPool* thPool);
void searchOutdatedPredicates(predicate** predicates, char *outdatedPredicates, int currentPredicate);

#endif
