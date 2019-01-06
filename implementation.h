#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"
#include "rowIdArrayMethods.h"
#include "threadPool.h"


typedef struct intermediate {
	result* ResultList;
	int leftRelation;
	int leftColumn;
	int rightRelation;
	int rightColumn;
	tuple* foundIdsLeft;
	tuple* foundIdsRight;
	int capacityLeft;
	int capacityRight;
} intermediate;


int queriesImplementation(FILE* file, relationsInfo* initRelations, int num_of_initRelations, threadPool* thPool);

int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsArray** rArray, int currentPredicate,
                        intermediate* inter, intermediate** intermediateStructs, int noJoins, threadPool* thPool);
int updatePredicates(predicate** predicates, rowIdsArray** rArray, int currentPredicate, int side, intermediate** intermediateStructs,
                        int noJoins, intermediate* currentIntermediate);

void searchOutdatedPredicates(predicate** predicates, tuple* projections, char *outdatedPredicates, int currentPredicate, int predicatesSize, int projectionsSize);

int checkSameId(tuple* foundIds, uint64_t rowId, int capacity, char checkFlag);

#endif
