#ifndef _IMPLEMENTATION_H_
#define _IMPLEMENTATION_H_

#include "queryMethods.h"
#include "relationMethods.h"
#include "rowIdListMethods.h"


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


int queriesImplementation(FILE* file, relationsInfo* initRelations, int num_of_initRelations);

int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate,
                        intermediate* inter, intermediate** intermediateStructs, int noJoins);
int updatePredicates(predicate** predicates, rowIdsList* rList, int currentPredicate, int side, intermediate** intermediateStructs,
                        int noJoins, intermediate* currentIntermediate);

#endif
