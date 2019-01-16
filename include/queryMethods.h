#ifndef _QUERY_METHODS_H_
#define _QUERY_METHODS_H_

#include "radixHashJoin.h"

typedef struct predicate{
	tuple* leftSide;
	tuple* rightSide;
	char comparator;
	int kind;				// kind:0 filter predicate - kind:1 join predicate
	int needsToBeDeleted;
} predicate;

// For query input
int* getRelationsFromLine(char* relationsStr, int* relationsSize);
tuple* getProjectionsFromLine(char* projectionsStr, int* projectionsSize);
predicate** getPredicatesFromLine(char* predicatesStr, int* predicatesSize);

// For predicate struct
predicate** createPredicate(int size);
void deletePredicate(predicate** p);
int setPredicate(char* str, predicate** p, predicate** allPredicates, int size);
int checkPredicate(predicate* p1,predicate* p2);
int checkIfSamePredicateExists(predicate* p, predicate** allPredicates, int size);
void printPredicate(predicate* p);
int isNumeric(char* s);

#endif
