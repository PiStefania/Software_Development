#ifndef _QUERY_METHODS_H_
#define _QUERY_METHODS_H_

#include "radixHashJoin.h"

typedef struct predicate{
	tuple* leftSide;
	tuple* rightSide;
	char comparator;
	int kind;				// kind:0 filter predicate - kind:1 join predicate
}predicate;

// For query input
int* getRelationsFromLine(char* relationsStr, int* relationsSize);
tuple* getProjectionsFromLine(char* projectionsStr, int* projectionsSize);
predicate** getPredicatesFromLine(char* predicatesStr, int* predicatesSize);
void setPredicate(char* str, predicate** p);

// For predicate struct
predicate** createPredicate(int size);
void deletePredicate(predicate** p);

int isNumeric(char* s);

#endif
