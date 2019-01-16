#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestImplementation.h"

void TestSearchOutdatedPredicates(CuTest *tc){
	int predicatesSize;
	char* normalStr = malloc((strlen("0.1=2.4&3.1=0.1&2.1>4")+1)*sizeof(char));
	strcpy(normalStr,"0.1=2.4&3.1=0.1&2.1>4");
	predicate** predicates = getPredicatesFromLine(normalStr, &predicatesSize);
	char* outdatedPredicates = malloc(3 * sizeof(char));
	searchOutdatedPredicates(predicates, outdatedPredicates, 2);
	CuAssertIntEquals(tc,0,outdatedPredicates[0]);
	CuAssertIntEquals(tc,2,outdatedPredicates[1]);
	for (int i = 0; i < 3; i++) {
		deletePredicate(&predicates[i]);
	}
	free(predicates);
	free(outdatedPredicates);
	free(normalStr);
}

CuSuite* ImplementationGetSuite() {
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestSearchOutdatedPredicates);
    return suite;
}

