#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRadixHashJoin.h"

#define HASHFUNC1VALUE1 200
#define HASHFUNC1VALUE2 211

void TestHashFunction1(CuTest *tc){
	int32_t result = hashFunction1(HASHFUNC1VALUE1);
	CuAssertIntEquals(tc,0,result);
	result = hashFunction1(HASHFUNC1VALUE2);
	CuAssertIntEquals(tc,3,result);
}

void TestHashFunction2(CuTest *tc) {
	int32_t result = hashFunction2(HASHFUNC1VALUE1);
	CuAssertIntEquals(tc,0,result);
	result = hashFunction2(HASHFUNC1VALUE2);
	CuAssertIntEquals(tc,3,result);
}

void TestcreateNode(CuTest *tc) {
	struct node * newNode = createNode();
	CuAssertPtrNotNull(tc, newNode);
	CuAssertPtrEquals(tc, NULL, newNode->next);
	CuAssertIntEquals(tc, 0, newNode->num_of_elems);
	int i;
	for (i=0; i < ARRAYSIZE; i++) {
		CuAssertIntEquals(tc, -1, newNode->array[i].rowId1);
		CuAssertIntEquals(tc, -1, newNode->array[i].rowId2);
	}
	free(newNode);
}

CuSuite* RadixHashJoinGetSuite() {		//adding TestAuxMethods Functions into suite
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestHashFunction1);
    SUITE_ADD_TEST(suite, TestHashFunction2);
	SUITE_ADD_TEST(suite, TestcreateNode);

    return suite;
}
