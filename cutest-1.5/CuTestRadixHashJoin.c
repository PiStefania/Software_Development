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

CuSuite* RadixHashJoinGetSuite() {		//adding TestAuxMethods Functions into suite
    CuSuite* suite = CuSuiteNew();
    
    SUITE_ADD_TEST(suite, TestHashFunction1);
    
    return suite;
}
