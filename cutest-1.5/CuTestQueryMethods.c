#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestQueryMethods.h"

void TestQ(CuTest *tc){
	
}

CuSuite* QueryMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestQ);
    return suite;
}

