#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRelationMethods.h"

void TestR(CuTest *tc){
	
}

CuSuite* RelationMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestR);
    return suite;
}

