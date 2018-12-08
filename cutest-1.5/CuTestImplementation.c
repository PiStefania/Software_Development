#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestImplementation.h"

void TestI(CuTest *tc){
	
}


CuSuite* ImplementationGetSuite() {
    CuSuite* suite = CuSuiteNew();
    
    SUITE_ADD_TEST(suite, TestI);

    return suite;
}

