#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestMetadataColumn.h"

void TestM(CuTest *tc){

}

CuSuite* MetadataColumnGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestM);
    return suite;
}

