#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestStatisticsMethods.h"

void TestCheckCompareStatistics(CuTest *tc){

}

void TestUpdateCompareStatistics(CuTest *tc){

}

void TestUpdateJoinStatistics(CuTest *tc){

}

void TestCopyMetadata(CuTest *tc){
	/*char* init = malloc((strlen("./files_for_testing/test.init")+1)*sizeof(char));
	strcpy(init,"./files_for_testing/test.init");
	FILE* initFile = fopen("./files_for_testing/test.init", "r");
	relationsInfo* info = getRelationsData(initFile, init, &num_of_initRelations);*/
}

CuSuite* StatisticsMethodsGetSuite() {
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestCheckCompareStatistics);
    SUITE_ADD_TEST(suite, TestUpdateCompareStatistics);
    SUITE_ADD_TEST(suite, TestUpdateJoinStatistics);
    SUITE_ADD_TEST(suite, TestCopyMetadata);
    return suite;
}