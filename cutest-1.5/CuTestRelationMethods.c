#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRelationMethods.h"


void TestGetRelationsData(CuTest *tc){
	// Open file for read
	int num_of_initRelations = 0;
	FILE* initFile = fopen("./files_for_testing/test.init", "r");
	relationsInfo* info = getRelationsData(initFile, &num_of_initRelations);
	CuAssertIntEquals(tc,3,num_of_initRelations);
	CuAssertStrEquals(tc,"./files_for_testing/r0",info[0].relName);
	CuAssertIntEquals(tc,1561,info[0].num_of_rows);
	CuAssertIntEquals(tc,3,info[0].num_of_columns);
	CuAssertIntEquals(tc,1,info[0].Rarray[0][0]);
	CuAssertStrEquals(tc,"./files_for_testing/r1",info[1].relName);
	CuAssertIntEquals(tc,3754,info[1].num_of_rows);
	CuAssertIntEquals(tc,3,info[1].num_of_columns);
	CuAssertIntEquals(tc,4,info[1].Rarray[0][0]);
	CuAssertStrEquals(tc,"./files_for_testing/r2",info[2].relName);
	CuAssertIntEquals(tc,26808,info[2].num_of_rows);
	CuAssertIntEquals(tc,4,info[2].num_of_columns);
	CuAssertIntEquals(tc,4,info[2].Rarray[0][0]);
	//TODO : check metadata columns

	deleteRelationsData(info, 3);
}

CuSuite* RelationMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestGetRelationsData);
    return suite;
}

