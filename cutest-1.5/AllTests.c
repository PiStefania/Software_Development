#include <stdio.h>
#include <stdlib.h>
#include "CuTest.h"
#include "CuTestRadixHashJoin.h"
#include "CuTestQueryMethods.h"
#include "CuTestRelationMethods.h"
#include "CuTestImplementation.h"

CuSuite* CuGetSuite();
CuSuite* CuStringGetSuite();

void RunAllTests(void){
	CuString *output = CuStringNew();
	CuSuite* suite = CuSuiteNew();
	
	//radixHashJoin
	CuSuite* radixHashJoinSuite =  RadixHashJoinGetSuite();
	CuSuiteAddSuite(suite, radixHashJoinSuite);
	free(radixHashJoinSuite);
	radixHashJoinSuite=NULL;

	//queryMethods
	CuSuite* queryMethodsSuite =  QueryMethodsGetSuite();
	CuSuiteAddSuite(suite, queryMethodsSuite);
	free(queryMethodsSuite);
	queryMethodsSuite=NULL;

	//relationMethods
	CuSuite* relationMethodsSuite =  RelationMethodsGetSuite();
	CuSuiteAddSuite(suite, relationMethodsSuite);
	free(relationMethodsSuite);
	relationMethodsSuite=NULL;

	//implementation
	CuSuite* implementationSuite =  ImplementationGetSuite();
	CuSuiteAddSuite(suite, implementationSuite);
	free(implementationSuite);
	implementationSuite=NULL;

	CuSuiteRun(suite);
	CuSuiteSummary(suite, output);
	CuSuiteDetails(suite, output);
	printf("%s\n", output->buffer);
	
	CuStringDelete(output);
	CuSuiteDelete(suite);
}



int main(void){
	printf("Beginning of unit testing.\n");
	RunAllTests();
	printf("End of unit testing.\n");
}
