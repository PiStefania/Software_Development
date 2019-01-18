#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRelationMethods.h"


void TestGetRelationsData(CuTest *tc){
	// Open file for read
	int num_of_initRelations = 0;
	char* init = malloc((strlen("./files_for_testing/test.init")+1)*sizeof(char));
	strcpy(init,"./files_for_testing/test.init");
	FILE* initFile = fopen("./files_for_testing/test.init", "r");
	relationsInfo* info = getRelationsData(initFile, init, &num_of_initRelations);
	CuAssertPtrNotNull(tc,info);
	CuAssertIntEquals(tc,3,num_of_initRelations);
	CuAssertStrEquals(tc,"r0",info[0].relName);
	CuAssertIntEquals(tc,1561,info[0].num_of_rows);
	CuAssertIntEquals(tc,3,info[0].num_of_columns);
	CuAssertIntEquals(tc,1,info[0].Rarray[0][0]);
	CuAssertStrEquals(tc,"r1",info[1].relName);
	CuAssertIntEquals(tc,3754,info[1].num_of_rows);
	CuAssertIntEquals(tc,3,info[1].num_of_columns);
	CuAssertIntEquals(tc,4,info[1].Rarray[0][0]);
	CuAssertStrEquals(tc,"r2",info[2].relName);
	CuAssertIntEquals(tc,26808,info[2].num_of_rows);
	CuAssertIntEquals(tc,4,info[2].num_of_columns);
	CuAssertIntEquals(tc,4,info[2].Rarray[0][0]);
	deleteRelationsData(info, &num_of_initRelations);
	free(init);
}


void TestDeleteRelationsData(CuTest *tc){
	// Delete malloced variable
	int num_of_initRelations = 0;
	char* init = malloc((strlen("./files_for_testing/test.init")+1)*sizeof(char));
	strcpy(init,"./files_for_testing/test.init");
	FILE* initFile = fopen("./files_for_testing/test.init", "r");
	relationsInfo* info = getRelationsData(initFile, init, &num_of_initRelations);
	deleteRelationsData(info, &num_of_initRelations);
	CuAssertIntEquals(tc, -1, num_of_initRelations);
	free(init);
}


void TestCreateNameList(CuTest *tc){
	stringNode* nameList = createNameList();
	CuAssertPtrNotNull(tc,nameList);
	CuAssertIntEquals(tc,1,nameList->isEmptyList);
	CuAssertPtrEquals(tc,NULL,nameList->next);
	CuAssertPtrEquals(tc,NULL,nameList->name);
	deleteNameList(&nameList);
}


void TestInsertIntoNameList(CuTest *tc){
	stringNode* nameList = createNameList();
	int result = -1;
	// Insert NULL
	result = insertIntoNameList(nameList, NULL);
	CuAssertIntEquals(tc, 0, result);
	result = insertIntoNameList(NULL, "test");
	CuAssertIntEquals(tc, 0, result);
	// Insert to firtst node
	char* name1 = malloc((strlen("file1")+1)*sizeof(char));
	strcpy(name1,"file1");
	char* name2 = malloc((strlen("file2")+1)*sizeof(char));
	strcpy(name2,"file2");
	result = insertIntoNameList(nameList, name1);
	CuAssertIntEquals(tc, 1, result);
	CuAssertPtrNotNull(tc,nameList);
	CuAssertStrEquals(tc,"file1",nameList->name);
	CuAssertPtrEquals(tc, NULL, nameList->next);
	// Insert to second node
	result = insertIntoNameList(nameList, name2);
	CuAssertIntEquals(tc, 1, result);
	CuAssertPtrNotNull(tc,nameList->next);
	CuAssertStrEquals(tc,"file2",nameList->next->name);
	CuAssertPtrEquals(tc, NULL, nameList->next->next);

	free(name1);
	free(name2);
	deleteNameList(&nameList);
}


void TestFindNameByIndex(CuTest *tc){
	stringNode* nameList = createNameList();
	int result = -1;
	char* name1 = malloc((strlen("file1")+1)*sizeof(char));
	strcpy(name1,"file1");
	char* name2 = malloc((strlen("file2")+1)*sizeof(char));
	strcpy(name2,"file2");
	result = insertIntoNameList(nameList, name1);
	result = insertIntoNameList(nameList, name2);
	// Normal search
	char* nameValue = findNameByIndex(nameList,0);
	CuAssertStrEquals(tc,name1,nameValue);
	nameValue = findNameByIndex(nameList,1);
	CuAssertStrEquals(tc,name2,nameValue);
	// Search with NULL nameList
	nameValue = findNameByIndex(NULL,0);
	CuAssertStrEquals(tc,NULL,nameValue);
	// Search with negative index
	nameValue = findNameByIndex(nameList,-2);
	CuAssertStrEquals(tc,NULL,nameValue);
	free(name1);
	free(name2);
	deleteNameList(&nameList);
}


void TestDeleteNameList(CuTest *tc){
	stringNode* nameList = createNameList();
	int result = -1;
	char* name1 = malloc((strlen("file1")+1)*sizeof(char));
	strcpy(name1,"file1");
	char* name2 = malloc((strlen("file2")+1)*sizeof(char));
	strcpy(name2,"file2");
	result = insertIntoNameList(nameList, name1);
	result = insertIntoNameList(nameList, name2);
	char* nameValue = findNameByIndex(nameList,0);
	nameValue = findNameByIndex(nameList,1);
	deleteNameList(&nameList);
	CuAssertPtrEquals(tc,NULL,nameList);
	free(name1);
	free(name2);
}


CuSuite* RelationMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestGetRelationsData);
    SUITE_ADD_TEST(suite, TestDeleteRelationsData);
    SUITE_ADD_TEST(suite, TestCreateNameList);
    SUITE_ADD_TEST(suite, TestInsertIntoNameList);
    SUITE_ADD_TEST(suite, TestFindNameByIndex);
    SUITE_ADD_TEST(suite, TestDeleteNameList);
    return suite;
}

