#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestImplementation.h"


void TestCreateRowIdList(CuTest *tc){
	rowIdNode* list = createRowIdList();
	CuAssertPtrNotNull(tc,list);
	CuAssertPtrEquals(tc,NULL,list->next);
	CuAssertIntEquals(tc,1,list->isEmptyList);
	CuAssertIntEquals(tc,-1,list->rowId);
	deleteRowIdList(&list);
}

void TestInsertIntoRowIdList(CuTest *tc){
	// Insert to NULL list
	rowIdNode* list = createRowIdList();
	int result = insertIntoRowIdList(NULL,1);
	CuAssertIntEquals(tc,-1,result);
	// Insert negative rowId
	result = insertIntoRowIdList(list,-3);
	CuAssertIntEquals(tc,-1,result);
	// Insert to first node
	result = insertIntoRowIdList(list,1);
	rowIdNode* currentNode = list;
	CuAssertIntEquals(tc,0,currentNode->isEmptyList);
	CuAssertIntEquals(tc,1,currentNode->rowId);
	CuAssertPtrEquals(tc,NULL,currentNode->next);
	CuAssertIntEquals(tc,1,result);
	// Insert to second node
	result = insertIntoRowIdList(list,2);
	currentNode = currentNode->next;
	CuAssertIntEquals(tc,0,currentNode->isEmptyList);
	CuAssertIntEquals(tc,2,currentNode->rowId);
	CuAssertPtrEquals(tc,NULL,currentNode->next);
	CuAssertIntEquals(tc,1,result);
	CuAssertPtrEquals(tc,list->next,currentNode);
	// Insert same id
	result = insertIntoRowIdList(list,2);
	CuAssertIntEquals(tc,0,result);
	deleteRowIdList(&list);
}

void TestDeleteRowIdList(CuTest *tc){
	rowIdNode* list = createRowIdList();
	int result = insertIntoRowIdList(list,1);
	result = insertIntoRowIdList(list,2);
	deleteRowIdList(&list);
	CuAssertPtrEquals(tc,NULL,list);
}

CuSuite* ImplementationGetSuite() {
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestCreateRowIdList);
    SUITE_ADD_TEST(suite, TestInsertIntoRowIdList);
    SUITE_ADD_TEST(suite, TestDeleteRowIdList);
    return suite;
}

