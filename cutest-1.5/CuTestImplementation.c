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

void TestSetRowIdsValuesToArray(CuTest *tc){
	// Create rList
	rowIdsList* rList = malloc(7*sizeof(rowIdsList));
	rList[0].relationId = 0;
	rList[0].num_of_rowIds = 10;
	rList[0].rowIds = createRowIdList();
	rList[1].num_of_rowIds = 12;
	rList[1].relationId = 5;
	rList[1].rowIds = createRowIdList();
	rList[2].relationId = 1;
	rList[2].num_of_rowIds = 2;
	rList[2].rowIds = createRowIdList();
	rList[3].relationId = 3;
	rList[3].num_of_rowIds = 8;
	rList[3].rowIds = createRowIdList();
	rList[4].relationId = 4;
	rList[4].num_of_rowIds = 5;
	rList[4].rowIds = createRowIdList();
	rList[5].relationId = 7;
	rList[5].num_of_rowIds = 0;
	rList[5].rowIds = createRowIdList();
	rList[6].relationId = 9;
	rList[6].num_of_rowIds = 5;
	rList[6].rowIds = NULL;
	int rowIds1[10] = {0,3,4,1,7,9,8,100,55,99};
	int rowIds2[12] = {98,3,4,10,72,90,8,120,50,91,87,66};
	int rowIds3[2] = {67,87};
	int rowIds4[8] = {94,10,72,90,8,126,50,87};
	int rowIds5[5] = {87,44,55,2,1};

	for(int i=0;i<10;i++){
		insertIntoRowIdList(rList[0].rowIds, rowIds1[i]);
	}
	for(int i=0;i<12;i++){
		insertIntoRowIdList(rList[1].rowIds, rowIds2[i]);
	}
	for(int i=0;i<2;i++){
		insertIntoRowIdList(rList[2].rowIds, rowIds3[i]);
	}
	for(int i=0;i<8;i++){
		insertIntoRowIdList(rList[3].rowIds, rowIds4[i]);
	}
	for(int i=0;i<5;i++){
		insertIntoRowIdList(rList[4].rowIds, rowIds5[i]);
	}
	// Create initRelations
	relationsInfo* initRelations = NULL;
	int num_of_initRelations = 0;
	FILE* initFile = fopen("./files_for_testing/test.init", "r");
	initRelations = getRelationsData(initFile, &num_of_initRelations);
	// Get array from rList from specific relation - values
	uint64_t* array1 = setRowIdsValuesToArray(rList, 0, initRelations, 0, 0, 1);
	CuAssertPtrNotNull(tc,array1);
	CuAssertIntEquals(tc,1,array1[0]);
	CuAssertIntEquals(tc,11,array1[1]);
	CuAssertIntEquals(tc,15,array1[2]);
	CuAssertIntEquals(tc,3,array1[3]);
	CuAssertIntEquals(tc,28,array1[4]);
	CuAssertIntEquals(tc,33,array1[5]);
	CuAssertIntEquals(tc,31,array1[6]);
	CuAssertIntEquals(tc,301,array1[7]);
	CuAssertIntEquals(tc,170,array1[8]);
	CuAssertIntEquals(tc,297,array1[9]);
	// Get values from column 1 (aka 2)
	uint64_t* array12 = setRowIdsValuesToArray(rList, 0, initRelations, 0, 1, 1);
	CuAssertPtrNotNull(tc,array12);
	CuAssertIntEquals(tc,8463,array12[0]);
	CuAssertIntEquals(tc,9259,array12[1]);
	CuAssertIntEquals(tc,7833,array12[2]);
	CuAssertIntEquals(tc,5165,array12[3]);
	CuAssertIntEquals(tc,4854,array12[4]);
	CuAssertIntEquals(tc,6973,array12[5]);
	CuAssertIntEquals(tc,9334,array12[6]);
	CuAssertIntEquals(tc,8550,array12[7]);
	CuAssertIntEquals(tc,6322,array12[8]);
	CuAssertIntEquals(tc,4678,array12[9]);
	// Get values from column 2 (aka 3)
	uint64_t* array13 = setRowIdsValuesToArray(rList, 0, initRelations, 0, 2, 1);
	CuAssertPtrNotNull(tc,array13);
	CuAssertIntEquals(tc,582,array13[0]);
	CuAssertIntEquals(tc,315,array13[1]);
	CuAssertIntEquals(tc,834,array13[2]);
	CuAssertIntEquals(tc,6962,array13[3]);
	CuAssertIntEquals(tc,6374,array13[4]);
	CuAssertIntEquals(tc,1441,array13[5]);
	CuAssertIntEquals(tc,3092,array13[6]);
	CuAssertIntEquals(tc,7302,array13[7]);
	CuAssertIntEquals(tc,7314,array13[8]);
	CuAssertIntEquals(tc,8132,array13[9]);
	// Get array from rList from specific relation - rowIds
	uint64_t* array2 = setRowIdsValuesToArray(rList, 0, initRelations, 0, 0, 0);
	CuAssertPtrNotNull(tc,array2);
	CuAssertIntEquals(tc,0,array2[0]);
	CuAssertIntEquals(tc,3,array2[1]);
	CuAssertIntEquals(tc,4,array2[2]);
	CuAssertIntEquals(tc,1,array2[3]);
	CuAssertIntEquals(tc,7,array2[4]);
	CuAssertIntEquals(tc,9,array2[5]);
	CuAssertIntEquals(tc,8,array2[6]);
	CuAssertIntEquals(tc,100,array2[7]);
	CuAssertIntEquals(tc,55,array2[8]);
	CuAssertIntEquals(tc,99,array2[9]);
	// Get array from rList from specific relation diff - values
	uint64_t* array3 = setRowIdsValuesToArray(rList, 1, initRelations, 0, 0, 1);
	CuAssertPtrEquals(tc,NULL,array3);
	// Invalid params
	uint64_t* array4 = setRowIdsValuesToArray(NULL, 1, initRelations, 0, 0, 1);
	CuAssertPtrEquals(tc,NULL,array4);
	uint64_t* array5 = setRowIdsValuesToArray(rList, -1, initRelations, 0, 0, 1);
	CuAssertPtrEquals(tc,NULL,array5);
	uint64_t* array6 = setRowIdsValuesToArray(rList, 1, NULL, 0, 0, 1);
	CuAssertPtrEquals(tc,NULL,array6);
	uint64_t* array7 = setRowIdsValuesToArray(rList, 1, initRelations, -4, 0, 1);
	CuAssertPtrEquals(tc,NULL,array7);
	uint64_t* array8 = setRowIdsValuesToArray(rList, 1, initRelations, 0, -5, 1);
	CuAssertPtrEquals(tc,NULL,array8);
	uint64_t* array9 = setRowIdsValuesToArray(rList, 1, initRelations, 0, 0, 2);
	CuAssertPtrEquals(tc,NULL,array9);
	uint64_t* array10 = setRowIdsValuesToArray(rList, 5, initRelations, 0, 0, 2);
	CuAssertPtrEquals(tc,NULL,array10);
	uint64_t* array11 = setRowIdsValuesToArray(rList, 6, initRelations, 0, 0, 2);
	CuAssertPtrEquals(tc,NULL,array11);
	// Delete structs
	for(int i=0;i<7;i++){
		if(rList[i].rowIds != NULL){
			deleteRowIdList(&rList[i].rowIds);
		}
	}
	free(rList);
	deleteRelationsData(initRelations, &num_of_initRelations);
	free(array1);
	free(array2);
	free(array3);
	free(array12);
	free(array13);
}

CuSuite* ImplementationGetSuite() {
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestCreateRowIdList);
    SUITE_ADD_TEST(suite, TestInsertIntoRowIdList);
    SUITE_ADD_TEST(suite, TestDeleteRowIdList);
    SUITE_ADD_TEST(suite, TestSetRowIdsValuesToArray);
    return suite;
}

