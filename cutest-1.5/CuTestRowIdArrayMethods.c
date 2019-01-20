#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRowIdArrayMethods.h"

void TestCreateRowIdsArray(CuTest *tc){
	// Create rowIdsArray normally, with relation id 2
	rowIdsArray* rArray = createRowIdsArray(2);
	CuAssertPtrNotNull(tc,rArray);
	CuAssertIntEquals(tc,2,rArray->relationId);
	CuAssertIntEquals(tc,1000,rArray->num_of_rowIds);
	CuAssertIntEquals(tc,0,rArray->position);
	CuAssertPtrNotNull(tc,rArray->rowIds);
	// Create rowIdsArray with negative relationId
	rowIdsArray* rArray1 = createRowIdsArray(-1);
	CuAssertPtrEquals(tc,NULL,rArray1);
	// Delete rowIdsArray
	deleteRowIdsArray(&rArray);
}

void TestInsertIntoRowIdsArray(CuTest *tc){
	rowIdsArray* rArray = NULL;
	// Insert to empty rArray
	int result = insertIntoRowIdsArray(rArray,4);
	CuAssertIntEquals(tc,-1,result);
	rArray = createRowIdsArray(2);
	// Insert one id
	result = insertIntoRowIdsArray(rArray,4);
	CuAssertIntEquals(tc,1,result);
	CuAssertIntEquals(tc,1,rArray->position);
	CuAssertIntEquals(tc,1000,rArray->num_of_rowIds);
	CuAssertIntEquals(tc,4,rArray->rowIds[rArray->position-1]);
	// Insert until double
	for(int i=0;i<999;i++){
		insertIntoRowIdsArray(rArray,4);
	}
	result = insertIntoRowIdsArray(rArray,86);
	CuAssertIntEquals(tc,1,result);
	CuAssertIntEquals(tc,1001,rArray->position);
	CuAssertIntEquals(tc,2000,rArray->num_of_rowIds);
	CuAssertIntEquals(tc,86,rArray->rowIds[rArray->position-1]);
	// Delete rowIdsArray
	deleteRowIdsArray(&rArray);
}

void TestDoubleRowIdsArray(CuTest *tc){
	rowIdsArray* rArray = createRowIdsArray(2);
	// Double rArray
	doubleRowIdsArray(rArray);
	CuAssertIntEquals(tc,2000,rArray->num_of_rowIds);
	CuAssertPtrNotNull(tc,rArray->rowIds);
	// Delete rowIdsArray
	deleteRowIdsArray(&rArray);
}

void TestDeleteRowIdsArray(CuTest *tc){
	// Create rArray
	rowIdsArray* rArray = createRowIdsArray(2);
	// Delete rowIdsArray
	deleteRowIdsArray(&rArray);
	CuAssertPtrEquals(tc,NULL,rArray);
	// Create only rowIdsArray
	rArray = malloc(sizeof(rowIdsArray));
	rArray->rowIds = NULL;
	deleteRowIdsArray(&rArray);
	CuAssertPtrEquals(tc,NULL,rArray);
}

CuSuite* RowIdArrayMethodsGetSuite() {
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestCreateRowIdsArray);
    SUITE_ADD_TEST(suite, TestInsertIntoRowIdsArray);
    SUITE_ADD_TEST(suite, TestDoubleRowIdsArray);
    SUITE_ADD_TEST(suite, TestDeleteRowIdsArray);
    return suite;
}