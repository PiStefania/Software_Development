#ifndef CU_TEST_ROW_ID_ARRAY_METHODS_H
#define CU_TEST_ROW_ID_ARRAY_METHODS_H
#include "CuTest.h"
#include "../include/rowIdArrayMethods.h"

void TestCreateRowIdsArray(CuTest *tc);
void TestInsertIntoRowIdsArray(CuTest *tc);
void TestDoubleRowIdsArray(CuTest *tc);
void TestDeleteRowIdsArray(CuTest *tc);

CuSuite* RowIdArrayMethodsGetSuite();

#endif