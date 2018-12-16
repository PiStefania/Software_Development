#ifndef CU_TEST_IMPLEMENTATION_H
#define CU_TEST_IMPLEMENTATION_H
#include "CuTest.h"
#include "../implementation.h"
#include "../relationMethods.h"

void TestCreateRowIdList(CuTest *tc);
void TestInsertIntoRowIdList(CuTest *tc);
void TestDeleteRowIdList(CuTest *tc);
void TestSetRowIdsValuesToArray(CuTest *tc);

CuSuite* ImplementationGetSuite();

#endif

