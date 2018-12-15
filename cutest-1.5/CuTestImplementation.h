#ifndef CU_TEST_IMPLEMENTATION_H
#define CU_TEST_IMPLEMENTATION_H
#include "CuTest.h"
#include "../implementation.h"

void TestCreateRowIdList(CuTest *tc);
void TestInsertIntoRowIdList(CuTest *tc);
void TestDeleteRowIdList(CuTest *tc);

CuSuite* ImplementationGetSuite();

#endif

