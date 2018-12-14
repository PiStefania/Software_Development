#ifndef CU_TEST_RELATION_METHODS_H
#define CU_TEST_RELATION_METHODS_H
#include "CuTest.h"
#include "../relationMethods.h"

void TestGetRelationsData(CuTest *tc);
void TestDeleteRelationsData(CuTest *tc);
void TestCreateNameList(CuTest *tc);
void TestInsertIntoNameList(CuTest *tc);
void TestFindNameByIndex(CuTest *tc);
void TestDeleteNameList(CuTest *tc);

CuSuite* RelationMethodsGetSuite();

#endif

