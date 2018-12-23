#ifndef CU_TEST_QUERY_METHODS_H
#define CU_TEST_QUERY_METHODS_H
#include "CuTest.h"
#include "../queryMethods.h"

void TestGetRelationsFromLine(CuTest *tc);
void TestGetProjectionsFromLine(CuTest *tc);
void TestGetPredicatesFromLine(CuTest *tc);
void TestCreatePredicate(CuTest *tc);
void TestDeletePredicate(CuTest *tc);
void TestSetPredicate(CuTest *tc);
void TestIsNumeric(CuTest *tc);
void TestCheckPredicate(CuTest *tc);

CuSuite* QueryMethodsGetSuite();

#endif

