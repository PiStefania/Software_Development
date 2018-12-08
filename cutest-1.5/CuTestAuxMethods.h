#ifndef CU_TEST_AUX_METHODS_H
#define CU_TEST_AUX_METHODS_H
#include "CuTest.h"
#include "../auxMethods.h"

void TestGetColumnOfArray(CuTest *tc);
void TestIsNumeric(CuTest *tc);

CuSuite* AuxMethodsGetSuite();

#endif

