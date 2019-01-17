#ifndef CU_TEST_STATISTICS_METHODS_H
#define CU_TEST_STATISTICS_METHODS_H
#include "CuTest.h"
#include "../include/statisticsMethods.h"

void TestCheckCompareStatistics(CuTest *tc);
void TestUpdateCompareStatistics(CuTest *tc);
void TestUpdateJoinStatistics(CuTest *tc);
void TestCopyMetadata(CuTest *tc);

CuSuite* StatisticsMethodsGetSuite();

#endif