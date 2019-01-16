#ifndef CU_TEST_RADIX_HASH_JOIN_H
#define CU_TEST_RADIX_HASH_JOIN_H
#include "CuTest.h"
#include "../include/radixHashJoin.h"

void TestHashFunction1(CuTest *tc);
void TestHashFunction2(CuTest *tc);
void TestCreateNode(CuTest *tc);
void TestCreateList(CuTest *tc);
void TestInsertToList(CuTest *tc);
void TestDeleteList(CuTest *tc);
void TestCreateRelation(CuTest *tc);
void TestDeleteRelation(CuTest *tc);
void TestCreateHistogram(CuTest *tc);
void TestCreatePsum(CuTest *tc);
void TestCreateROrdered(CuTest *tc);
void TestIndexCompareJoin(CuTest *tc);
void TestCreateHistogramThread(CuTest *tc);

CuSuite* RadixHashJoinGetSuite();

#endif
