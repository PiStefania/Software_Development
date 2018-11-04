#ifndef CU_TEST_RADIX_HASH_JOIN_H
#define CU_TEST_RADIX_HASH_JOIN_H
#include "CuTest.h"
#include "../radixHashJoin.h"

void TestHashFunction1(CuTest *tc);
void TestHashFunction2(CuTest *tc);
void TestcreateNode(CuTest *tc);

CuSuite* RadixHashJoinGetSuite();

#endif
