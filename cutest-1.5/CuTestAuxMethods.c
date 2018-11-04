#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "CuTestAuxMethods.h"

#define RANGE 17							// Range of values for arrays initialization (1 to RANGE)
#define R_ROWS 7							// Number of rows for R array
#define COLUMNS 1							// Colums of arrays (for this program, only 1)

void TestGetColumnOfArray(CuTest *tc){
	//Create randomly filled array
	srand(time(NULL));
	int32_t** R = malloc(R_ROWS * sizeof(int*));
	//Check if malloc is ok
	CuAssertPtrNotNull(tc,R);
	for (int i = 0; i < R_ROWS; i++) {
		R[i] = malloc(COLUMNS * sizeof(int));
		CuAssertPtrNotNull(tc,R[i]);
	}
  	for (int i = 0; i < R_ROWS; i++) {
    	for (int j = 0; j < COLUMNS; j++) {
      		R[i][j] = (rand() % RANGE) + 1;
    	}
  	}
	
	//Get the requested column (for this implementation we need just one)
	//That's why COLUMNS is set to 1 above (R, S have only one field, e.g: a)
	int* Rcolumn = getColumnOfArray(R, R_ROWS, 0);
	//Check if column is the right one
	CuAssertIntEquals(tc,R[0][0],Rcolumn[0]);
	CuAssertIntEquals(tc,R[1][0],Rcolumn[1]);
	CuAssertIntEquals(tc,R[2][0],Rcolumn[2]);
	CuAssertIntEquals(tc,R[3][0],Rcolumn[3]);
	CuAssertIntEquals(tc,R[4][0],Rcolumn[4]);
	CuAssertIntEquals(tc,R[5][0],Rcolumn[5]);
	CuAssertIntEquals(tc,R[6][0],Rcolumn[6]);
	
	//Get out-of-range column
	int* RcolumnOutPositive = getColumnOfArray(R, R_ROWS, 1);
	//Ptr should be NULL
	CuAssertPtrEquals(tc,NULL,RcolumnOutPositive);
	
	//Get out-of-range column with negative value
	int* RcolumnOutNegative = getColumnOfArray(R, R_ROWS, -1);
	//Ptr should be NULL
	CuAssertPtrEquals(tc,NULL,RcolumnOutNegative);
	
	//Delete array
	for (int i = 0; i < R_ROWS; i++) {
		free(R[i]);
	}
	free(R);
	free(Rcolumn);
	free(RcolumnOutPositive);
	free(RcolumnOutNegative);
}

CuSuite* AuxMethodsGetSuite() {		//adding TestAuxMethods Functions into suite
    CuSuite* suite = CuSuiteNew();
    
    SUITE_ADD_TEST(suite, TestGetColumnOfArray);
    
    return suite;
}

