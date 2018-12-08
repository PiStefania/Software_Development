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

void TestIsNumeric(CuTest *tc){
	//if string is actually a number
	int result;
	char* number = malloc(5*sizeof(char));
	strcpy(number,"2012");
	result = isNumeric(number);
	CuAssertTrue(tc,result);
	//if string is not a number
	char* notANumber = malloc(5*sizeof(char));
	strcpy(notANumber,"etct");
	result = isNumeric(notANumber);
	CuAssertTrue(tc,!result);
	//if string is partially a number (2 cases)
	char* partiallyANumber1 = malloc(5*sizeof(char));
	strcpy(partiallyANumber1,"etc1");
	result = isNumeric(partiallyANumber1);
	CuAssertTrue(tc,!result);
	char* partiallyANumber2 = malloc(5*sizeof(char));
	strcpy(partiallyANumber2,"13et");
	result = isNumeric(partiallyANumber2);
	CuAssertTrue(tc,!result);
	//if string is null
	result = isNumeric(NULL);
	CuAssertTrue(tc,!result);
	//if string is a number with scaling zeros
	char* scaling = malloc(5*sizeof(char));
	strcpy(scaling,"0012");
	result = isNumeric(scaling);
	CuAssertTrue(tc,result);
	//free arrays
	free(number);
	free(notANumber);
	free(partiallyANumber1);
	free(partiallyANumber2);
	free(scaling);
}


CuSuite* AuxMethodsGetSuite() {		//adding TestAuxMethods Functions into suite
    CuSuite* suite = CuSuiteNew();
    
    SUITE_ADD_TEST(suite, TestGetColumnOfArray);
    SUITE_ADD_TEST(suite, TestIsNumeric);

    return suite;
}

