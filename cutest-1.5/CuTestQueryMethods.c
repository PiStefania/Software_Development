#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestQueryMethods.h"

void TestGetRelationsFromLine(CuTest *tc){
	int relationsSize;
	// Test normal string
	char* normalStr = malloc((strlen("0 2 4    7")+1)*sizeof(char));
	strcpy(normalStr,"0 2 4 7");
	int* relations1 = getRelationsFromLine(normalStr, &relationsSize);
	CuAssertPtrNotNull(tc,relations1);
	CuAssertIntEquals(tc,0,relations1[0]);
	CuAssertIntEquals(tc,2,relations1[1]);
	CuAssertIntEquals(tc,4,relations1[2]);
	CuAssertIntEquals(tc,7,relations1[3]);
	CuAssertIntEquals(tc,4,relationsSize);
	free(normalStr);
	free(relations1);
	// Test null string
	int* relations2 = getRelationsFromLine(NULL, &relationsSize);
	CuAssertPtrEquals(tc,NULL,relations2);
	CuAssertIntEquals(tc,0,relationsSize);
	// Test non numeric relations
	char* weirdStr = malloc((strlen("0  1k 2")+1)*sizeof(char));
	strcpy(weirdStr,"0  1k 2");
	int* relations3 = getRelationsFromLine(weirdStr, &relationsSize);
	CuAssertPtrEquals(tc,NULL,relations3);
	CuAssertIntEquals(tc,0,relationsSize);
	free(weirdStr);
}


void TestGetProjectionsFromLine(CuTest *tc){
	int projectionsSize;
	// Test normal string
	char* normalStr = malloc((strlen("0.1 2.4")+1)*sizeof(char));
	strcpy(normalStr,"0.1 2.4");
	tuple* projections1 = getProjectionsFromLine(normalStr, &projectionsSize);
	CuAssertPtrNotNull(tc,projections1);
	CuAssertIntEquals(tc,0,projections1[0].rowId);
	CuAssertIntEquals(tc,1,projections1[0].value);
	CuAssertIntEquals(tc,2,projections1[1].rowId);
	CuAssertIntEquals(tc,4,projections1[1].value);
	CuAssertIntEquals(tc,2,projectionsSize);
	free(normalStr);
	free(projections1);
	// Test null string
	tuple* projections2 = getProjectionsFromLine(NULL, &projectionsSize);
	CuAssertPtrEquals(tc,NULL,projections2);
	CuAssertIntEquals(tc,0,projectionsSize);
	// Test non numeric projections
	char* weirdStr1 = malloc((strlen("0 1.2")+1)*sizeof(char));
	strcpy(weirdStr1,"0 1.2");
	tuple* projections3 = getProjectionsFromLine(weirdStr1, &projectionsSize);
	CuAssertPtrEquals(tc,NULL,projections3);
	CuAssertIntEquals(tc,0,projectionsSize);
	free(weirdStr1);
	char* weirdStr2 = malloc((strlen("0.k 1.2")+1)*sizeof(char));
	strcpy(weirdStr2,"0.k 1.2");
	tuple* projections4 = getProjectionsFromLine(weirdStr2, &projectionsSize);
	CuAssertPtrEquals(tc,NULL,projections3);
	CuAssertIntEquals(tc,0,projectionsSize);
	free(weirdStr2);
}

void TestGetPredicatesFromLine(CuTest *tc){
	int predicatesSize;
	// Test normal string
	char* normalStr = malloc((strlen("0.1=2.4&2.1>4")+1)*sizeof(char));
	strcpy(normalStr,"0.1=2.4&2.1>4");
	predicate** predicate1 = getPredicatesFromLine(normalStr, &predicatesSize);
	CuAssertPtrNotNull(tc,predicate1);
	CuAssertIntEquals(tc,2,predicate1[0]->leftSide->rowId);
	CuAssertIntEquals(tc,1,predicate1[0]->leftSide->value);
	CuAssertIntEquals(tc,4,predicate1[0]->rightSide->rowId);
	CuAssertIntEquals(tc,-1,predicate1[0]->rightSide->value);
	CuAssertIntEquals(tc,'>',predicate1[0]->comparator);
	CuAssertIntEquals(tc,0,predicate1[0]->kind);
	CuAssertIntEquals(tc,2,predicatesSize);
	CuAssertIntEquals(tc,0,predicate1[1]->leftSide->rowId);
	CuAssertIntEquals(tc,1,predicate1[1]->leftSide->value);
	CuAssertIntEquals(tc,2,predicate1[1]->rightSide->rowId);
	CuAssertIntEquals(tc,4,predicate1[1]->rightSide->value);
	CuAssertIntEquals(tc,'=',predicate1[1]->comparator);
	CuAssertIntEquals(tc,1,predicate1[1]->kind);
	free(normalStr);
	deletePredicate(&predicate1[0]);
	deletePredicate(&predicate1[1]);
	free(predicate1);
	// Test null string
	predicate** predicate2 = getPredicatesFromLine(NULL, &predicatesSize);
	CuAssertPtrEquals(tc,NULL,predicate2);
	CuAssertIntEquals(tc,0,predicatesSize);
}

void TestCreatePredicate(CuTest *tc){
	// Create with size > 0
	predicate** p = createPredicate(4);
	CuAssertPtrNotNull(tc,p);
	for(int i=0;i<4;i++){
		CuAssertPtrNotNull(tc,p[i]);
		CuAssertPtrNotNull(tc,p[i]->leftSide);
		CuAssertIntEquals(tc,-1,p[i]->leftSide->rowId);
		CuAssertIntEquals(tc,-1,p[i]->leftSide->value);
		CuAssertPtrNotNull(tc,p[i]->rightSide);
		CuAssertIntEquals(tc,-1,p[i]->rightSide->rowId);
		CuAssertIntEquals(tc,-1,p[i]->rightSide->value);
		CuAssertIntEquals(tc,'0',p[i]->comparator);
		CuAssertIntEquals(tc,-1,p[i]->kind);
	}
	for(int i=0;i<4;i++){
		deletePredicate(&p[i]);
	}
	free(p);
	// Create with size <=0
	p = createPredicate(0);
	CuAssertPtrEquals(tc,NULL,p);
	p = createPredicate(-3);
	CuAssertPtrEquals(tc,NULL,p);
}


void TestDeletePredicate(CuTest *tc){
	predicate** p = createPredicate(4);
	for(int i=0;i<4;i++){
		deletePredicate(&p[i]);
		CuAssertPtrEquals(tc,NULL,p[i]);
	}
	free(p);
}


void TestSetPredicate(CuTest *tc){
	// Set kind 1 predicate
	int result = 0;
	predicate* pred = malloc(sizeof(predicate));
	char* predicateStr = malloc((strlen("0.1=2.2")+1)*sizeof(char));
	pred->leftSide = malloc(sizeof(tuple));
	pred->leftSide->rowId = -1;
	pred->leftSide->value = -1;
	pred->rightSide = malloc(sizeof(tuple));
	pred->rightSide->rowId = -1;
	pred->rightSide->value = -1;
	pred->comparator = '0';
	pred->kind = -1;
	strcpy(predicateStr,"0.1=2.2");
	result = setPredicate(predicateStr,&pred,NULL,0);
	CuAssertIntEquals(tc,0,result);
	CuAssertPtrNotNull(tc,pred->leftSide);
	CuAssertIntEquals(tc,0,pred->leftSide->rowId);
	CuAssertIntEquals(tc,1,pred->leftSide->value);
	CuAssertPtrNotNull(tc,pred->rightSide);
	CuAssertIntEquals(tc,2,pred->rightSide->rowId);
	CuAssertIntEquals(tc,2,pred->rightSide->value);
	CuAssertIntEquals(tc,'=',pred->comparator);
	CuAssertIntEquals(tc,1,pred->kind);
	free(predicateStr);
	deletePredicate(&pred);
	// Set kind 0 predicate
	predicateStr = malloc((strlen("2.3>20")+1)*sizeof(char));
	strcpy(predicateStr,"2.3>20");
	pred = malloc(sizeof(predicate));
	pred->leftSide = malloc(sizeof(tuple));
	pred->leftSide->rowId = -1;
	pred->leftSide->value = -1;
	pred->rightSide = malloc(sizeof(tuple));
	pred->rightSide->rowId = -1;
	pred->rightSide->value = -1;
	pred->comparator = '0';
	pred->kind = -1;
	result = setPredicate(predicateStr,&pred,NULL,0);
	CuAssertIntEquals(tc,0,result);
	CuAssertPtrNotNull(tc,pred);
	CuAssertPtrNotNull(tc,pred->leftSide);
	CuAssertIntEquals(tc,2,pred->leftSide->rowId);
	CuAssertIntEquals(tc,3,pred->leftSide->value);
	CuAssertPtrNotNull(tc,pred->rightSide);
	CuAssertIntEquals(tc,20,pred->rightSide->rowId);
	CuAssertIntEquals(tc,-1,pred->rightSide->value);
	CuAssertIntEquals(tc,'>',pred->comparator);
	CuAssertIntEquals(tc,0,pred->kind);
	free(predicateStr);
	deletePredicate(&pred);
	// Set NULL string
	result = setPredicate(NULL,&pred,NULL,0);
	CuAssertIntEquals(tc,-1,result);
	CuAssertPtrEquals(tc,NULL,pred);
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

void TestCheckPredicate(CuTest *tc){
	// Initialize predicates
	int result = 0;
	predicate* pred1 = malloc(sizeof(predicate));
	pred1->leftSide = malloc(sizeof(tuple));
	pred1->leftSide->rowId = 0;
	pred1->leftSide->value = 1;
	pred1->rightSide = malloc(sizeof(tuple));
	pred1->rightSide->rowId = 1;
	pred1->rightSide->value = 0;
	pred1->needsToBeDeleted = 0;
	pred1->comparator = '=';
	pred1->kind = 1;
	predicate* pred2 = malloc(sizeof(predicate));
	pred2->leftSide = malloc(sizeof(tuple));
	pred2->leftSide->rowId = 0;
	pred2->leftSide->value = 1;
	pred2->rightSide = malloc(sizeof(tuple));
	pred2->rightSide->rowId = 1;
	pred2->rightSide->value = 0;
	pred2->needsToBeDeleted = 0;
	pred2->comparator = '=';
	pred2->kind = 1;
	// Check same
	result = checkPredicate(pred1,pred2);
	CuAssertIntEquals(tc,1,result);
	predicate* pred3 = malloc(sizeof(predicate));
	pred3->leftSide = malloc(sizeof(tuple));
	pred3->leftSide->rowId = 1;
	pred3->leftSide->value = 0;
	pred3->rightSide = malloc(sizeof(tuple));
	pred3->rightSide->rowId = 0;
	pred3->rightSide->value = 1;
	pred3->needsToBeDeleted = 0;
	pred3->comparator = '=';
	pred3->kind = 1;
	// Check same
	result = checkPredicate(pred1,pred3);
	CuAssertIntEquals(tc,1,result);

	predicate* pred4 = malloc(sizeof(predicate));
	pred4->leftSide = malloc(sizeof(tuple));
	pred4->leftSide->rowId = 0;
	pred4->leftSide->value = 2;
	pred4->rightSide = malloc(sizeof(tuple));
	pred4->rightSide->rowId = 2000;
	pred4->rightSide->value = -1;
	pred4->needsToBeDeleted = 0;
	pred4->comparator = '>';
	pred4->kind = 0;
	predicate* pred5 = malloc(sizeof(predicate));
	pred5->leftSide = malloc(sizeof(tuple));
	pred5->leftSide->rowId = 0;
	pred5->leftSide->value = 2;
	pred5->rightSide = malloc(sizeof(tuple));
	pred5->rightSide->rowId = 2000;
	pred5->rightSide->value = -1;
	pred5->needsToBeDeleted = 0;
	pred5->comparator = '>';
	pred5->kind = 0;
	predicate* pred9 = malloc(sizeof(predicate));
	pred9->leftSide = malloc(sizeof(tuple));
	pred9->leftSide->rowId = 2;
	pred9->leftSide->value = 2;
	pred9->rightSide = malloc(sizeof(tuple));
	pred9->rightSide->rowId = 1001;
	pred9->rightSide->value = -1;
	pred9->needsToBeDeleted = 0;
	pred9->comparator = '>';
	pred9->kind = 0;
	// Check same
	result = checkPredicate(pred4,pred5);
	CuAssertIntEquals(tc,1,result);
	predicate* pred6 = malloc(sizeof(predicate));
	pred6->leftSide = malloc(sizeof(tuple));
	pred6->leftSide->rowId = 2;
	pred6->leftSide->value = 2;
	pred6->rightSide = malloc(sizeof(tuple));
	pred6->rightSide->rowId = 90;
	pred6->rightSide->value = -1;
	pred6->needsToBeDeleted = 0;
	pred6->comparator = '<';
	pred6->kind = 0;
	predicate* pred7 = malloc(sizeof(predicate));
	pred7->leftSide = malloc(sizeof(tuple));
	pred7->leftSide->rowId = 2;
	pred7->leftSide->value = 2;
	pred7->rightSide = malloc(sizeof(tuple));
	pred7->rightSide->rowId = 90;
	pred7->rightSide->value = -1;
	pred7->needsToBeDeleted = 0;
	pred7->comparator = '<';
	pred7->kind = 0;
	predicate* pred8 = malloc(sizeof(predicate));
	pred8->leftSide = malloc(sizeof(tuple));
	pred8->leftSide->rowId = 2;
	pred8->leftSide->value = 2;
	pred8->rightSide = malloc(sizeof(tuple));
	pred8->rightSide->rowId = 91;
	pred8->rightSide->value = -1;
	pred8->needsToBeDeleted = 0;
	pred8->comparator = '<';
	pred8->kind = 0;
	// Check same
	result = checkPredicate(pred6,pred7);
	CuAssertIntEquals(tc,1,result);
	// Check not same
	result = checkPredicate(pred1,pred2);
	CuAssertIntEquals(tc,1,result);
	result = checkPredicate(pred1,pred4);
	CuAssertIntEquals(tc,0,result);
	result = checkPredicate(pred4,pred6);
	CuAssertIntEquals(tc,0,result);
	result = checkPredicate(pred7,pred8);
	CuAssertIntEquals(tc,0,result);
	result = checkPredicate(pred5,pred9);
	CuAssertIntEquals(tc,0,result);
	// Check for deleted
	pred3->needsToBeDeleted = 1;
	result = checkPredicate(pred1,pred3);
	CuAssertIntEquals(tc,0,result);
	// Check for NULL
	result = checkPredicate(NULL,pred3);
	CuAssertIntEquals(tc,-1,result);
	result = checkPredicate(pred1,NULL);
	CuAssertIntEquals(tc,-1,result);
	
	//delete
	deletePredicate(&pred1);
	deletePredicate(&pred2);
	deletePredicate(&pred3);
	deletePredicate(&pred4);
	deletePredicate(&pred5);
	deletePredicate(&pred6);
	deletePredicate(&pred7);
	deletePredicate(&pred8);
	deletePredicate(&pred9);
}


CuSuite* QueryMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestGetRelationsFromLine);
    SUITE_ADD_TEST(suite, TestGetProjectionsFromLine);
    SUITE_ADD_TEST(suite, TestGetPredicatesFromLine);
    SUITE_ADD_TEST(suite, TestCreatePredicate);
    SUITE_ADD_TEST(suite, TestDeletePredicate);
    SUITE_ADD_TEST(suite, TestSetPredicate);
    SUITE_ADD_TEST(suite, TestIsNumeric);
    SUITE_ADD_TEST(suite, TestCheckPredicate);
    return suite;
}

