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
	CuAssertIntEquals(tc,0,predicate1[0]->leftSide->rowId);
	CuAssertIntEquals(tc,1,predicate1[0]->leftSide->value);
	CuAssertIntEquals(tc,2,predicate1[0]->rightSide->rowId);
	CuAssertIntEquals(tc,4,predicate1[0]->rightSide->value);
	CuAssertIntEquals(tc,'=',predicate1[0]->comparator);
	CuAssertIntEquals(tc,1,predicate1[0]->kind);
	CuAssertIntEquals(tc,2,predicatesSize);
	CuAssertIntEquals(tc,2,predicate1[1]->leftSide->rowId);
	CuAssertIntEquals(tc,1,predicate1[1]->leftSide->value);
	CuAssertIntEquals(tc,4,predicate1[1]->rightSide->rowId);
	CuAssertIntEquals(tc,-1,predicate1[1]->rightSide->value);
	CuAssertIntEquals(tc,'>',predicate1[1]->comparator);
	CuAssertIntEquals(tc,0,predicate1[1]->kind);
	CuAssertIntEquals(tc,2,predicatesSize);
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

/*
void TestSetPredicate(CuTest *tc){
	// Set kind 1 predicate
	predicate* pred = NULL;
	char* predicateStr = malloc((strlen("0.1=2.2")+1)*sizeof(char));
	strcpy(predicateStr,"0.1=0.2");
	setPredicate(predicateStr,pred);
	//CuAssertPtrNotNull(tc,pred);
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
	setPredicate(predicateStr,pred);
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

}*/


CuSuite* QueryMethodsGetSuite() {		
    CuSuite* suite = CuSuiteNew();
    SUITE_ADD_TEST(suite, TestGetRelationsFromLine);
    SUITE_ADD_TEST(suite, TestGetProjectionsFromLine);
    SUITE_ADD_TEST(suite, TestGetPredicatesFromLine);
    SUITE_ADD_TEST(suite, TestCreatePredicate);
    SUITE_ADD_TEST(suite, TestDeletePredicate);
    //SUITE_ADD_TEST(suite, TestSetPredicate);
    return suite;
}

