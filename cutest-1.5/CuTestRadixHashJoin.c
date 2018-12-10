#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "CuTestRadixHashJoin.h"

#define HASHFUNC1VALUE1 200
#define HASHFUNC1VALUE2 211

void TestHashFunction1(CuTest *tc){
	int32_t result = hashFunction1(HASHFUNC1VALUE1);
	CuAssertIntEquals(tc,0,result);
	result = hashFunction1(HASHFUNC1VALUE2);
	CuAssertIntEquals(tc,3,result);
}

void TestHashFunction2(CuTest *tc) {
	int32_t result = hashFunction2(HASHFUNC1VALUE1);
	CuAssertIntEquals(tc,0,result);
	result = hashFunction2(HASHFUNC1VALUE2);
	CuAssertIntEquals(tc,3,result);
}

void TestCreateNode(CuTest *tc) {
	struct node * newNode = createNode();
	CuAssertPtrNotNull(tc, newNode);
	CuAssertPtrEquals(tc, NULL, newNode->next);
	CuAssertIntEquals(tc, 0, newNode->num_of_elems);
	int i;
	for (i=0; i < ARRAYSIZE; i++) {
		CuAssertIntEquals(tc, -1, newNode->array[i].rowId1);
		CuAssertIntEquals(tc, -1, newNode->array[i].rowId2);
	}
	free(newNode);
}

void TestCreateList(CuTest *tc) {
	result* list = createList();
	CuAssertPtrNotNull(tc, list);
	CuAssertPtrNotNull(tc, list->head);
	deleteList(&list);
}

void TestInsertToList(CuTest *tc) {
	//change ARRAYSIZE to 5 in radixHashJoin.h in order to work correctly
	result* list = NULL;
	//insert to first node
	int result = insertToList(&list,11,23);
	CuAssertPtrNotNull(tc, list);
	resultNode* temp = list->head;
	CuAssertPtrNotNull(tc, temp);
	CuAssertIntEquals(tc, 11, temp->array[0].rowId1);
	CuAssertIntEquals(tc, 23, temp->array[0].rowId2);
	CuAssertIntEquals(tc, 1, temp->num_of_elems);
	//insert until new node
	result = insertToList(&list,1,2);
	result = insertToList(&list,3,4);
	result = insertToList(&list,46,54);
	result = insertToList(&list,7,30);
	result = insertToList(&list,8,9);
	temp = list->head->next;
	CuAssertPtrNotNull(tc, temp);
	CuAssertIntEquals(tc, 8, temp->array[0].rowId1);
	CuAssertIntEquals(tc, 9, temp->array[0].rowId2);
	CuAssertIntEquals(tc, 1, temp->num_of_elems);
	deleteList(&list);
}

void TestDeleteList(CuTest *tc) {
	//change ARRAYSIZE to 5 in radixHashJoin.h in order to work correctly
	result* list = NULL;
	//insert to first node
	int result = insertToList(&list,11,23);
	//delete list with only one record
	deleteList(&list);
	CuAssertPtrEquals(tc, NULL, list);
	//insert until new node
	result = insertToList(&list,1,2);
	result = insertToList(&list,3,4);
	result = insertToList(&list,46,54);
	result = insertToList(&list,7,30);
	result = insertToList(&list,8,9);
	result = insertToList(&list,47,98);
	//delete list with one new node
	deleteList(&list);
	CuAssertPtrEquals(tc, NULL, list);
	list = NULL;
	//delete NULL pointer
	deleteList(&list);
	CuAssertPtrEquals(tc, NULL, list);
}

void TestCreateRelation(CuTest *tc) {
	uint64_t* col = malloc(5*sizeof(uint64_t));
	col[0] = 2;
	col[1] = 3;
	col[2] = 5;
	col[3] = 45;
	col[4] = 34;
	relation* rel = createRelation(col,5);
	CuAssertPtrNotNull(tc, rel);
	CuAssertPtrNotNull(tc, rel->tuples);
	CuAssertIntEquals(tc, 5, rel->num_tuples);
	for (int i=0; i < 5; i++) {
		CuAssertIntEquals(tc, i, rel->tuples[i].rowId);
		CuAssertIntEquals(tc, col[i], rel->tuples[i].value);
	}
	deleteRelation(&rel);
	free(col);
}

void TestDeleteRelation(CuTest *tc) {
	//delete normal relation
	uint64_t* col = malloc(5*sizeof(uint64_t));
	col[0] = 2;
	col[1] = 3;
	col[2] = 5;
	col[3] = 45;
	col[4] = 34;
	relation* rel = createRelation(col,5);
	deleteRelation(&rel);
	CuAssertPtrEquals(tc, NULL, rel);
	//delete NULL relation
	rel = NULL;
	deleteRelation(&rel);
	CuAssertPtrEquals(tc, NULL, rel);
	free(col);
}

void TestCreateHistogram(CuTest *tc) {
	//create Hist from normal relation
	uint64_t* col = malloc(5*sizeof(uint64_t));
	col[0] = 2;
	col[1] = 3;
	col[2] = 5;
	col[3] = 45;
	col[4] = 34;
	relation* rel = createRelation(col,5);
	relation* Hist = createHistogram(rel);
	CuAssertPtrNotNull(tc, Hist);
	CuAssertIntEquals(tc, BUCKETS, Hist->num_tuples);
	CuAssertPtrNotNull(tc, Hist->tuples);
	CuAssertIntEquals(tc, 0, Hist->tuples[0].value);
	CuAssertIntEquals(tc, 2, Hist->tuples[1].value);
	CuAssertIntEquals(tc, 2, Hist->tuples[2].value);
	CuAssertIntEquals(tc, 1, Hist->tuples[3].value);
	CuAssertIntEquals(tc, 0, Hist->tuples[0].rowId);
	CuAssertIntEquals(tc, 1, Hist->tuples[1].rowId);
	CuAssertIntEquals(tc, 2, Hist->tuples[2].rowId);
	CuAssertIntEquals(tc, 3, Hist->tuples[3].rowId);
	//create Hist from NULL relation
	deleteRelation(&rel);
	deleteRelation(&Hist);
	Hist = createHistogram(rel);
	CuAssertPtrEquals(tc, NULL, Hist);
	//create Hist from empty relation
	rel = malloc(sizeof(relation));
	rel->tuples = NULL;
	rel->num_tuples = 0;
	Hist = createHistogram(rel);
	CuAssertPtrEquals(tc, NULL, Hist);
	free(col);
	deleteRelation(&rel);
}

void TestCreatePsum(CuTest *tc) {
	//create Psum from normal Hist
	uint64_t* col = malloc(5*sizeof(uint64_t));
	col[0] = 2;
	col[1] = 3;
	col[2] = 5;
	col[3] = 45;
	col[4] = 34;
	relation* rel = createRelation(col,5);
	relation* Hist = createHistogram(rel);
	relation* Psum = createPsum(Hist);
	CuAssertPtrNotNull(tc, Psum);
	CuAssertIntEquals(tc, BUCKETS, Psum->num_tuples);
	CuAssertPtrNotNull(tc, Psum->tuples);
	CuAssertIntEquals(tc, -1, Psum->tuples[0].value);
	CuAssertIntEquals(tc, 0, Psum->tuples[1].value);
	CuAssertIntEquals(tc, 2, Psum->tuples[2].value);
	CuAssertIntEquals(tc, 4, Psum->tuples[3].value);
	CuAssertIntEquals(tc, 0, Psum->tuples[0].rowId);
	CuAssertIntEquals(tc, 1, Psum->tuples[1].rowId);
	CuAssertIntEquals(tc, 2, Psum->tuples[2].rowId);
	CuAssertIntEquals(tc, 3, Psum->tuples[3].rowId);
	//create Hist from NULL relation
	deleteRelation(&rel);
	deleteRelation(&Hist);
	deleteRelation(&Psum);
	Psum = createPsum(Hist);
	CuAssertPtrEquals(tc, NULL, Psum);
	//create Hist from empty relation
	Hist = malloc(sizeof(relation));
	Hist->tuples = NULL;
	Hist->num_tuples = 0;
	Psum = createPsum(Hist);
	CuAssertPtrEquals(tc, NULL, Psum);
	free(col);
	deleteRelation(&Hist);
}

void TestCreateROrdered(CuTest *tc) {
	//create ROrdered normal
	uint64_t* col = malloc(5*sizeof(uint64_t));
	col[0] = 2;
	col[1] = 3;
	col[2] = 5;
	col[3] = 45;
	col[4] = 34;
	relation* rel = createRelation(col,5);
	relation* Hist = createHistogram(rel);
	relation* Psum = createPsum(Hist);
	relation* ROrdered = createROrdered(rel,Hist,Psum);
	CuAssertPtrNotNull(tc, ROrdered);
	CuAssertIntEquals(tc, rel->num_tuples, ROrdered->num_tuples);
	CuAssertPtrNotNull(tc, ROrdered->tuples);
	CuAssertIntEquals(tc, 5, ROrdered->tuples[0].value);
	CuAssertIntEquals(tc, 45, ROrdered->tuples[1].value);
	CuAssertIntEquals(tc, 2, ROrdered->tuples[2].value);
	CuAssertIntEquals(tc, 34, ROrdered->tuples[3].value);
	CuAssertIntEquals(tc, 3, ROrdered->tuples[4].value);
	CuAssertIntEquals(tc, 2, ROrdered->tuples[0].rowId);
	CuAssertIntEquals(tc, 3, ROrdered->tuples[1].rowId);
	CuAssertIntEquals(tc, 0, ROrdered->tuples[2].rowId);
	CuAssertIntEquals(tc, 4, ROrdered->tuples[3].rowId);
	CuAssertIntEquals(tc, 1, ROrdered->tuples[4].rowId);
	//create ROrdered from NULL relation
	deleteRelation(&rel);
	deleteRelation(&Hist);
	deleteRelation(&Psum);
	deleteRelation(&ROrdered);
	ROrdered = createROrdered(rel,Hist,Psum);
	CuAssertPtrEquals(tc, NULL, Psum);
	//create ROrdered from Hist with empty tuples
	Hist = malloc(sizeof(relation));
	Hist->tuples = NULL;
	Hist->num_tuples = 0;
	ROrdered = createROrdered(rel,Hist,Psum);
	CuAssertPtrEquals(tc, NULL, Psum);
	free(col);
	deleteRelation(&Hist);
}

void TestIndexCompareJoin(CuTest *tc) {
	//check partial match
	result* ResultList = createList();
	uint64_t* colR = malloc(5*sizeof(uint64_t));
	colR[0] = 2;
	colR[1] = 3;
	colR[2] = 5;
	colR[3] = 45;
	colR[4] = 34;
	relation* relR = createRelation(colR,5);
	relation* RHist = createHistogram(relR);
	relation* RPsum = createPsum(RHist);
	relation* ROrdered = createROrdered(relR,RHist,RPsum);
	uint64_t* colS = malloc(5*sizeof(uint64_t));
	colS[0] = 45;
	colS[1] = 4;
	colS[2] = 6;
	colS[3] = 1;
	colS[4] = 3;
	relation* relS = createRelation(colS,5);
	relation* SHist = createHistogram(relS);
	relation* SPsum = createPsum(SHist);
	relation* SOrdered = createROrdered(relS,SHist,SPsum);
	int result = indexCompareJoin(ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum);
	CuAssertIntEquals(tc, 0, result);
	CuAssertPtrNotNull(tc, ResultList);
	CuAssertPtrNotNull(tc, ResultList->head);
	CuAssertIntEquals(tc, 2, ResultList->head->num_of_elems);
	CuAssertIntEquals(tc, 3, ResultList->head->array[0].rowId1);
	CuAssertIntEquals(tc, 0, ResultList->head->array[0].rowId2);
	CuAssertIntEquals(tc, 1, ResultList->head->array[1].rowId1);
	CuAssertIntEquals(tc, 4, ResultList->head->array[1].rowId2);
	//free
	deleteRelation(&relR);
	deleteRelation(&RHist);
	deleteRelation(&RPsum);
	deleteRelation(&ROrdered);
	deleteRelation(&relS);
	deleteRelation(&SHist);
	deleteRelation(&SPsum);
	deleteRelation(&SOrdered);
	deleteList(&ResultList);
	//check no match
	ResultList = createList();
	colR[0] = 2;
	colR[1] = 3;
	colR[2] = 5;
	colR[3] = 45;
	colR[4] = 34;
	relR = createRelation(colR,5);
	RHist = createHistogram(relR);
	RPsum = createPsum(RHist);
	ROrdered = createROrdered(relR,RHist,RPsum);
	colS[0] = 1;
	colS[1] = 6;
	colS[2] = 88;
	colS[3] = 35;
	colS[4] = 98;
	relS = createRelation(colS,5);
	SHist = createHistogram(relS);
	SPsum = createPsum(SHist);
	SOrdered = createROrdered(relS,SHist,SPsum);
	result = indexCompareJoin(ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum);
	CuAssertIntEquals(tc, 0, result);
	CuAssertPtrNotNull(tc, ResultList);
	CuAssertPtrNotNull(tc, ResultList->head);
	CuAssertIntEquals(tc, 0, ResultList->head->num_of_elems);
	//free
	deleteRelation(&relR);
	deleteRelation(&RHist);
	deleteRelation(&RPsum);
	deleteRelation(&ROrdered);
	deleteRelation(&relS);
	deleteRelation(&SHist);
	deleteRelation(&SPsum);
	deleteRelation(&SOrdered);
	deleteList(&ResultList);
	//check full match
	ResultList = createList();
	colR[0] = 2;
	colR[1] = 3;
	colR[2] = 5;
	colR[3] = 45;
	colR[4] = 34;
	relR = createRelation(colR,5);
	RHist = createHistogram(relR);
	RPsum = createPsum(RHist);
	ROrdered = createROrdered(relR,RHist,RPsum);
	colS[0] = 45;
	colS[1] = 3;
	colS[2] = 5;
	colS[3] = 2;
	colS[4] = 34;
	relS = createRelation(colS,5);
	SHist = createHistogram(relS);
	SPsum = createPsum(SHist);
	SOrdered = createROrdered(relS,SHist,SPsum);
	result = indexCompareJoin(ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum);
	CuAssertIntEquals(tc, 0, result);
	CuAssertPtrNotNull(tc, ResultList);
	CuAssertPtrNotNull(tc, ResultList->head);
	CuAssertIntEquals(tc, 5, ResultList->head->num_of_elems);
	CuAssertIntEquals(tc, 2, ResultList->head->array[0].rowId1);
	CuAssertIntEquals(tc, 2, ResultList->head->array[0].rowId2);
	CuAssertIntEquals(tc, 3, ResultList->head->array[1].rowId1);
	CuAssertIntEquals(tc, 0, ResultList->head->array[1].rowId2);
	CuAssertIntEquals(tc, 0, ResultList->head->array[2].rowId1);
	CuAssertIntEquals(tc, 3, ResultList->head->array[2].rowId2);
	CuAssertIntEquals(tc, 4, ResultList->head->array[3].rowId1);
	CuAssertIntEquals(tc, 4, ResultList->head->array[3].rowId2);
	CuAssertIntEquals(tc, 1, ResultList->head->array[4].rowId1);
	CuAssertIntEquals(tc, 1, ResultList->head->array[4].rowId2);
	//free
	deleteRelation(&relR);
	deleteRelation(&RHist);
	deleteRelation(&RPsum);
	deleteRelation(&ROrdered);
	deleteRelation(&relS);
	deleteRelation(&SHist);
	deleteRelation(&SPsum);
	deleteRelation(&SOrdered);
	deleteList(&ResultList);
	free(colS);
	free(colR);
}


CuSuite* RadixHashJoinGetSuite() {
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestHashFunction1);
    SUITE_ADD_TEST(suite, TestHashFunction2);
	SUITE_ADD_TEST(suite, TestCreateNode);
	SUITE_ADD_TEST(suite, TestCreateList);
	//SUITE_ADD_TEST(suite, TestInsertToList); 	-> change radixHashJoin.h file (set ARRAYSIZE to 5) and uncomment specific test
	//SUITE_ADD_TEST(suite, TestDeleteList);	-> change radixHashJoin.h file (set ARRAYSIZE to 5) and uncomment specific test
	SUITE_ADD_TEST(suite, TestCreateRelation);
	SUITE_ADD_TEST(suite, TestDeleteRelation);
	SUITE_ADD_TEST(suite, TestCreateHistogram);
	SUITE_ADD_TEST(suite, TestCreatePsum);
	SUITE_ADD_TEST(suite, TestCreateROrdered);
	SUITE_ADD_TEST(suite, TestIndexCompareJoin);

    return suite;
}
