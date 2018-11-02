#include <stdio.h>
#include <stdlib.h>
#include "radixHashJoin.h"

#define BUCKETS 8				// Number of buckets is 2^n, where n = num of last bits for hashing


//Create new node to add to list
resultNode * createNode() {
    resultNode * newNode;
    int i;

    if ((newNode = malloc(sizeof(resultNode))) == NULL) {
        return NULL;
    }

    //initialize newNode
    newNode->next = NULL;
    newNode->num_of_elems = 0;
    for (i=0; i<ARRAYSIZE; i++) {
        newNode->array[i].rowId1 = -1;
        newNode->array[i].rowId2 = -1;
    }

    return newNode;
}

result * createList() {
	result* list;

    if ((list = malloc(sizeof(result))) == NULL) {
        return NULL;
    }

    list->head = createNode();
	if (list->head == NULL) {
		return NULL;
	}

	return list;
}

//Create result as a list of arrays
int insertToList(result * list, int32_t rowID1, int32_t rowID2) {
    resultNode * temp = list->head;
	if (temp->num_of_elems < ARRAYSIZE) {
		//insert to current node
		temp->array[temp->num_of_elems].rowId1 = rowID1;
		temp->array[temp->num_of_elems].rowId2 = rowID2;
		temp->num_of_elems += 1;
	}
	else {
		//create and add new node, then continue
		temp->next = createNode();
		if (temp->next == NULL) {
			return -1;
		}
		temp = temp->next;

		//insert to current node (new node)
		temp->array[temp->num_of_elems].rowId1 = rowID1;
		temp->array[temp->num_of_elems].rowId2 = rowID2;
		temp->num_of_elems += 1;
	}
	return 0;
}

void printList(result * list) {
  resultNode * curr = list->head;
  printf("1st Relation's RowID--------2nd Relation's RowID\n");
  while (curr != NULL) {
    int i;
    for (i=0; i < curr->num_of_elems; i++) {
      printf("%8d %15d\n", curr->array[i].rowId1, curr->array[i].rowId2);
    }
    curr = curr->next;
  }

  return;
}

void deleteList(result * list) {
  resultNode * curr = list->head;
  resultNode * prev;
  while (curr->next != NULL) {
    prev = curr;
    curr = curr->next;
    free(prev);
  }
  free(curr);
  free(list);

  return;
}

//H1 for bucket selection, get last 3 bits
int32_t hashFunction1(int32_t value){
	return value & 0x7;
}

//create relation array for field
relation* createRelation(int* col, int noOfElems){
	relation* rel = malloc(sizeof(relation));
	rel->tuples = malloc(noOfElems*sizeof(tuple));
	for(int i=0;i<noOfElems;i++){
		rel->tuples[i].rowId = i;
		rel->tuples[i].value = col[i];
	}
	rel->num_tuples = noOfElems;
	return rel;
}


void deleteRelation(relation** rel){
	free((*rel)->tuples);
	(*rel)->tuples = NULL;
	free(*rel);
	*rel = NULL;
}

void printRelation(relation* rel){
	printf("Relation has %d tuples\n",rel->num_tuples);
	for(int i=0;i<rel->num_tuples;i++){
		printf("Row with rowId: %d and value: %d\n",rel->tuples[i].rowId,rel->tuples[i].value);
	}
}

// Create the Histogram, where we store the number of elements corresponding with each hash value
// The size of tuples array is the number of buckets (one position for each hash value)
relation* createHistogram(relation* R){
	relation* Hist = malloc(sizeof(relation));
	Hist->num_tuples = BUCKETS;
	Hist->tuples = malloc(BUCKETS * sizeof(tuple));
	//initialize Hist
	for(int i=0;i<Hist->num_tuples;i++){
		Hist->tuples[i].rowId = i;
		Hist->tuples[i].value = 0;
	}
	//populate Hist
	for(int i=0;i<R->num_tuples;i++){
		int bucket = hashFunction1(R->tuples[i].value);
		Hist->tuples[bucket].value++;
	}
	return Hist;
}

// Create Psum just like histogram, but for different purpose
// We calculate the position of the first element from each bucket in the new ordered array
relation* createPsum(relation* Hist){
	relation* Psum = malloc(sizeof(relation));
	Psum->num_tuples = BUCKETS;
	Psum->tuples = malloc(BUCKETS * sizeof(tuple));
	int32_t sum = 0;
	for(int i=0;i<Psum->num_tuples;i++){
		Psum->tuples[i].rowId = Hist->tuples[i].rowId;
		if (Hist->tuples[i].value == 0)
			Psum->tuples[i].value = -1;				// If there is no item for a hash value, we write -1
		else
			Psum->tuples[i].value = sum;
		sum += Hist->tuples[i].value;
	}
	return Psum;
}

// Create the new ordered array (R')
relation* createROrdered(relation* R, relation* Hist, relation* Psum){
	// Allocate a relation array same as R
	relation* ROrdered = malloc(sizeof(relation));
	ROrdered->num_tuples = R->num_tuples;
	ROrdered->tuples = malloc(ROrdered->num_tuples * sizeof(tuple));

	// Create a copy of Histogram, so as to use it while filling in the new ordered array
	// Whenever we copy an element from old array to the new one, reduce the counter of its hist id (in RemainHist)
	// In this way, we read the old array only one time - complexity O(n) (n = R->num_tuples)
	// We copy all the elements one by one, in the proper position of the new array
	relation* RemainHist = malloc(sizeof(relation));
	RemainHist->num_tuples = Hist->num_tuples;
	RemainHist->tuples = malloc(RemainHist->num_tuples * sizeof(tuple));
	for (int i = 0; i < Hist->num_tuples; i++) {
		RemainHist->tuples[i].rowId = Hist->tuples[i].rowId;
		RemainHist->tuples[i].value = Hist->tuples[i].value;
	}
	// Now copy the elements of old array to the new one by buckets (ordered)
	for (int i = 0; i < R->num_tuples; i++) {
		int32_t hashId = hashFunction1(R->tuples[i].value);
		int offset = Hist->tuples[hashId].value - RemainHist->tuples[hashId].value;		// Total hash items - hash items left
		int ElementNewPosition = Psum->tuples[hashId].value + offset;					// Position = bucket's position + offset
		RemainHist->tuples[hashId].value--;
		ROrdered->tuples[ElementNewPosition].rowId = R->tuples[i].rowId; 					// Copy the element form old to new array
		ROrdered->tuples[ElementNewPosition].value = R->tuples[i].value;
	}
	// Delete the RemainHist Array
	deleteRelation(&RemainHist);
	return ROrdered;
}
