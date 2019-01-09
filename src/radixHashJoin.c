#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/radixHashJoin.h"


//Create new node to add to list
resultNode * createNode() {
    resultNode * newNode;

    if ((newNode = malloc(sizeof(resultNode))) == NULL) {
        return NULL;
    }
    //initialize newNode
    newNode->next = NULL;
    newNode->num_of_elems = 0;
    for (int i=0; i<ARRAYSIZE; i++) {
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
int insertToList(result** list, uint32_t rowID1, uint32_t rowID2) {

    if (*list == NULL) {
      *list = createList();
    }

    resultNode * temp = (*list)->head;

    while (temp->num_of_elems == ARRAYSIZE) {
        if (temp->next == NULL) {
            temp->next = createNode();
        }
        temp = temp->next;
    }

	//insert to current node (new node)
	temp->array[temp->num_of_elems].rowId1 = rowID1;
	temp->array[temp->num_of_elems].rowId2 = rowID2;
	temp->num_of_elems++;

	return 0;
}

void printList(result * list) {
    resultNode * curr = list->head;
    printf("1st Relation's RowID (R)--------2nd Relation's RowID (S)\n");
    while (curr != NULL) {
        for (int i=0; i < curr->num_of_elems; i++) {
            printf("%8d %31d\n", curr->array[i].rowId1, curr->array[i].rowId2);
        }
        curr = curr->next;
    }
    return;
}

void deleteList(result ** list) {
	if(list == NULL || *list == NULL)
		return;
    resultNode * curr = (*list)->head;
    if(curr == NULL)
    	return;
    resultNode * prev;
    while (curr->next != NULL) {
        prev = curr;
        curr = curr->next;
        free(prev);
    }
    free(curr);
    free(*list);
    *list = NULL;
    return;
}

//H1 for bucket selection, get last 2 bits
int hashFunction1(uint64_t value){
	return value & HEXBUCKETS;
}

//H2 for indexing in buckets, get last 3 bits
int hashFunction2(uint64_t value){
	return value & HEXHASH2;
}

//create relation array for field
relation* createRelation(uint64_t* col, uint64_t* rowIds, uint64_t noOfElems){
	relation* rel = malloc(sizeof(relation));
	rel->tuples = malloc(noOfElems*sizeof(tuple));
	for (int i = 0; i < noOfElems; i++){
        if (rowIds == NULL) {
            rel->tuples[i].rowId = i;
        } else {
            rel->tuples[i].rowId = rowIds[i];
        }
		rel->tuples[i].value = col[i];
	}
	rel->num_tuples = noOfElems;
	return rel;
}

relation* createRelationFromRarray(rowIdsArray* rArray, relationsInfo* initRelations, int relationId, int relColumn, foundIds* foundIdsRelation){
	relation* rel = malloc(sizeof(relation));
	rel->tuples = malloc(rArray->position*sizeof(tuple));
	for (int i = 0; i < rArray->position; i++){
        rel->tuples[i].rowId = rArray->rowIds[i];
		rel->tuples[i].value = initRelations[relationId].Rarray[relColumn][rArray->rowIds[i]];
		// Insert to intermediate's field with sorting
		//insertionSortFoundIds(foundIdsRelation, rArray->rowIds[i]);
		insertIdsHash(foundIdsRelation, rArray->rowIds[i]);
	}
	rel->num_tuples = rArray->position;
	return rel;
}

void deleteRelation(relation** rel){
	if(*rel == NULL)
		return;
	if((*rel)->tuples == NULL){
		free(*rel);
		*rel = NULL;
		return;
	}
	free((*rel)->tuples);
	(*rel)->tuples = NULL;
	free(*rel);
	*rel = NULL;
}

void printRelation(relation* rel){
	printf("Relation has %d tuples\n",rel->num_tuples);
	for (int i=0;i<rel->num_tuples;i++){
		printf("Row with rowId: %ld and value: %ld\n",rel->tuples[i].rowId,rel->tuples[i].value);
	}
}

// Create the Histogram, where we store the number of elements corresponding with each hash value
// The size of tuples array is the number of buckets (one position for each hash value)
relation* createHistogram(relation* R){
	if(R == NULL)
		return NULL;
	if(R->tuples == NULL || R->num_tuples == 0)
		return NULL;
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
	if(Hist == NULL)
		return NULL;
	if(Hist->tuples == NULL || Hist->num_tuples == 0)
		return NULL;
	relation* Psum = malloc(sizeof(relation));
	Psum->num_tuples = BUCKETS;
	Psum->tuples = malloc(BUCKETS * sizeof(tuple));
	uint32_t sum = 0;
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
	if(R == NULL || Hist == NULL || Psum == NULL)
		return NULL;
	if(R != NULL){
		if(R->tuples == NULL)
			return NULL;
	}
	if(Hist != NULL){
		if(Hist->tuples == NULL)
			return NULL;
	}
	if(Psum != NULL){
		if(Psum->tuples == NULL)
			return NULL;
	}

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
		int hashId = hashFunction1(R->tuples[i].value);
		int offset = Hist->tuples[hashId].value - RemainHist->tuples[hashId].value;		// Total hash items - hash items left
		int ElementNewPosition = Psum->tuples[hashId].value + offset;					// Position = bucket's position + offset
		RemainHist->tuples[hashId].value--;
		ROrdered->tuples[ElementNewPosition].rowId = R->tuples[i].rowId; 				// Copy the element form old to new array
		ROrdered->tuples[ElementNewPosition].value = R->tuples[i].value;
	}
	// Delete the RemainHist Array
	deleteRelation(&RemainHist);
	return ROrdered;
}


//Create indexes for each bucket in the smaller one, compare the items of bigger with smaller's and finally join the same values (return in the list rowIds)
// We create indexes for each bucket, one by one, for the smaller bucket of the 2 arrays (for optimization)
int indexCompareJoin(result* ResultList, relation* ROrdered, relation* RHist, relation* RPsum, relation* SOrdered, relation* SHist, relation* SPsum) {
    for (int i = 0; i < BUCKETS; i++) {
		// Find which of the 2 buckets (from R and S array) is the smaller one, in order to create the index in that one
        relation *smallOrdered, *smallHist, *smallPsum, *bigOrdered, *bigHist, *bigPsum;
        if (RHist->tuples[i].value < SHist->tuples[i].value) {
            smallOrdered = ROrdered;
            smallHist    = RHist;
            smallPsum    = RPsum;
            bigOrdered   = SOrdered;
            bigHist      = SHist;
            bigPsum      = SPsum;
        }
        else {
            smallOrdered = SOrdered;
            smallHist    = SHist;
            smallPsum    = SPsum;
            bigOrdered   = ROrdered;
            bigHist      = RHist;
            bigPsum      = RPsum;
        }
		int itemsInSmallBucket = smallHist->tuples[i].value;
		int chain[itemsInSmallBucket];
		int bucket[HASH2];
		for (int j = 0; j < HASH2; j++) {             // Initialize bucket array with -1
			bucket[j] = -1;
		}
        // Create the index for smaller bucket with a second hash value
		for (int j = 0; j < itemsInSmallBucket; j++){
			int itemSmallOrderedOffset = smallPsum->tuples[i].value + j;
			int hash2Id = hashFunction2(smallOrdered->tuples[itemSmallOrderedOffset].value);
			if (bucket[hash2Id] == -1)
				chain[j] = -1;                      // The first item hashed (2) with current value, is set to -1
			else chain[j] = bucket[hash2Id];        // Write the last position to chain and current to bucket array
			bucket[hash2Id] = j;
		}
        // Print Chain and Bucket arrays (indexing on smaller bucket)
        if (PRINT) {
            char arrayStr[3];
            if (RHist->tuples[i].value < SHist->tuples[i].value)
                strcpy(arrayStr, "R");
            else strcpy(arrayStr, "S");
        }
		// Search all the items from unindexed bigger bucket in the same bucket and compare them with smaller's
		int itemsInBigBucket = bigHist->tuples[i].value;
		for (int j = 0; j < itemsInBigBucket; j++){
			int itemBigOrderedOffset = bigPsum->tuples[i].value + j;
			int hash2Id = hashFunction2(bigOrdered->tuples[itemBigOrderedOffset].value);
			if (bucket[hash2Id] != -1) {
				int currentInChain = bucket[hash2Id];       // Search each item from bigger to the similarly hashed ones from smaller (help from bucket and chain)
				do {
					int itemSmallOrderedOffset = smallPsum->tuples[i].value + currentInChain;
					if (smallOrdered->tuples[itemSmallOrderedOffset].value == bigOrdered->tuples[itemBigOrderedOffset].value) {
                        int itemROrderedOffset, itemSOrderedOffset;
                        if (RHist->tuples[i].value < SHist->tuples[i].value) {
                            itemROrderedOffset = itemSmallOrderedOffset;
                            itemSOrderedOffset = itemBigOrderedOffset;
                        }                                                       // First field of result tuples is for R's rowId while second for S's rowId
                        else {
                            itemROrderedOffset = itemBigOrderedOffset;
                            itemSOrderedOffset = itemSmallOrderedOffset;
                        }
						if (insertToList(&ResultList, ROrdered->tuples[itemROrderedOffset].rowId, SOrdered->tuples[itemSOrderedOffset].rowId)) {
							printf("Error\n");
                            return -1;                      // Insert the rowIds of same valued tuples in Result List (if error return)
						}
					}
					currentInChain = chain[currentInChain];        // Go on in chain to compare other similar items of smaller with the current one from bigger
				} while (currentInChain != -1);                    // When a chain item is -1, then there is no similar tuple from smaller left
			}
		}
	}
    return 0;
}

// Create Histogram with thread
void createHistogramThread(histArgs* args){
	relation* R = args->R;
	relation** Hist = args->Hist;
	if(R == NULL)
		return;
	if(R->tuples == NULL || R->num_tuples == 0)
		return;
	*Hist = malloc(sizeof(relation));
	(*Hist)->num_tuples = BUCKETS;
	(*Hist)->tuples = malloc(BUCKETS * sizeof(tuple));
	//initialize Hist
	for(int i=0;i<(*Hist)->num_tuples;i++){
		(*Hist)->tuples[i].rowId = i;
		(*Hist)->tuples[i].value = 0;
	}
	//populate Hist
	for(int i=0;i<R->num_tuples;i++){
		int bucket = hashFunction1(R->tuples[i].value);
		(*Hist)->tuples[bucket].value++;
	}
}

// For update
foundIds* initializeFoundIds(){
	foundIds* foundIdsRelation = malloc(sizeof(foundIds));
	foundIdsRelation->length = DEFAULT_ROWS;
	foundIdsRelation->position = 0;
	foundIdsRelation->idsHash = malloc(foundIdsRelation->length*sizeof(tuple));
	return foundIdsRelation;
}

void doubleFoundIds(foundIds* foundIdsRelation){
	foundIdsRelation->length *= 2;
	foundIdsRelation->idsHash = realloc(foundIdsRelation->idsHash, foundIdsRelation->length*sizeof(tuple));
}

void deleteFoundIds(foundIds** foundIdsRelation){
	if(*foundIdsRelation != NULL){
		free((*foundIdsRelation)->idsHash);
		(*foundIdsRelation)->idsHash = NULL;
		free(*foundIdsRelation);
		*foundIdsRelation = NULL;
	}
}

void insertIdsHash(foundIds* foundIdsRelation, uint64_t rowId) {
	for (int i = 0; i < foundIdsRelation->position; i++) {
		// If rowId is the same as the one given
		if (foundIdsRelation->idsHash[i].rowId == rowId) {
			foundIdsRelation->idsHash[i].value++;
			return;
		}
	}

	// Not found -> insert
	if(foundIdsRelation->position == foundIdsRelation->length){
		doubleFoundIds(foundIdsRelation);
	}

	foundIdsRelation->idsHash[foundIdsRelation->position].rowId = rowId;
	foundIdsRelation->idsHash[foundIdsRelation->position].value = 1;
	foundIdsRelation->position++;
}

// Binary search foundIds->idsHash array depending on id and return its position
int binarySearchFoundIds(foundIds* fIds, uint64_t rowId){
	int first = 0;
	int last = fIds->position-2;
	if(last == -2){
		return 0;
	}
	int mid = 0;
	tuple* rowIds = fIds->idsHash;
	while (first < last){
        int mid = first + (last-first)/2;
        if(rowIds[mid].rowId == rowId)
            return mid;
        if (rowIds[mid].rowId < rowId)
            first = mid + 1;
		else
            last = mid - 1;
    }
	if(last<=first){
		if(rowId > fIds->idsHash[first].rowId)
			return first+1;
		else
			return first;
	}
    return mid;
}

// Insert a rowId to foundIds
void insertionSortFoundIds(foundIds* fIds, uint64_t rowId){
	int getPosition = binarySearchFoundIds(fIds,rowId);

	// Full array, need to double
	if(fIds->position == fIds->length){
		doubleFoundIds(fIds);
	}

	if(fIds->position == 0){
		// Insert to first position
		fIds->idsHash[0].rowId = rowId;
		fIds->idsHash[0].value = 1;
		fIds->position++;
	}else{
		if(getPosition < fIds->position){
			// If same id in position, increment counter value
			if(rowId == fIds->idsHash[getPosition].rowId){
				fIds->idsHash[getPosition].value++;
			}else{
				// Need to shift other elements to insert a new node with different rowId
				int fullMoveSize = (fIds->position - getPosition)*sizeof(fIds->idsHash[fIds->position]);
				memmove(&(fIds->idsHash[getPosition+1]), &(fIds->idsHash[getPosition]), fullMoveSize);
				fIds->idsHash[getPosition].rowId = rowId;
				fIds->idsHash[getPosition].value = 1;
				fIds->position++;
			}
		}else{
			fIds->idsHash[getPosition].rowId = rowId;
			fIds->idsHash[getPosition].value = 1;
		}
	}
}
