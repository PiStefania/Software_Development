#include <stdio.h>
#include <stdlib.h>
#include "rowIdArrayMethods.h"

// Create a list in rowIdsList to store the rowIds found to satisfy the predicates
rowIdsArray* createRowIdsArray(int relationId){
	rowIdsArray* rArray = malloc(sizeof(rowIdsArray));
	rArray->relationId = relationId;
	rArray->num_of_rowIds = DEFAULT_ROWS;
	rArray->position = 0;
	rArray->rowIds = malloc(rArray->num_of_rowIds*sizeof(uint64_t));
	return rArray;
}


// Insert strings into the list (helps while reading files)
int insertIntoRowIdsArray(rowIdsArray* rArray, int rowId){
	if(rArray == NULL){
		return -1;
	}
	// double array of ids if necessary
	if(rArray->position == rArray->num_of_rowIds){
		doubleRowIdsArray(rArray);
	}

	rArray->rowIds[rArray->position] = rowId;
	rArray->position++;
	return 1;
}

void doubleRowIdsArray(rowIdsArray* rArray){
	rArray->num_of_rowIds *= 2;
	rArray->rowIds = realloc(rArray->rowIds, rArray->num_of_rowIds*sizeof(uint64_t));
}


// Delete rowIdsArray
void deleteRowIdsArray(rowIdsArray** rArray){
	free((*rArray)->rowIds);
	(*rArray)->rowIds = NULL;
	free((*rArray));
	*rArray = NULL;
}


void printRowIdsArray(rowIdsArray* rArray, int noOfRelations){
	for (int i = 0; i < noOfRelations; i++) {
		printf("----Relation: %d----\n",rArray[i].relationId);
		for(int j=0;j<rArray[i].position;j++){
			printf("%ld\n",rArray[i].rowIds[j]);
		}
	}
}

// params: rArray: array of relations and found rowIds
// params: position: position of relationId we want from rArray
// params: initRelations: helpful for getting values of specific rowIds
// params: relationId: get values from initRelations from specific relationId
// params: relColumn: column for getting value from initRelations
// params: type: type of setting (rowId:0,value:1)
/*uint64_t* setRowIdsValuesToArray(rowIdsArray** rArray, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity) {
	if (rArray == NULL || position < 0 || initRelations == NULL || relationId < 0 || relColumn < 0 || type < 0 || type > 1) {
		return NULL;
	}
	if (rArray[position]->position == 0 || rArray[position]->rowIds == NULL) {
		return NULL;
	}
	if (rArray[position]->relationId != relationId) {
		printf("rowIds DONT MATCHHH!!!\n");
		return NULL;
	}
	uint64_t* returnedArray = calloc(rArray[position]->position, sizeof(uint64_t));
	int index = 0;
	for(int i=0;i<rArray[position]->position;i++){
		// Check if rowId is inside array of rowId - counter pairs
		if (!checkSameId(foundIds, rArray[position]->rowIds[i], *capacity, type)) {
			// If not, it is inserted
			if (type == 1) {
				foundIds[*capacity].rowId = rArray[position]->rowIds[i];
				foundIds[*capacity].value = 1;
			}
			(*capacity)++;
		}
		if (type == 1) {
			returnedArray[index] = initRelations[relationId].Rarray[relColumn][rArray[position]->rowIds[i]];
		} else {
			returnedArray[index] = rArray[position]->rowIds[i];
		}
		index++;
	}
	return returnedArray;
}*/



