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