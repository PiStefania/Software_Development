#include <stdio.h>
#include <stdlib.h>
#include "rowIdListMethods.h"
#include "relationMethods.h"
#include "queryMethods.h"
#include "radixHashJoin.h"



int checkSameId(tuple* foundIds, uint32_t rowId, int capacity, char checkFlag) {
	for (int i = 0; i < capacity; i++) {
		// If rowId is the same as the one given
		if (foundIds[i].rowId == rowId) {
			if (checkFlag == 1) {
				// Increment number of times found
				foundIds[i].value++;
			}
			return 1;
		}
	}
	return 0;
}


// Create a list in rowIdsList to store the rowIds found to satisfy the predicates
rowIdNode* createRowIdList() {
	rowIdNode* list;
	if ((list = malloc(sizeof(rowIdNode))) == NULL) return NULL;
	list->isEmptyList = 1;
	list->next = NULL;
	list->rowId = -1;
	return list;
}


// Insert strings into the list (helps while reading files)
int insertIntoRowIdList(rowIdNode** list, int rowId) {
	if (list == NULL || rowId < 0) {
		return -1;
	}
	rowIdNode *newNode;
	if ((*list)->isEmptyList == 1){
		(*list)->rowId = rowId;
		(*list)->isEmptyList = 0;
		(*list)->next = NULL;
		return 1;
	}

	if ((newNode = malloc(sizeof(rowIdNode))) == NULL) return -1;
	newNode->rowId = rowId;
    newNode->isEmptyList = 0;
	newNode->next = *list;
	*list = newNode;
	return 1;
}


// Delete above list
void deleteRowIdList(rowIdNode** list) {
	rowIdNode* tempNode;
	rowIdNode* currentNode = *list;
	while (currentNode != NULL){
		tempNode = currentNode;
		currentNode = currentNode->next;
		free(tempNode);
	}
	*list = NULL;
}


void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations){
	for (int i = 0; i < noOfRelations; i++) {
		printf("----Relation: %d----\n",rowIdsList[i].relationId);
		rowIdNode* temp = rowIdsList[i].rowIds;
		while (temp != NULL) {
			printf("%d ",temp->rowId);
			temp = temp->next;
		}
	}
}

// params: rList: list of relations and found rowIds
// params: position: position of relationId we want from rList
// params: initRelations: helpful for getting values of specific rowIds
// params: relationId: get values from initRelations from specific relationId
// params: relColumn: column for getting value from initRelations
// params: type: type of setting (rowId:0,value:1)
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity) {
	if (rList == NULL || position < 0 || initRelations == NULL || relationId < 0 || relColumn < 0 || type < 0 || type > 1) {
		return NULL;
	}
	if (rList[position].num_of_rowIds == 0 || rList[position].rowIds == NULL) {
		return NULL;
	}
	if (rList[position].relationId != relationId) {
		printf("rowIds DONT MATCHHH!!!\n");
		return NULL;
	}
	uint64_t* returnedArray = calloc(rList[position].num_of_rowIds, sizeof(uint64_t));
	rowIdNode* temp = rList[position].rowIds;
	int index = 0;
	while (temp != NULL) {
		// Check if rowId is inside array of rowId - counter pairs
		if (!checkSameId(foundIds, temp->rowId, *capacity, type)) {
			// If not, it is inserted
			if (type == 1) {
				foundIds[*capacity].rowId = temp->rowId;
				foundIds[*capacity].value = 1;
			}
			(*capacity)++;
		}
		if (type == 1) {
			returnedArray[index] = initRelations[relationId].Rarray[relColumn][temp->rowId];
		} else {
			returnedArray[index] = temp->rowId;
		}
		temp = temp->next;
		index++;
	}
	return returnedArray;
}


// Search for previous predicates need to be updated
void searchOutdatedPredicates(predicate** predicates, tuple* projections, char *outdatedPredicates, int currentPredicate, int predicatesSize, int projectionsSize) {
    // We update only the join predicates, not the compare ones
    for (int j = 0; j < predicatesSize; j++) {
        // Each position same to predicates array - 0 good / 1 outdated, needs to be executed again
        outdatedPredicates[j] = 0;
    }
    // After join, look for outdated join predicates (if a member of current predicate has already been used in another join)
    for (int j = 0; j < currentPredicate; j++) {
        if (predicates[j]->kind != 0) {
            uint64_t iLeftRow = predicates[currentPredicate]->leftSide->rowId;
            uint64_t iLeftColumn = predicates[currentPredicate]->leftSide->value;
            uint64_t iRightRow = predicates[currentPredicate]->rightSide->rowId;
            uint64_t iRightColumn = predicates[currentPredicate]->rightSide->value;
            uint64_t jLeftRow = predicates[j]->leftSide->rowId;
            uint64_t jLeftColumn = predicates[j]->leftSide->value;
            uint64_t jRightRow = predicates[j]->rightSide->rowId;
            uint64_t jRightColumn = predicates[j]->rightSide->value;
            if ((iLeftRow == jLeftRow && iLeftColumn == jLeftColumn) || (iRightRow == jLeftRow && iRightColumn == jLeftColumn)) {
                if (iLeftRow == jRightRow || iRightRow == jRightRow){
                    continue;
                }
                for (int k = 0; k < projectionsSize; k++) {
                    if (projections[k].rowId == jRightRow) {
                        outdatedPredicates[j] = 1;
                        break;
                    }
                    else continue;
                }
            }
            else if ((iLeftRow == jRightRow && iLeftColumn == jRightColumn) || (iRightRow == jRightRow && iRightColumn == jRightColumn)) {
                if (iLeftRow == jLeftRow || iRightRow == jLeftRow){
                    continue;
                }
                for (int k = 0; k < projectionsSize; k++) {
                    if (projections[k].rowId == jLeftRow) {
                        outdatedPredicates[j] = 2;
                        break;
                    }
                    else continue;
                }
            }
        }
        else continue;
    }
}
