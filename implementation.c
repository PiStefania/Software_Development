#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "implementation.h"
#include "relationMethods.h"
#include "queryMethods.h"
#include "radixHashJoin.h"


int queriesImplementation(FILE* file, relationsInfo* initRelations) {
	char *line = NULL;
	size_t len = 0;
	int read;
	int failed = 0;

	// If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		printf("Please input queries (End input with letter 'F'):\n");
		file = stdin;
	}

	while ((read = getline(&line, &len, file)) != -1) {
		// Get line and each section
		printf("%s", line);
		char* lineStr = strtok(line,"\n");
		char* relationsStr = strtok(lineStr,"|");
		char* predicatesStr = strtok(NULL,"|");
		char* projectionsStr = strtok(NULL,"");

		// Check for NULL Strings
		if (lineStr == NULL || relationsStr == NULL || predicatesStr == NULL || projectionsStr == NULL) {
			if (lineStr != NULL) {
				if (strcmp(lineStr,"F") != 0) {
					printf("Query line is incorrect, please provide another file or input correct statement.\n");
					failed = 1;
				}
			} else {
				continue;
			}
            if (file == stdin) {
                break;
            }
            continue;
		}

		// Get relations
		int* relations = NULL;
		int relationsSize = 0;
		if(relationsStr != NULL){
			relations = getRelationsFromLine(relationsStr,&relationsSize);
			if(relations == NULL){
				printf("Relations are incorrect!\n");
				failed = 1;
			}
		}

		// Get projections
		tuple* projections = NULL;
		int projectionsSize = 0;
		if (projectionsStr != NULL) {
			projections = getProjectionsFromLine(projectionsStr,&projectionsSize);
			if (projections == NULL) {
				printf("Projections are incorrect!\n");
				failed = 1;
			}
		}

        // List to store the rowIds of all predicates found
        rowIdsList* rList = NULL;

		// Get predicates
		predicate** predicates = NULL;
		int predicatesSize = 0;
		if (predicatesStr != NULL) {
			predicates = getPredicatesFromLine(predicatesStr,&predicatesSize);
			if (predicates == NULL) {
				printf("Predicates are incorrect!\n");
				failed = 1;
			} else {
				//get number of join predicates
				int joinPredicates = 0;
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 1)
						joinPredicates++;
				}
				// create resultlists as intermediate structures
				intermediate** intermediateStructs = malloc(joinPredicates*sizeof(intermediate*));
				for(int i=0;i<joinPredicates;i++){
					intermediateStructs[i] = malloc(sizeof(intermediate));
					intermediateStructs[i]->ResultList = createList();
				}
                // Create the list to store the rowIds that satisfy each predicate
                rList = malloc(relationsSize * sizeof(rowIdsList));
                for (int i = 0; i < relationsSize; i++) {
					rList[i].rowIds = createRowIdList();
                    rList[i].num_of_rowIds = 0;
				}
				int currentJoinPredicates = 0;
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 0) {
						//printPredicate(predicates[i]);
						// Get relation from line of predicate
                        int relationId1 = relations[predicates[i]->leftSide->rowId];
                        // Get column that we need to compare, from predicate
                        int relColumn = predicates[i]->leftSide->value;
                        //for each row of current relation, compare column, j is number of row aka id
                        for (int j = 0; j < initRelations[relationId1].num_of_rows; j++) {
                            if (predicates[i]->comparator == '=') {
                            	// Check if are equal
                                if (initRelations[relationId1].Rarray[relColumn][j] == predicates[i]->rightSide->rowId) {
                                	// Insert row id of predicare into rList of specific relation id
                                	int result = insertIntoRowIdList(&rList[predicates[i]->leftSide->rowId].rowIds, j);
                                    if (result == -1) return 0;
                                    else if (result == 1) {
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                            else if (predicates[i]->comparator == '>') {
                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
                                	int result = insertIntoRowIdList(&rList[predicates[i]->leftSide->rowId].rowIds, j);
                                    if (result == -1) return 0;
                                    else if (result == 1) {
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                            else if (predicates[i]->comparator == '<') {
                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
                                	int result = insertIntoRowIdList(&rList[predicates[i]->leftSide->rowId].rowIds, j);
                                    if (result == -1) return 0;
                                    else if (result == 1) {
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                        }
						if (rList[predicates[i]->leftSide->rowId].num_of_rowIds == 0) {
							rList[predicates[i]->leftSide->rowId].num_of_rowIds = -1;
						}
					} else {	// Join
                        // Call Radix Hash Join
                        intermediateStructs[currentJoinPredicates]->leftRelation = relations[predicates[i]->leftSide->rowId];
                        intermediateStructs[currentJoinPredicates]->rightRelation = relations[predicates[i]->rightSide->rowId];
						int result = joinColumns(relations, predicates, initRelations, rList, i,intermediateStructs[currentJoinPredicates]);
						currentJoinPredicates++;
                        if (result == -1) {
							return 0;
						}
						else if (result == 0) {
							continue;
						}
						// Create an array to store which predicates need an update after a repeatitive presence of a certain column of a relation
						char *outdatedPredicates;
						outdatedPredicates = malloc(predicatesSize * sizeof(char)); 	// We update only the join predicates, not the compare ones
						for (int i = 0; i < predicatesSize; i++) {
							outdatedPredicates[i] = 0;				// Each position same to predicates array - 0 good / 1 outdated, needs to be executed again
						}
						// After join, look for outdated join predicates (if a member of current predicate has already been used in another join)
						for (int j = 0; j < i; j++) {
							if (predicates[j]->kind != 0) {
								uint64_t iLeftRow = predicates[i]->leftSide->rowId;
								uint64_t iLeftColumn = predicates[i]->leftSide->value;
								uint64_t iRightRow = predicates[i]->rightSide->rowId;
								uint64_t iRightColumn = predicates[i]->rightSide->value;
								uint64_t jLeftRow = predicates[j]->leftSide->rowId;
								uint64_t jLeftColumn = predicates[j]->leftSide->value;
								uint64_t jRightRow = predicates[j]->rightSide->rowId;
								uint64_t jRightColumn = predicates[j]->rightSide->value;
								if ((iLeftRow == jLeftRow && iLeftColumn == jLeftColumn) || (iRightRow == jLeftRow && iRightColumn == jLeftColumn)) {
									if (iLeftRow == jRightRow || iRightRow == jRightRow) continue;
									for (int k = 0; k < projectionsSize; k++) {
										if (projections[k].rowId == jRightRow) {
											outdatedPredicates[j] = 1;
											break;
										}
										else continue;
									}
								}
								else if ((iLeftRow == jRightRow && iLeftColumn == jRightColumn) || (iRightRow == jRightRow && iRightColumn == jRightColumn)) {
									if (iLeftRow == jLeftRow || iRightRow == jLeftRow) continue;
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
						// Update outdated relations
						for (int i = 0; i < predicatesSize; i++) {
							if (outdatedPredicates[i] != 0) {
								int result = updatePredicates(predicates, rList, i, outdatedPredicates[i], intermediateStructs, joinPredicates, intermediateStructs[currentJoinPredicates-1]);
								if (result == -1) return 0;
							}
						}
						free(outdatedPredicates);
					}
				}

				// Delete intermediateStructs
				for(int i=0;i<currentJoinPredicates;i++){
					deleteList(&intermediateStructs[i]->ResultList);
					free(intermediateStructs[i]);
				}
				free(intermediateStructs);
			}
		}

        // Find final results (values summary)
        for (int i = 0; i < projectionsSize; i++) {
            uint64_t valueSummary = 0;
			rowIdNode* currentRowId;
			currentRowId = rList[projections[i].rowId].rowIds;
            for (int j = 0; j < rList[projections[i].rowId].num_of_rowIds; j++) {
                valueSummary += initRelations[relations[projections[i].rowId]].Rarray[projections[i].value][currentRowId->rowId];
				currentRowId = currentRowId->next;
            }
			if (valueSummary == 0){
				if(i==projectionsSize-1){
					printf("NULL");
				}else{
					printf("NULL ");
				}
			} else {
				if(i==projectionsSize-1){
					printf("%ld", valueSummary);
				}else{
					printf("%ld ", valueSummary);
				}
			}
        }
        printf("\n");

		// Free vars for each line
		if (relations) {
			free(relations);
			relations=NULL;
		}
		if (predicates) {
			for (int i = 0; i < predicatesSize; i++) {
				deletePredicate(&predicates[i]);
			}
			for (int i = 0; i < relationsSize; i++) {
				deleteRowIdList(&rList[i].rowIds);
			}
			free(predicates);
			predicates = NULL;
            free(rList);
			rList = NULL;
		}
		if (projections) {
			free(projections);
			projections=NULL;
		}
		if (failed) break;
	}
	// Free vars
	if (line) {
		free(line);
		line = NULL;
	}
	// Close file
	if (file != NULL  && file != stdin)
		fclose(file);

	if (failed) return 0;

	return 1;
}



int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate, intermediate* inter) {
    //create relation
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    relation* Rrel = NULL;
    tuple* foundIdsLeft = NULL;
    int capacityLeft = 0;
    if (rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds > 0) {
    	foundIdsLeft = malloc(rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds * sizeof(tuple));
		uint64_t* arrayValues = setRowIdsValuesToArray(rList, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1, 1, foundIdsLeft, &capacityLeft,1);
		capacityLeft = 0;
		uint64_t* arrayRowIds = setRowIdsValuesToArray(rList, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1, 0, foundIdsLeft, &capacityLeft,0);
    	Rrel = createRelation(arrayValues, arrayRowIds, rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds);
		free(arrayValues);
		free(arrayRowIds);
    }
	else if (rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds == 0) {
    	Rrel = createRelation(initRelations[relationId1].Rarray[relColumn1], NULL, initRelations[relationId1].num_of_rows);
    }
	else if (rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds == -1) {
		rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds = -1;
		return 0;
	}
    if (PRINT) printRelation(Rrel);

    //create histogram
    relation* RHist = createHistogram(Rrel);
    if (PRINT) printRelation(RHist);

    //create Psum
    relation* RPsum = createPsum(RHist);
    if (PRINT) printRelation(RPsum);

    //create ordered R
    relation* ROrdered = createROrdered(Rrel, RHist, RPsum);
    if (PRINT) printRelation(ROrdered);

    // Now the same procedure for S array
    int relationId2 = relations[predicates[currentPredicate]->rightSide->rowId];
    int relColumn2 = predicates[currentPredicate]->rightSide->value;
    relation* Srel = NULL;
    tuple* foundIdsRight = NULL;
    int capacityRight = 0;
    if (rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds > 0) {
    	foundIdsRight = malloc(rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds * sizeof(tuple));
    	uint64_t* arrayValues = setRowIdsValuesToArray(rList, predicates[currentPredicate]->rightSide->rowId, initRelations, relationId2, relColumn2, 1, foundIdsRight, &capacityRight,1);
		capacityRight = 0;
		uint64_t* arrayRowIds = setRowIdsValuesToArray(rList, predicates[currentPredicate]->rightSide->rowId, initRelations, relationId2, relColumn2, 0, foundIdsRight, &capacityRight,0);
    	Srel = createRelation(arrayValues, arrayRowIds, rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds);
    	free(arrayValues);
		free(arrayRowIds);
    }
	else if (rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds == 0) {
    	Srel = createRelation(initRelations[relationId2].Rarray[relColumn2], NULL, initRelations[relationId2].num_of_rows);
    }
	else if (rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds == -1) {
		rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds = -1;
		deleteRelation(&Rrel);
	    deleteRelation(&RHist);
	    deleteRelation(&RPsum);
	    deleteRelation(&ROrdered);
		return 0;
	}
    if (PRINT) printRelation(Srel);

    //create histogram
    relation* SHist = createHistogram(Srel);
    if (PRINT) printRelation(SHist);

    //create Psum
    relation* SPsum = createPsum(SHist);
    if (PRINT) printRelation(SPsum);

    //create ordered R
    relation* SOrdered = createROrdered(Srel, SHist, SPsum);
    if (PRINT) printRelation(SOrdered);

    // Index (in the smallest bucket of the 2 arrays for each hash1 value), compare and join by bucket
    if (indexCompareJoin(inter->ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum)) {
        printf("Error\n");
        return -1;
    }
    if (PRINT) printList(inter->ResultList);

    
	// Delete the (left & right) relation's current data in rList and replace it with the values found after radix join
	if (rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds != 0) {
		deleteRowIdList(&rList[predicates[currentPredicate]->leftSide->rowId].rowIds);
		rList[predicates[currentPredicate]->leftSide->rowId].rowIds = createRowIdList();
		rList[predicates[currentPredicate]->leftSide->rowId].relationId = relationId1;
		rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds = 0;
	}
	if (rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds != 0) {
		deleteRowIdList(&rList[predicates[currentPredicate]->rightSide->rowId].rowIds);
		rList[predicates[currentPredicate]->rightSide->rowId].rowIds = createRowIdList();
		rList[predicates[currentPredicate]->rightSide->rowId].relationId = relationId2;
		rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds = 0;
	}

    // Copy result list's item to our local rowIdList
    resultNode* curr = inter->ResultList->head;
    while (curr != NULL) {
        for (int j = 0; j < curr->num_of_elems; j++) {
        	// find id in array
        	int counter = 1;
        	for(int foundCounter=0;foundCounter<capacityLeft;foundCounter++){
        		if(curr->array[j].rowId1 == foundIdsLeft[foundCounter].rowId){
        			counter = foundIdsLeft[foundCounter].value;
        			break;
        		}
        	}
        	for(int i=0;i<counter;i++){
	            int result = insertIntoRowIdList(&rList[predicates[currentPredicate]->leftSide->rowId].rowIds, curr->array[j].rowId1);
	            if (result == -1) return -1;
	            else if(result == 1){
		            rList[predicates[currentPredicate]->leftSide->rowId].relationId = relationId1;
		            rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds++;
		        }
		    }
		    counter = 1;
        	for(int foundCounter=0;foundCounter<capacityRight;foundCounter++){
        		if(curr->array[j].rowId2 == foundIdsRight[foundCounter].rowId){
        			counter = foundIdsRight[foundCounter].value;
        			break;
        		}
        	}
        	for(int i=0;i<counter;i++){
		        int result = insertIntoRowIdList(&rList[predicates[currentPredicate]->rightSide->rowId].rowIds, curr->array[j].rowId2);
	            if (result == -1) return -1;
	            else if(result == 1){
		            rList[predicates[currentPredicate]->rightSide->rowId].relationId = relationId2;
		            rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds++;
		        }
		    }
        }
        curr = curr->next;
    }
    // Delete all structure created by allocating memory dynamically
    deleteRelation(&Rrel);
    deleteRelation(&RHist);
    deleteRelation(&RPsum);
    deleteRelation(&ROrdered);

    deleteRelation(&Srel);
    deleteRelation(&SHist);
    deleteRelation(&SPsum);
    deleteRelation(&SOrdered);

    free(foundIdsLeft);
    free(foundIdsRight);

    return 1;
}



int updatePredicates(predicate** predicates, rowIdsList* rList, int currentPredicate, int side, intermediate** intermediateStructs, int noJoins, intermediate* currentIntermediate) {
	// get relations for current outdated predicate
	int leftRelation = rList[predicates[currentPredicate]->leftSide->rowId].relationId;
	int rightRelation = rList[predicates[currentPredicate]->rightSide->rowId].relationId;
	// get intermediate structure from previous join
	intermediate* previousIntermediate;
	for(int i=0;i<noJoins;i++){
		if(intermediateStructs[i]->leftRelation == leftRelation && intermediateStructs[i]->rightRelation == rightRelation){
			previousIntermediate = intermediateStructs[i];
			break;
		}
	}

	if (side != 1 && side != 2){
		return -1;
	}

	rowIdsList *newOutdatedList = NULL;
	if (side == 1) {
		newOutdatedList = &rList[predicates[currentPredicate]->rightSide->rowId];
	}
	else {
		newOutdatedList = &rList[predicates[currentPredicate]->leftSide->rowId];
	}

	// delete rowIds of main structure and create another from the beginning
	deleteRowIdList(&(newOutdatedList)->rowIds);
	newOutdatedList->rowIds = createRowIdList();
	newOutdatedList->num_of_rowIds = 0;

	resultNode* previousResultList = previousIntermediate->ResultList->head;
	resultNode* currentResultList = currentIntermediate->ResultList->head;
	int currentSide;
	// TODO - check
	if (side == 1 && currentIntermediate->leftRelation == leftRelation) {
		currentSide = 2;
	}
	else {
		currentSide = 1;
	}

	while (currentResultList != NULL) {
		for(int counterCurrentIntermediate = 0; counterCurrentIntermediate < currentResultList->num_of_elems;counterCurrentIntermediate++){
			uint32_t currentIntermediateRowId;
			if(currentSide == 1){
				currentIntermediateRowId = currentResultList->array[counterCurrentIntermediate].rowId1;
			}else{
				currentIntermediateRowId = currentResultList->array[counterCurrentIntermediate].rowId2;
			}

			previousResultList = previousIntermediate->ResultList->head;
			while(previousResultList != NULL){
				for(int counterPreviousIntermediate = 0;counterPreviousIntermediate<previousResultList->num_of_elems;counterPreviousIntermediate++){
					uint32_t previousIntermediateRowId;
					uint32_t needToInsertRowId;
					if(side == 1){
						previousIntermediateRowId = previousResultList->array[counterPreviousIntermediate].rowId1;
						needToInsertRowId = previousResultList->array[counterPreviousIntermediate].rowId2;
					}else{
						previousIntermediateRowId = previousResultList->array[counterPreviousIntermediate].rowId2;
						needToInsertRowId = previousResultList->array[counterPreviousIntermediate].rowId1;	
					}
				
					if(currentIntermediateRowId == previousIntermediateRowId){					
						// insert to new rowIdsList
						int result = insertIntoRowIdList(&(newOutdatedList)->rowIds, needToInsertRowId);
						if(result == -1){
							return -1;
						}else if(result == 1){
							newOutdatedList->num_of_rowIds++;
						}
					}
				}
				previousResultList = previousResultList->next;	
			}
		}
		currentResultList = currentResultList->next;
	}
	return 1;
}

int checkSameId(tuple* foundIds, uint32_t rowId, int capacity, int checkFlag){
	for(int i=0;i<capacity;i++){
		if(foundIds[i].rowId == rowId){
			if(checkFlag){
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
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type, tuple* foundIds, int* capacity, int checkFlag) {
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
	uint64_t* returnedArray = calloc(rList[position].num_of_rowIds,sizeof(uint64_t));
	rowIdNode* temp = rList[position].rowIds;
	int index = 0;
	while (temp != NULL) {
		if(checkSameId(foundIds,temp->rowId,*capacity,checkFlag)){
			temp = temp->next;
			continue;
		}
		if(checkFlag){
			foundIds[*capacity].rowId = temp->rowId;
			foundIds[*capacity].value = 1;
		}
		(*capacity)++;
		if (type == 1) {
			//printf("index: %d\n",index);
			//printf("returnedArray[index]: %d\n",returnedArray[index]);
			//printf("value: %d\n",initRelations[relationId].Rarray[relColumn][temp->rowId]);
			returnedArray[index] = initRelations[relationId].Rarray[relColumn][temp->rowId];
		} else {
			returnedArray[index] = temp->rowId;
		}
		temp = temp->next;
		index++;
	}
	return returnedArray;
}
