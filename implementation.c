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
		char* lineStr = strtok(line,"\n");
		char* relationsStr = strtok(lineStr,"|");
		char* predicatesStr = strtok(NULL,"|");
		char* projectionsStr = strtok(NULL,"");

		//printf("lineStr:        '%s'\n",lineStr);
		//printf("relationsStr:   '%s'\n",relationsStr);
		//printf("predicatesStr:  '%s'\n",predicatesStr);
		//printf("projectionsStr: '%s'\n",projectionsStr);

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
                // Create the list to store the rowIds that satisfy each predicate
                rList = malloc(relationsSize * sizeof(rowIdsList));
                for (int i = 0; i < relationsSize; i++) {
					rList[i].rowIds = createRowIdList();
                    rList[i].num_of_rowIds = 0;
				}
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 0) {
						printf("predicate: %ld.%ld %c %ld\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
									predicates[i]->comparator, predicates[i]->rightSide->rowId);
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
                                    else if (result == 1){
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                            else if (predicates[i]->comparator == '>') {
                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
                                	int result = insertIntoRowIdList(&rList[predicates[i]->leftSide->rowId].rowIds, j);
                                    if (result == -1) return 0;
                                    else if(result == 1){
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                            else if (predicates[i]->comparator == '<') {
                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
                                	int result = insertIntoRowIdList(&rList[predicates[i]->leftSide->rowId].rowIds, j);
                                    if (result == -1) return 0;
                                    else if(result == 1){
	                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
	                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
	                                }
                                }
                            }
                        }
					} else {	// Join
						printf("predicate: %ld.%ld %c %ld.%ld\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
									predicates[i]->comparator, predicates[i]->rightSide->rowId, predicates[i]->rightSide->value);
                        // Call Radix Hash Join
                        if (joinColumns(relations, predicates, initRelations, rList, i) == 0) return 0;
					}

				}
			}
		}

		// Get projections
		tuple* projections = NULL;
		int projectionsSize = 0;
		if(projectionsStr != NULL){
			projections = getProjectionsFromLine(projectionsStr,&projectionsSize);
			if(projections == NULL){
				printf("Projections are incorrect!\n");
				failed = 1;
			}
		}

        // Find final results (values summary)
		//printf("------------------------------------------------------\n");
        for (int i = 0; i < projectionsSize; i++) {
            uint64_t valueSummary = 0;
            //projections[i].rowId = number of relation, projections[i].value = column
			//uint64_t* array = setRowIdsToArray(rList, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1);
			rowIdNode* currentRowId;
			currentRowId = rList[projections[i].rowId].rowIds;
            for (int j = 0; j < rList[projections[i].rowId].num_of_rowIds; j++) {
                valueSummary += initRelations[relations[projections[i].rowId]].Rarray[projections[i].value][currentRowId->rowId];
				currentRowId = currentRowId->next;
            }
			if (valueSummary == 0){
				printf("NULL ");
			} else {
				printf("%ld ", valueSummary);
			}
			//free(array);
        }
        printf("\n");
		//printf("\n------------------------------------------------------\n");

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



int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate) {
    //create relation
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    relation* Rrel = NULL;
    if (rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds != 0) {
		uint64_t* arrayValues = setRowIdsValuesToArray(rList, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1, 1);
		uint64_t* arrayRowIds = setRowIdsValuesToArray(rList, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1, 0);
    	Rrel = createRelation(arrayValues, arrayRowIds, rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds);
		free(arrayValues);
		free(arrayRowIds);
    } else {
    	Rrel = createRelation(initRelations[relationId1].Rarray[relColumn1], NULL, initRelations[relationId1].num_of_rows);
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
    if (rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds != 0) {
    	uint64_t* arrayValues = setRowIdsValuesToArray(rList, predicates[currentPredicate]->rightSide->rowId, initRelations, relationId2, relColumn2, 1);
		uint64_t* arrayRowIds = setRowIdsValuesToArray(rList, predicates[currentPredicate]->rightSide->rowId, initRelations, relationId2, relColumn2, 0);
    	Srel = createRelation(arrayValues, arrayRowIds, rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds);
    	free(arrayValues);
		free(arrayRowIds);
    } else {
    	Srel = createRelation(initRelations[relationId2].Rarray[relColumn2], NULL, initRelations[relationId2].num_of_rows);
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

    // Create the list of joined values
    result* ResultList = createList();
    // Index (in the smallest bucket of the 2 arrays for each hash1 value), compare and join by bucket
    if (indexCompareJoin(ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum)) {
        printf("Error\n");
        return 0;
    }
    if (PRINT) printList(ResultList);

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
    resultNode* curr = ResultList->head;
    while (curr != NULL) {
        for (int j = 0; j < curr->num_of_elems; j++) {
            int result = insertIntoRowIdList(&rList[predicates[currentPredicate]->leftSide->rowId].rowIds, curr->array[j].rowId1);
            if (result == -1) return 0;
            else if(result == 1){
	            rList[predicates[currentPredicate]->leftSide->rowId].relationId = relationId1;
	            rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds++;
	        }
	        result = insertIntoRowIdList(&rList[predicates[currentPredicate]->rightSide->rowId].rowIds, curr->array[j].rowId2);
            if (result == -1) return 0;
            else if(result == 1){
	            rList[predicates[currentPredicate]->rightSide->rowId].relationId = relationId2;
	            rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds++;
	        }
        }
        curr = curr->next;
    }
    deleteList(&ResultList);

    // Delete all structure created by allocating memory dynamically
    deleteRelation(&Rrel);
    deleteRelation(&RHist);
    deleteRelation(&RPsum);
    deleteRelation(&ROrdered);

    deleteRelation(&Srel);
    deleteRelation(&SHist);
    deleteRelation(&SPsum);
    deleteRelation(&SOrdered);

    return 1;
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
uint64_t* setRowIdsValuesToArray(rowIdsList* rList, int position, relationsInfo* initRelations, int relationId, int relColumn, char type) {
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
	uint64_t* returnedArray = malloc(rList[position].num_of_rowIds * sizeof(uint64_t));
	rowIdNode* temp = rList[position].rowIds;
	int index = 0;
	while (temp != NULL) {
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
