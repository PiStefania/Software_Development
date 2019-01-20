#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/implementation.h"
#include "../include/rowIdArrayMethods.h"
#include "../include/statisticsMethods.h"
#include "../include/relationMethods.h"
#include "../include/queryMethods.h"
#include "../include/radixHashJoin.h"


int queriesImplementation(FILE* file, relationsInfo* initRelations, int num_of_initRelations, threadPool* thPool) {
	char *line = NULL;
	size_t len = 0;
	int read;
	int failed = 0;

	// If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		//printf("Please input queries (End input with letter 'F'):\n");
		file = stdin;
	}

	// Create an assisting structure of metadata, which we will use for each query, keeping the initial statistics in the initRelations structure unchanged
	metadataCol** queryMetadata = malloc(num_of_initRelations * sizeof(metadataCol*));
	for (int i = 0; i < num_of_initRelations; i++) {
		queryMetadata[i] = malloc(initRelations[i].num_of_columns * sizeof(metadataCol));
	}
	copyMetadata(initRelations, num_of_initRelations, NULL, 0, queryMetadata);

	while ((read = getline(&line, &len, file)) != -1) {
		// Get line and each section
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

        // Array of arrays to store the rowIds of all predicates found
        rowIdsArray** rArray = NULL;

		// Get predicates
		predicate** predicates = NULL;
		int predicatesSize = 0;
		if (predicatesStr != NULL) {
			predicates = getPredicatesFromLine(predicatesStr, &predicatesSize);
			if (predicates == NULL) {
				printf("Predicates are incorrect!\n");
				failed = 1;
			} else {
				// Find the best order of predicates, using statistics in joinEnumeration algorithm
				int joinEnumVal = 1;
				joinEnumVal = joinEnumeration(predicates, predicatesSize, initRelations, num_of_initRelations, relations, queryMetadata);
				if (joinEnumVal == -1) return -1;
                // Create the array to store the rowIds that satisfy each predicate
                rArray = malloc(relationsSize * sizeof(rowIdsArray*));
                for (int i = 0; i < relationsSize; i++) {
					rArray[i] = createRowIdsArray(relations[i]);
				}
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 0) {
						// Get relation from line of predicate
                        int relationId1 = relations[predicates[i]->leftSide->rowId];
                        // Get column that we need to compare, from predicate
                        int relColumn = predicates[i]->leftSide->value;
						// Check if the compare values are legitimate for this column and update the first statistics if so
						if (joinEnumVal == 0) continue;
                        // If rArray for specific relation is empty, use initRelations
                        if(rArray[predicates[i]->leftSide->rowId]->position == 0){
	                        // For each row of current relation, compare column, j is number of row aka id
	                        for (int j = 0; j < initRelations[relationId1].num_of_rows; j++) {
	                            if (predicates[i]->comparator == '=') {
	                            	// Check if are equal
	                                if (initRelations[relationId1].Rarray[relColumn][j] == predicates[i]->rightSide->rowId) {
	                                	// Insert row id of predicare into rArray of specific relation id
	                                	int result = insertIntoRowIdsArray(rArray[predicates[i]->leftSide->rowId], j);
	                                    if (result == -1) return 0;
	                                }
	                            }
	                            if (predicates[i]->comparator == '>') {
	                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
	                                	int result = insertIntoRowIdsArray(rArray[predicates[i]->leftSide->rowId], j);
	                                    if (result == -1) return 0;
	                                }
	                            }
	                            else if (predicates[i]->comparator == '<') {
	                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
	                                	int result = insertIntoRowIdsArray(rArray[predicates[i]->leftSide->rowId], j);
	                                    if (result == -1) return 0;
	                                }
	                            }
	                        }
							if (rArray[predicates[i]->leftSide->rowId]->position == 0) {
								rArray[predicates[i]->leftSide->rowId]->position = -1;
							}
						} else {
							// Create new rowIdsArray
							rowIdsArray* new_rowIds = createRowIdsArray(relationId1);
							// Get rowIds from rArray
							rowIdsArray* currentArray = rArray[predicates[i]->leftSide->rowId];
							for (int counter = 0; counter < currentArray->position; counter++) {
								// Get value of rowId
								int value = initRelations[relationId1].Rarray[predicates[i]->leftSide->value][currentArray->rowIds[counter]];
								if (predicates[i]->comparator == '=') {
	                            	// Check if are equal
	                                if (value == predicates[i]->rightSide->rowId) {
	                                	// Insert row id of predicare into rArray of specific relation id
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
	                                }
	                            }
	                            if (predicates[i]->comparator == '>') {
	                                if (value > predicates[i]->rightSide->rowId) {
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
	                                }
	                            }
	                            else if (predicates[i]->comparator == '<') {
	                                if (value < predicates[i]->rightSide->rowId) {
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
	                                }
	                            }
	                        }
							// Delete pre-existing rowIds from rArray and set new one
							deleteRowIdsArray(&rArray[predicates[i]->leftSide->rowId]);
							rArray[predicates[i]->leftSide->rowId] = new_rowIds;
							if (rArray[predicates[i]->leftSide->rowId]->position == 0) {
								rArray[predicates[i]->leftSide->rowId]->position = -1;
							}
						}
					}
					else {	// Join
					  	// Join columns
						int result = joinColumns(relations, predicates, initRelations, rArray, i, thPool);
                        if (result == -1) {
							return 0;
						}
						else if (result == 0) {
							continue;
						}
					}
				}
			}
		}

        // Find final results (values summary)
        for (int i = 0; i < projectionsSize; i++) {
            uint64_t valueSummary = 0;
			rowIdsArray* currentArray = rArray[projections[i].rowId];
            for (int j = 0; j < currentArray->position; j++) {
                valueSummary += initRelations[relations[projections[i].rowId]].Rarray[projections[i].value][currentArray->rowIds[j]];
            }
			if (valueSummary == 0) {
				if (i == projectionsSize-1) {
					printf("NULL");
				} else {
					printf("NULL ");
				}
			} else {
				if (i == projectionsSize-1) {
					printf("%ld", valueSummary);
				} else {
					printf("%ld ", valueSummary);
				}
			}
        }
        printf("\n");

		// Reset initial metadata to the assisting structure, which we used above, in order to use it again
		copyMetadata(initRelations, num_of_initRelations, relations, relationsSize, queryMetadata);

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
				deleteRowIdsArray(&rArray[i]);
			}
			free(predicates);
			predicates = NULL;
            free(rArray);
			rArray = NULL;
		}
		if (projections) {
			free(projections);
			projections=NULL;
		}
		if (failed) break;
	}
	// Free assisting metadata structure
	for (int i = 0; i < num_of_initRelations; i++) {
		free(queryMetadata[i]);
	}
	free(queryMetadata);
	// Free vars
	if (line) {
		free(line);
		line = NULL;
	}
	// Close file
	if (file != NULL  && file != stdin) {
		fclose(file);
	}
	if (failed) return 0;

	return 1;
}





int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsArray** rArray, int currentPredicate, threadPool* thPool) {
	// Check for self join occassions
	int leftPredicateRelation = predicates[currentPredicate]->leftSide->rowId;
	int rightPredicateRelation = predicates[currentPredicate]->rightSide->rowId;
	// Check if relations in predicate are the same
	if (leftPredicateRelation == rightPredicateRelation) {
		printPredicate(predicates[currentPredicate]);
		int leftColumn = predicates[currentPredicate]->leftSide->value;
		int rightColumn = predicates[currentPredicate]->rightSide->value;
		rowIdsArray* newRArray = createRowIdsArray(relations[leftPredicateRelation]);
		for (int i = 0; i < rArray[leftPredicateRelation]->position; i++) {
			int leftRowId = rArray[leftPredicateRelation]->rowIds[i];
			int leftValue = initRelations[relations[leftPredicateRelation]].Rarray[leftColumn][leftRowId];
			int rightValue = initRelations[relations[rightPredicateRelation]].Rarray[rightColumn][leftRowId];
			if (leftValue == rightValue) {
				// Insert this rowId to rArray
				if (!insertIntoRowIdsArray(newRArray, leftRowId)) return 0;
			}
		}
		deleteRowIdsArray(&rArray[leftPredicateRelation]);
		rArray[leftPredicateRelation] = newRArray;
		return 1;
	}
	// Check if the relation have already been joined before, then use self join
	if (rArray[leftPredicateRelation]->position > 0 && rArray[rightPredicateRelation]->position > 0) {
		// If the 2 rArrays have not the same number of elements, then normal radix hash join
		if (rArray[leftPredicateRelation]->position == rArray[rightPredicateRelation]->position) {
			int leftColumn = predicates[currentPredicate]->leftSide->value;
			int rightColumn = predicates[currentPredicate]->rightSide->value;
			rowIdsArray* newRArrayLeft = createRowIdsArray(relations[leftPredicateRelation]);
			rowIdsArray* newRArrayRight = createRowIdsArray(relations[rightPredicateRelation]);
			// Compare the values pointed from rowIds and keep the same ones
			for (int i = 0; i < rArray[leftPredicateRelation]->position; i++) {
				int leftRowId = rArray[leftPredicateRelation]->rowIds[i];
				int rightRowId = rArray[rightPredicateRelation]->rowIds[i];
				int leftValue = initRelations[relations[leftPredicateRelation]].Rarray[leftColumn][leftRowId];
				int rightValue = initRelations[relations[rightPredicateRelation]].Rarray[rightColumn][rightRowId];
				// If values from initRelations are the same, keep these row ids
				if (leftValue == rightValue) {
					// Insert this rowIds to rArrays
					if (!insertIntoRowIdsArray(newRArrayLeft, leftRowId)) return 0;
					if (!insertIntoRowIdsArray(newRArrayRight, rightRowId)) return 0;
				}
			}
			// Replace the old rowIds arrays with the new ones
			deleteRowIdsArray(&rArray[leftPredicateRelation]);
			rArray[leftPredicateRelation] = newRArrayLeft;
			deleteRowIdsArray(&rArray[rightPredicateRelation]);
			rArray[rightPredicateRelation] = newRArrayRight;
			return 1;
		}
	}

    // Create relations
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    relation* Rrel = NULL;
    // If relation in predicate exists in main structure
    if (rArray[predicates[currentPredicate]->leftSide->rowId]->position > 0) {
    	// Get rowid - value pairs for creating a new relation for join
    	Rrel = createRelationFromRarray(rArray[predicates[currentPredicate]->leftSide->rowId], initRelations, relationId1, relColumn1);
    }
    // Relation doesn't exist in main structure
	else if (rArray[predicates[currentPredicate]->leftSide->rowId]->position == 0) {
    	Rrel = createRelation(initRelations[relationId1].Rarray[relColumn1], NULL, initRelations[relationId1].num_of_rows);
    }
    // Error occured
	else if (rArray[predicates[currentPredicate]->leftSide->rowId]->position == -1) {
		rArray[predicates[currentPredicate]->rightSide->rowId]->position = -1;
		return 0;
	}
    if (PRINT) printRelation(Rrel);

    // Create histogram
    // Use threads for creating RHist by cutting it to pieces as the number of threads exist
   	relation* RHist = mergeIntoHist(thPool, Rrel);
    if (PRINT) printRelation(RHist);

    // Create Psum
    relation* RPsum = createPsum(RHist);
    if (PRINT) printRelation(RPsum);

    // Create ordered R
    relation* ROrdered = createROrdered(Rrel, RHist, RPsum);
    if (PRINT) printRelation(ROrdered);

    // Now the same procedure for S array
    int relationId2 = relations[predicates[currentPredicate]->rightSide->rowId];
    int relColumn2 = predicates[currentPredicate]->rightSide->value;
    relation* Srel = NULL;
    if (rArray[predicates[currentPredicate]->rightSide->rowId]->position > 0) {
    	Srel = createRelationFromRarray(rArray[predicates[currentPredicate]->rightSide->rowId], initRelations, relationId2, relColumn2);
    }
	else if (rArray[predicates[currentPredicate]->rightSide->rowId]->position == 0) {
    	Srel = createRelation(initRelations[relationId2].Rarray[relColumn2], NULL, initRelations[relationId2].num_of_rows);
    }
	else if (rArray[predicates[currentPredicate]->rightSide->rowId]->position == -1) {
		rArray[predicates[currentPredicate]->leftSide->rowId]->position = -1;
		deleteRelation(&Rrel);
	    deleteRelation(&RHist);
	    deleteRelation(&RPsum);
	    deleteRelation(&ROrdered);
		return 0;
	}
    if (PRINT) printRelation(Srel);

    // Create histogram
    // Use threads for creating SHist by cutting it to pieces as the number of threads exist
   	relation* SHist = mergeIntoHist(thPool, Srel);
    if (PRINT) printRelation(SHist);

    // Create Psum
    relation* SPsum = createPsum(SHist);
    if (PRINT) printRelation(SPsum);

    // Create ordered R
    relation* SOrdered = createROrdered(Srel, SHist, SPsum);
    if (PRINT) printRelation(SOrdered);

    // Index (in the smallest bucket of the 2 arrays for each hash1 value), compare and join by bucket
    // Create array of args for each thread
	indexCompareJoinArgs* args = malloc(BUCKETS * sizeof(indexCompareJoinArgs));
	for(int i=0;i<BUCKETS;i++){
		args[i].ResultList = createList();
		args[i].ROrdered = ROrdered;
		args[i].RHist = RHist;
		args[i].RPsum = RPsum;
		args[i].SOrdered = SOrdered;
		args[i].SHist = SHist;
		args[i].SPsum = SPsum;
		args[i].currentBucket = i;
	}

	// Calculate resultLists with threads
	result* resultList = mergeIntoResultList(thPool,args);
	for(int i=1;i<BUCKETS;i++){
   		free(args[i].ResultList);
	}
    free(args);

	// Create an array to store which predicates need an update after a repeatitive presence of a certain column of a relation
	rowIdsArray** newUpdatedLeftRArray = NULL;
	rowIdsArray** newUpdatedRightRArray = NULL;
	char *outdatedPredicates = NULL;
	if (currentPredicate != 0) {
		outdatedPredicates = malloc(currentPredicate * sizeof(char));
		searchOutdatedPredicates(predicates, outdatedPredicates, currentPredicate);
		newUpdatedLeftRArray = malloc(currentPredicate * sizeof(rowIdsArray*));
		newUpdatedRightRArray = malloc(currentPredicate * sizeof(rowIdsArray*));
	}
	// Update outdated relations after specifying them
	for (int j = 0; j < currentPredicate; j++) {
		newUpdatedLeftRArray[j] = NULL;
		newUpdatedRightRArray[j] = NULL;
		if (outdatedPredicates[j] == 1) {
			newUpdatedLeftRArray[j] = createRowIdsArray(relations[predicates[j]->leftSide->rowId]);
		}
		else if (outdatedPredicates[j] == 2) {
			newUpdatedRightRArray[j] = createRowIdsArray(relations[predicates[j]->rightSide->rowId]);
		}
	}

	// Delete the (left & right) relation's current data in rArray and replace it with the values found after radix join
	rowIdsArray* newRArrayLeft = createRowIdsArray(relationId1);
	rowIdsArray* newRArrayRight = createRowIdsArray(relationId2);

    // Copy result list's item to our local rowIdList
    resultNode* curr = resultList->head;
    while (curr != NULL) {
        for (int i = 0; i < curr->num_of_elems; i++) {
			// Insert row ids found into the RowIds Arrays
			if (insertIntoRowIdsArray(newRArrayLeft, curr->array[i].rowId1) == -1) return -1;
			if (insertIntoRowIdsArray(newRArrayRight, curr->array[i].rowId2) == -1) return -1;

			for (int j = 0; j < currentPredicate; j++) {
				if (outdatedPredicates[j] == 1) {
					int itemPosition = predicates[j]->leftSide->rowId;
					if (predicates[currentPredicate]->leftSide->rowId == predicates[j]->rightSide->rowId) {
						if (insertIntoRowIdsArray(newUpdatedLeftRArray[j], rArray[itemPosition]->rowIds[curr->array[i].rArrayRow1]) == -1) return -1;
					}
					else if (predicates[currentPredicate]->rightSide->rowId == predicates[j]->rightSide->rowId) {
						if (insertIntoRowIdsArray(newUpdatedLeftRArray[j], rArray[itemPosition]->rowIds[curr->array[i].rArrayRow2]) == -1) return -1;
					}
				}
				if (outdatedPredicates[j] == 2) {
					int itemPosition = predicates[j]->rightSide->rowId;
					if (predicates[currentPredicate]->leftSide->rowId == predicates[j]->leftSide->rowId) {
						if (insertIntoRowIdsArray(newUpdatedRightRArray[j], rArray[itemPosition]->rowIds[curr->array[i].rArrayRow1]) == -1) return -1;
					}
					else if (predicates[currentPredicate]->rightSide->rowId == predicates[j]->leftSide->rowId) {
						if (insertIntoRowIdsArray(newUpdatedRightRArray[j], rArray[itemPosition]->rowIds[curr->array[i].rArrayRow2]) == -1) return -1;
					}
				}
			}
        }
        curr = curr->next;
    }
	deleteList(&resultList);

	// Delete the old rArrays and replace them with the new ones
	deleteRowIdsArray(&rArray[predicates[currentPredicate]->leftSide->rowId]);
	rArray[predicates[currentPredicate]->leftSide->rowId] = newRArrayLeft;
	deleteRowIdsArray(&rArray[predicates[currentPredicate]->rightSide->rowId]);
	rArray[predicates[currentPredicate]->rightSide->rowId] = newRArrayRight;

	for (int i = 0; i < currentPredicate; i++) {
		if (outdatedPredicates[i] == 1) {
			deleteRowIdsArray(&rArray[predicates[i]->leftSide->rowId]);
			rArray[predicates[i]->leftSide->rowId] = newUpdatedLeftRArray[i];
		}
		else if (outdatedPredicates[i] == 2) {
			deleteRowIdsArray(&rArray[predicates[i]->rightSide->rowId]);
			rArray[predicates[i]->rightSide->rowId] = newUpdatedRightRArray[i];
		}
	}
	if (outdatedPredicates != NULL) {
		free(outdatedPredicates);
		free(newUpdatedLeftRArray);
		free(newUpdatedRightRArray);
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

    return 1;
}




// Search for previous predicates need to be updated
void searchOutdatedPredicates(predicate** predicates, char *outdatedPredicates, int currentPredicate) {
    // After join, look for outdated join predicates (if a member of current predicate has already been used in another join)
    for (int j = 0; j < currentPredicate; j++) {
		// We update only the join predicates, not the compare ones
        if (predicates[j]->kind != 0) {
            uint64_t iLeftRow = predicates[currentPredicate]->leftSide->rowId;
            uint64_t iLeftColumn = predicates[currentPredicate]->leftSide->value;
            uint64_t iRightRow = predicates[currentPredicate]->rightSide->rowId;
            uint64_t iRightColumn = predicates[currentPredicate]->rightSide->value;
            uint64_t jLeftRow = predicates[j]->leftSide->rowId;
            uint64_t jLeftColumn = predicates[j]->leftSide->value;
            uint64_t jRightRow = predicates[j]->rightSide->rowId;
            uint64_t jRightColumn = predicates[j]->rightSide->value;
			if ((iLeftRow == jRightRow && iLeftColumn == jRightColumn) || (iRightRow == jRightRow && iRightColumn == jRightColumn)) {
                if (iLeftRow == jLeftRow || iRightRow == jLeftRow) {
					outdatedPredicates[j] = 0;
                    continue;
                }
				else outdatedPredicates[j] = 1;
			}
            else if ((iLeftRow == jLeftRow && iLeftColumn == jLeftColumn) || (iRightRow == jLeftRow && iRightColumn == jLeftColumn)) {
                if (iLeftRow == jRightRow || iRightRow == jRightRow) {
					outdatedPredicates[j] = 0;
                    continue;
                }
				else outdatedPredicates[j] = 2;
            }
			else outdatedPredicates[j] = 0;
        }
        else outdatedPredicates[j] = 0;
    }
}
