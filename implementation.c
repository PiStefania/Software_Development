#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "implementation.h"
#include "rowIdArrayMethods.h"
#include "statisticsMethods.h"
#include "relationMethods.h"
#include "queryMethods.h"
#include "radixHashJoin.h"


int queriesImplementation(FILE* file, relationsInfo* initRelations, int num_of_initRelations, threadPool* thPool) {
	char *line = NULL;
	size_t len = 0;
	int read;
	int failed = 0;

	// If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		printf("Please input queries (End input with letter 'F'):\n");
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
		//printf("%s", line);
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
			predicates = getPredicatesFromLine(predicatesStr,&predicatesSize);
			if (predicates == NULL) {
				printf("Predicates are incorrect!\n");
				failed = 1;
			} else {
				// Get number of join predicates
				int joinPredicates = 0;
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 1)
						joinPredicates++;
				}
				// Create intermediate structures which contain resultlists and other data for update
				intermediate** intermediateStructs = malloc(joinPredicates*sizeof(intermediate*));
				for (int i = 0; i < joinPredicates; i++) {
					intermediateStructs[i] = malloc(sizeof(intermediate));
					intermediateStructs[i]->ResultList = createList();
					intermediateStructs[i]->foundIdsLeft = NULL;
					intermediateStructs[i]->foundIdsRight = NULL;
					intermediateStructs[i]->capacityLeft = 0;
					intermediateStructs[i]->capacityRight = 0;
					intermediateStructs[i]->leftColumn = -1;
					intermediateStructs[i]->rightColumn = -1;
				}
                // Create the array to store the rowIds that satisfy each predicate
                rArray = malloc(relationsSize * sizeof(rowIdsArray*));
                for (int i = 0; i < relationsSize; i++) {
					rArray[i] = createRowIdsArray(relations[i]);
				}
				int currentJoinPredicates = 0;
				for (int i = 0; i < predicatesSize; i++) {
					// Compare column with a number
					if (predicates[i]->kind == 0) {
						// Get relation from line of predicate
                        int relationId1 = relations[predicates[i]->leftSide->rowId];
                        // Get column that we need to compare, from predicate
                        int relColumn = predicates[i]->leftSide->value;
						// Check if the compare values are legitimate for this column and update the first statistics if so
						metadataCol *oldMetadata = malloc(sizeof(metadataCol));
						int outOfBoundaries = checkCompareStatistics(predicates, queryMetadata, oldMetadata, i, relationId1, relColumn);
						if (outOfBoundaries == 1) continue;
						char foundValue = 0;
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
	                                    else if (result == 1) {
											foundValue = 1;
		                                }
	                                }
	                            }
	                            if (predicates[i]->comparator == '>') {
	                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
	                                	int result = insertIntoRowIdsArray(rArray[predicates[i]->leftSide->rowId], j);
	                                    if (result == -1) return 0;
	                                    else if (result == 1) {
											foundValue = 1;
		                                }
	                                }
	                            }
	                            else if (predicates[i]->comparator == '<') {
	                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
	                                	int result = insertIntoRowIdsArray(rArray[predicates[i]->leftSide->rowId], j);
	                                    if (result == -1) return 0;
	                                    else if (result == 1) {
											foundValue = 1;
		                                }
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
							for(int counter=0;counter<currentArray->position;counter++) {
								// Get value of rowId
								int value = initRelations[relationId1].Rarray[predicates[i]->leftSide->value][currentArray->rowIds[counter]];
								if (predicates[i]->comparator == '=') {
	                            	// Check if are equal
	                                if (value == predicates[i]->rightSide->rowId) {
	                                	// Insert row id of predicare into rArray of specific relation id
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
										foundValue = 1;
	                                }
	                            }
	                            if (predicates[i]->comparator == '>') {
	                                if (value > predicates[i]->rightSide->rowId) {
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
										foundValue = 1;
	                                }
	                            }
	                            else if (predicates[i]->comparator == '<') {
	                                if (value < predicates[i]->rightSide->rowId) {
	                                	if (!insertIntoRowIdsArray(new_rowIds, currentArray->rowIds[counter])) return 0;
										foundValue = 1;
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
						// Update the rest of statistics
						updateCompareStatistics(predicates, initRelations, queryMetadata, oldMetadata, i, relationId1, relColumn, foundValue);
						free(oldMetadata);
					}
					else {	// Join
                        //printf("JOIN\n");
                        //printPredicate(predicates[i]);
                        // Update specific intermediate structure before join
                        intermediateStructs[currentJoinPredicates]->leftRelation = relations[predicates[i]->leftSide->rowId];
                        intermediateStructs[currentJoinPredicates]->rightRelation = relations[predicates[i]->rightSide->rowId];
                        intermediateStructs[currentJoinPredicates]->leftColumn = predicates[i]->leftSide->value;
                        intermediateStructs[currentJoinPredicates]->rightColumn = predicates[i]->rightSide->value;

						// Update join statistics
						updateJoinStatistics(predicates, initRelations, relations, queryMetadata, i);

					  	// Join columns
						int result = joinColumns(relations, predicates, initRelations, rArray, i, intermediateStructs[currentJoinPredicates], intermediateStructs, currentJoinPredicates, thPool);
						currentJoinPredicates++;
                        if (result == -1) {
							return 0;
						}
						else if (result == 0) {
							continue;
						}

						// Create an array to store which predicates need an update after a repeatitive presence of a certain column of a relation
						char *outdatedPredicates;
						outdatedPredicates = malloc(predicatesSize * sizeof(char));
						searchOutdatedPredicates(predicates, projections, outdatedPredicates, i, predicatesSize, projectionsSize);

						// Update outdated relations after specifying them
						for (int j = 0; j < predicatesSize; j++) {
							if (outdatedPredicates[j] != 0) {
								//printf("UPDATE\n");
								//printPredicate(predicates[j]);
								int result = updatePredicates(predicates, rArray, j, outdatedPredicates[j], intermediateStructs, joinPredicates, intermediateStructs[currentJoinPredicates-1]);
								if (result == -1) return 0;
							}
						}
						free(outdatedPredicates);
					}
				}

				// Delete intermediateStructs
				for(int i=0;i<joinPredicates;i++){
					deleteList(&intermediateStructs[i]->ResultList);
					free(intermediateStructs[i]->foundIdsLeft);
	    			free(intermediateStructs[i]->foundIdsRight);
					free(intermediateStructs[i]);
				}
				free(intermediateStructs);
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


int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsArray** rArray, int currentPredicate, intermediate* inter, intermediate** intermediateStructs, int noJoins, threadPool* thPool) {
	// Check if relations in predicate are the same
	int leftRelationSame = predicates[currentPredicate]->leftSide->rowId;
	int rightRelationSame = predicates[currentPredicate]->rightSide->rowId;
	if(leftRelationSame == rightRelationSame){
		// if rArray is empty for this relation
		if(rArray[predicates[currentPredicate]->leftSide->rowId]->position == 0){
			// Don't execute radix hash join, instead get rowIds of columns that are the same
			int relationId = leftRelationSame;
			int leftColumn = predicates[currentPredicate]->leftSide->value;
			int rightColumn = predicates[currentPredicate]->rightSide->value;
			// Insert to rArray only if predicate's columns are different
			if(leftColumn != rightColumn){
				// Set rArrays's relationId
				rArray[predicates[currentPredicate]->leftSide->rowId]->relationId = relationId;
				for(int i=0;i<initRelations[relationId].num_of_rows;i++){
					int leftValue = initRelations[relationId].Rarray[leftColumn][i];
					int rightValue = initRelations[relationId].Rarray[rightColumn][i];
					if(leftValue == rightValue){
						// Insert this rowId to rArray
						if(!insertIntoRowIdsArray(rArray[predicates[currentPredicate]->leftSide->rowId],i)){
							return 0;
						}
					}
				}
			}
		}
		return 1;
	}
	// Check if both relations from predicate already exist
	if(rArray[predicates[currentPredicate]->leftSide->rowId]->position > 0 && rArray[predicates[currentPredicate]->rightSide->rowId]->position > 0){
		// Find column used for previous predicate
		// Get relations
		int leftRelation = rArray[predicates[currentPredicate]->leftSide->rowId]->relationId;
		int rightRelation = rArray[predicates[currentPredicate]->rightSide->rowId]->relationId;
		int side = 0;
		// Get intermediate structure from previous join
		intermediate* previousIntermediate = NULL;
		for(int i=0;i<noJoins;i++){
			if(intermediateStructs[i]->leftRelation == leftRelation && intermediateStructs[i]->rightRelation == rightRelation){
				previousIntermediate = intermediateStructs[i];
				side = 1;
				break;
			}else if(intermediateStructs[i]->leftRelation == rightRelation && intermediateStructs[i]->rightRelation == leftRelation){
				previousIntermediate = intermediateStructs[i];
				side = 2;
				break;
			}
		}
		if(previousIntermediate != NULL){
			int leftColumnPrevious, leftColumnCurrent, rightColumnPrevious, rightColumnCurrent;
			rowIdsArray* leftCurrentArray = rArray[predicates[currentPredicate]->leftSide->rowId];
			rowIdsArray* rightCurrentArray = rArray[predicates[currentPredicate]->rightSide->rowId];
			rowIdsArray* newRowIdsArrayLeft = NULL;
			rowIdsArray* newRowIdsArrayRight = NULL;
			// Current left relation equals previous left relation and right the same
			if(side == 1){
				// Get rowIds and values from main structure if columns are not the same
				// For left relation
				leftColumnPrevious = previousIntermediate->leftColumn;
				leftColumnCurrent = inter->leftColumn;
				// For right relation
				rightColumnPrevious = previousIntermediate->rightColumn;
				rightColumnCurrent = inter->rightColumn;
			}
			// Current left relation equals previous right relation and right equals left
			else if(side == 2){
				// Get rowIds and values from main structure if columns are not the same
				// For left relation
				leftColumnPrevious = previousIntermediate->rightColumn;
				leftColumnCurrent = inter->leftColumn;
				// For right relation
				rightColumnPrevious = previousIntermediate->leftColumn;
				rightColumnCurrent = inter->rightColumn;
			} else{
				// Error occured
				return 0;
			}

			// If columns used for joins are not the same
			if(leftColumnPrevious != leftColumnCurrent){
				newRowIdsArrayLeft = createRowIdsArray(leftCurrentArray->relationId);
				// Get rowIds and values for each column
				for(int i=0;i<leftCurrentArray->position;i++){
					// Check if column values are the same for same rowId
					int previousColumnValue = initRelations[leftRelation].Rarray[leftColumnPrevious][leftCurrentArray->rowIds[i]];
					int currentColumnValue = initRelations[leftRelation].Rarray[leftColumnCurrent][leftCurrentArray->rowIds[i]];
					if(previousColumnValue == currentColumnValue){
						// Insert this node to new rowIds list
						if(!insertIntoRowIdsArray(newRowIdsArrayLeft,leftCurrentArray->rowIds[i])){
							return 0;
						}
					}
				}
			}

			if(rightColumnPrevious != rightColumnCurrent){
				newRowIdsArrayRight = createRowIdsArray(rightCurrentArray->relationId);
				// Get rowIds and values for each column
				for(int i=0;i<rightCurrentArray->position;i++){
					// Check if column values are the same for same rowId
					int previousColumnValue = initRelations[rightRelation].Rarray[rightColumnPrevious][rightCurrentArray->rowIds[i]];
					int currentColumnValue = initRelations[rightRelation].Rarray[rightColumnCurrent][rightCurrentArray->rowIds[i]];
					if(previousColumnValue == currentColumnValue){
						// Insert this node to new rowIds list
						if(!insertIntoRowIdsArray(newRowIdsArrayRight,rightCurrentArray->rowIds[i])){
							return 0;
						}
					}
				}
			}

			// Delete previous rowIds and set to new ones if new ones not empty
			if(newRowIdsArrayLeft != NULL){
				deleteRowIdsArray(&rArray[predicates[currentPredicate]->leftSide->rowId]);
				rArray[predicates[currentPredicate]->leftSide->rowId] = newRowIdsArrayLeft;
				deleteRowIdsArray(&newRowIdsArrayLeft);
			}

			if(newRowIdsArrayRight != NULL){
				deleteRowIdsArray(&rArray[predicates[currentPredicate]->rightSide->rowId]);
				rArray[predicates[currentPredicate]->rightSide->rowId] = newRowIdsArrayRight;
				deleteRowIdsArray(&newRowIdsArrayRight);
			}
			return 1;
		}
	}

    // Create relations
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    relation* Rrel = NULL;
    tuple* foundIdsLeft = NULL;
    int capacityLeft = 0;
    // If relation in predicate exists in main structure
    if (rArray[predicates[currentPredicate]->leftSide->rowId]->position > 0) {
    	// Get rowid - value pairs for creating a new relation for join
    	// In addition, get rowid - number of times found pairs, helpful for updating predicates
    	foundIdsLeft = malloc(rArray[predicates[currentPredicate]->leftSide->rowId]->position * sizeof(tuple));
		//uint64_t* arrayValues = setRowIdsValuesToArray(rArray, predicates[currentPredicate]->leftSide->rowId, initRelations, relationId1, relColumn1, 1, foundIdsLeft, &capacityLeft);
    	Rrel = createRelationFromRarray(rArray[predicates[currentPredicate]->leftSide->rowId], initRelations, relationId1, relColumn1);
		inter->foundIdsLeft = foundIdsLeft;
   		inter->capacityLeft = capacityLeft;
		capacityLeft = 0;
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
   	//relation* RHist = mergeIntoHist(thPool, Rrel);
    relation* RHist = createHistogram(Rrel);
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
    tuple* foundIdsRight = NULL;
    int capacityRight = 0;
    if (rArray[predicates[currentPredicate]->rightSide->rowId]->position > 0) {
    	foundIdsRight = malloc(rArray[predicates[currentPredicate]->rightSide->rowId]->position * sizeof(tuple));
    	//uint64_t* arrayValues = setRowIdsValuesToArray(rArray, predicates[currentPredicate]->rightSide->rowId, initRelations, relationId2, relColumn2, 1, foundIdsRight, &capacityRight);
    	Srel = createRelationFromRarray(rArray[predicates[currentPredicate]->rightSide->rowId], initRelations, relationId2, relColumn2);
		inter->foundIdsRight = foundIdsRight;
    	inter->capacityRight = capacityRight;
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
    // Use threads for creating RHist by cutting it to pieces as the number of threads exist
   	//relation* SHist = mergeIntoHist(thPool, Srel);
    relation* SHist = createHistogram(Srel);
    if (PRINT) printRelation(SHist);

    // Create Psum
    relation* SPsum = createPsum(SHist);
    if (PRINT) printRelation(SPsum);

    // Create ordered R
    relation* SOrdered = createROrdered(Srel, SHist, SPsum);
    if (PRINT) printRelation(SOrdered);

    // Index (in the smallest bucket of the 2 arrays for each hash1 value), compare and join by bucket
    if (indexCompareJoin(inter->ResultList, ROrdered, RHist, RPsum, SOrdered, SHist, SPsum)) {
        printf("Error\n");
        return -1;
    }
    if (PRINT) printList(inter->ResultList);

	// Delete the (left & right) relation's current data in rArray and replace it with the values found after radix join
	if (rArray[predicates[currentPredicate]->leftSide->rowId]->position != 0) {
		deleteRowIdsArray(&rArray[predicates[currentPredicate]->leftSide->rowId]);
		rArray[predicates[currentPredicate]->leftSide->rowId] = createRowIdsArray(relationId1);
	}
	if (rArray[predicates[currentPredicate]->rightSide->rowId]->position != 0) {
		deleteRowIdsArray(&rArray[predicates[currentPredicate]->rightSide->rowId]);
		rArray[predicates[currentPredicate]->rightSide->rowId] = createRowIdsArray(relationId2);
	}

    // Copy result list's item to our local rowIdList
    resultNode* curr = inter->ResultList->head;
    while (curr != NULL) {
        for (int j = 0; j < curr->num_of_elems; j++) {
        	int result = insertIntoRowIdsArray(rArray[predicates[currentPredicate]->leftSide->rowId], curr->array[j].rowId1);
			if (result == -1) return -1;
			result = insertIntoRowIdsArray(rArray[predicates[currentPredicate]->rightSide->rowId], curr->array[j].rowId2);
			if (result == -1) return -1;
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

    return 1;
}


int updatePredicates(predicate** predicates, rowIdsArray** rArray, int currentPredicate, int side, intermediate** intermediateStructs, int noJoins, intermediate* currentIntermediate) {
	// Get relations for current outdated predicate
	int leftRelation = rArray[predicates[currentPredicate]->leftSide->rowId]->relationId;
	int rightRelation = rArray[predicates[currentPredicate]->rightSide->rowId]->relationId;
	// Get intermediate structure from outdated join
	intermediate* previousIntermediate;
	for (int i = 0; i < noJoins; i++) {
		if(intermediateStructs[i]->leftRelation == leftRelation && intermediateStructs[i]->rightRelation == rightRelation){
			previousIntermediate = intermediateStructs[i];
			break;
		}
	}
	// Error occured
	if (side != 1 && side != 2) return -1;

	// Delete main structure of relation in order to update it
	rowIdsArray* newOutdatedArray = NULL;
	if (side == 1) {	// Delete right relation from the outdated predicate
		newOutdatedArray = rArray[predicates[currentPredicate]->rightSide->rowId];
	}
	else {		// Delete left relation from the outdated predicate
		newOutdatedArray = rArray[predicates[currentPredicate]->leftSide->rowId];
	}
	// Delete rowIdsArray rowIds of selected relation main structure and create another one from the beginning
	free(newOutdatedArray->rowIds);
	newOutdatedArray->rowIds = malloc(DEFAULT_ROWS*sizeof(uint64_t));
	newOutdatedArray->position = 0;
	newOutdatedArray->num_of_rowIds = DEFAULT_ROWS;

	// Get resultLists of joins for outdated join
	resultNode* previousResultList = previousIntermediate->ResultList->head;
	// For current join which triggered the update procedure
	resultNode* currentResultList = currentIntermediate->ResultList->head;

	// Pinpoint which side to get rowIds to compare - need to be the other side from the deleted one
	int currentSide;

	// Get rowIds from left relation in current intermediate structure if 1 else 2
	if (side == 1 && currentIntermediate->leftRelation == leftRelation) {
		currentSide = 1;
	} else if(side == 1 && currentIntermediate->leftRelation == rightRelation){
		currentSide = 2;
	} else if(side == 1 && currentIntermediate->rightRelation == rightRelation){
		currentSide = 2;
	} else{
		currentSide = 1;
	}

	// Get no of rowIds in current intermediate resultlist
	tuple* tempWholeLeft = NULL;
	tuple* tempWholeRight = NULL;
	int capacityLeft = 0;
	int capacityRight = 0;
	int currentElems = currentResultList->num_of_elems;
	while(currentResultList != NULL){
		if(tempWholeLeft == NULL && tempWholeRight == NULL){
			tempWholeLeft = malloc(currentElems * sizeof(tuple));
			tempWholeRight = malloc(currentElems * sizeof(tuple));
		}else{
			currentElems += currentResultList->num_of_elems;
			tempWholeLeft = realloc(tempWholeLeft,currentElems * sizeof(tuple));
			tempWholeRight = realloc(tempWholeRight,currentElems * sizeof(tuple));
		}
		for(int i=0;i<currentResultList->num_of_elems;i++){
			if(!checkSameId(tempWholeLeft,currentResultList->array[i].rowId1,capacityLeft,1)){
				tempWholeLeft[capacityLeft].rowId = currentResultList->array[i].rowId1;
				tempWholeLeft[capacityLeft].value = 1;
				capacityLeft++;
			}
			if(!checkSameId(tempWholeRight,currentResultList->array[i].rowId2,capacityRight,1)){
				tempWholeRight[capacityRight].rowId = currentResultList->array[i].rowId2;
				tempWholeRight[capacityRight].value = 1;
				capacityRight++;
			}
		}
		currentResultList = currentResultList->next;
	}

	// Update for number of appearances
	for(int i=0;i<capacityLeft;i++){
		for(int j=0;j<currentIntermediate->capacityLeft;j++){
			if(tempWholeLeft[i].rowId == currentIntermediate->foundIdsLeft[j].rowId){
				tempWholeLeft[i].value /= currentIntermediate->foundIdsLeft[j].value;
				break;
			}
		}
		for(int j=0;j<currentIntermediate->capacityRight;j++){
			if(tempWholeRight[i].rowId == currentIntermediate->foundIdsRight[j].rowId){
				tempWholeRight[i].value /= currentIntermediate->foundIdsRight[j].value;
				break;
			}
		}
	}

	// For each resultlist node of current intermediate join
	currentResultList = currentIntermediate->ResultList->head;
	while (currentResultList != NULL) {
		// For each row in array of resultlist
		for(int counterCurrentIntermediate = 0; counterCurrentIntermediate < currentResultList->num_of_elems;counterCurrentIntermediate++){
			uint32_t currentIntermediateRowId;
			int counter;
			// Get rowId according to currentSide (left or right value (rowId1/rowId2))
			if(currentSide == 1 && capacityLeft > 0){
				currentIntermediateRowId = currentResultList->array[counterCurrentIntermediate].rowId1;
				// Find counter of rowId
				int tempIndex = 0;
				for(int i=0;i<capacityLeft;i++){
					if(currentIntermediateRowId == tempWholeLeft[i].rowId){
						counter = tempWholeLeft[i].value;
						tempIndex = i;
						break;
					}
				}
				// If counter is zero, then continue to next rowId (do not reuse it)
				if(counter == 0){
					continue;
				// Else decrement counter and use it for compare
				}else{
					tempWholeLeft[tempIndex].value--;
				}
			// Same as above
			}else if(currentSide == 2 && capacityRight > 0){
				currentIntermediateRowId = currentResultList->array[counterCurrentIntermediate].rowId2;
				int tempIndex = 0;
				for(int i=0;i<capacityRight;i++){
					if(currentIntermediateRowId == tempWholeRight[i].rowId){
						counter = tempWholeRight[i].value;
						tempIndex = i;
						break;
					}
				}
				if(counter == 0){
					continue;
				}else{
					tempWholeRight[tempIndex].value--;
				}
			}

			// For each resultlist node of outdated intermediate join (previous join)
			previousResultList = previousIntermediate->ResultList->head;
			while(previousResultList != NULL){
				// For each row in array of resultlist
				for(int counterPreviousIntermediate = 0;counterPreviousIntermediate<previousResultList->num_of_elems;counterPreviousIntermediate++){
					uint32_t previousIntermediateRowId;
					uint32_t needToInsertRowId;
					// If side == 1, get left rowId for compare, and right to insert, else reverse
					if(side == 1){
						previousIntermediateRowId = previousResultList->array[counterPreviousIntermediate].rowId1;
						needToInsertRowId = previousResultList->array[counterPreviousIntermediate].rowId2;
					}else{
						previousIntermediateRowId = previousResultList->array[counterPreviousIntermediate].rowId2;
						needToInsertRowId = previousResultList->array[counterPreviousIntermediate].rowId1;
					}

					// If ids are the same
					if(currentIntermediateRowId == previousIntermediateRowId){
						// Insert to new rowIdsList
						int result = insertIntoRowIdsArray(newOutdatedArray, needToInsertRowId);
						if(result == -1){
							return -1;
						}
					}
				}
				previousResultList = previousResultList->next;
			}
		}
		currentResultList = currentResultList->next;
	}
	free(tempWholeLeft);
	free(tempWholeRight);
	return 1;
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

int checkSameId(tuple* foundIds, uint64_t rowId, int capacity, char checkFlag) {
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