#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "implementation.h"
#include "relationMethods.h"
#include "queryMethods.h"
#include "auxMethods.h"
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

		printf("lineStr:        '%s'\n",lineStr);
		printf("relationsStr:   '%s'\n",relationsStr);
		printf("predicatesStr:  '%s'\n",predicatesStr);
		printf("projectionsStr: '%s'\n",projectionsStr);

		// Check for NULL Strings
		if(lineStr == NULL || relationsStr == NULL || predicatesStr == NULL || projectionsStr == NULL){
			if(lineStr != NULL){
				if(strcmp(lineStr,"F") != 0){
					printf("Query line is incorrect, please provide another file or input correct statement.\n");
					failed = 1;
				}
			}else{
				continue;
			}
            if(file == stdin){
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
			}else{
				for(int i=0;i<relationsSize;i++){
					printf("rel: %d\n",relations[i]);
				}
			}
		}

        // List to store the rowIds of all predicates found
        rowIdsList* rList = NULL;

		// Get predicates
		predicate** predicates = NULL;
		int predicatesSize = 0;
		if(predicatesStr != NULL){
			predicates = getPredicatesFromLine(predicatesStr,&predicatesSize);
			if(predicates == NULL){
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
					if(predicates[i]->kind == 0){
						printf("predicate: %d.%d %c %d\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
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
                                    if (insertIntoRowIdList(rList[predicates[i]->leftSide->rowId].rowIds, j) == 0) return 0;
                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;		// -> ??
                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
                                }
                            }
                            else if (predicates[i]->comparator == '>') {
                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
                                    if (insertIntoRowIdList(rList[predicates[i]->leftSide->rowId].rowIds, j) == 0) return 0;
                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
                                }
                            }
                            else if (predicates[i]->comparator == '<') {
                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
                                    if (insertIntoRowIdList(rList[predicates[i]->leftSide->rowId].rowIds, j) == 0) return 0;
                                    rList[predicates[i]->leftSide->rowId].relationId = relationId1;
                                    rList[predicates[i]->leftSide->rowId].num_of_rowIds++;
                                }
                            }
                        }
					} else {	// Join
						printf("predicate: %d.%d %c %d.%d\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
									predicates[i]->comparator, predicates[i]->rightSide->rowId, predicates[i]->rightSide->value);
                        // Call Radix Hash Join
                        if (joinColumns(relations, predicates, initRelations, rList, i) == 0) return 0;
					}
				}
			}
		}

		// Print all results
		printRowIdsList(rList, relationsSize);

		// Get projections
		tuple* projections = NULL;
		int projectionsSize = 0;
		if(projectionsStr != NULL){
			projections = getProjectionsFromLine(projectionsStr,&projectionsSize);
			if(projections == NULL){
				printf("Projections are incorrect!\n");
				failed = 1;
			}else{
				for(int i=0;i<projectionsSize;i++){
					printf("projection rel: %d, col: %d\n", projections[i].rowId, projections[i].value);
				}
			}
		}

        // Find final results (values summary)
        for (int i = 0; i < projectionsSize; i++) {
            uint64_t valueSummary = 0;
            for (int j = 0; j < rList[projections[i].rowId].num_of_rowIds; j++) {
                valueSummary = valueSummary + initRelations[relations[projections[i].rowId]].Rarray[projections[i].value][j];
                //printf("rel: %d, col: %d, Summary:%ld\n", projections[i].rowId, projections[i].value, valueSummary);
            }
            printf("%ld\n", valueSummary);
        }

		// Free vars for each line
		if(relations){
			free(relations);
			relations=NULL;
		}
		if(predicates){
			for(int i=0;i<predicatesSize;i++){
				deletePredicate(&predicates[i]);
                deleteRowIdList(rList[i].rowIds);
			}
			free(predicates);
			predicates = NULL;
            free(rList);
		}
		if(projections){
			free(projections);
			projections=NULL;
		}

		if(failed)
			break;
	}

	// Free vars
	if(line){
		free(line);
		line=NULL;
	}

	// Close file
	if (file != NULL  && file != stdin)
		fclose(file);

	if (failed)
		return 0;
	return 1;
}



int joinColumns(int* relations, predicate** predicates, relationsInfo* initRelations, rowIdsList* rList, int currentPredicate) {
    //create relation
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    relation* Rrel = createRelation(initRelations[relationId1].Rarray[relColumn1], initRelations[relationId1].num_of_rows);
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
    relation* Srel = createRelation(initRelations[relationId2].Rarray[relColumn2], initRelations[relationId2].num_of_rows);
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

    // Copy result list's item to our local rowIdList
    resultNode* curr = ResultList->head;
    while (curr != NULL) {
        for (int j = 0; j < curr->num_of_elems; j++) {
            //printf("%8d %31d\n", curr->array[j].rowId1, curr->array[j].rowId2);
            if (insertIntoRowIdList(rList[predicates[currentPredicate]->leftSide->rowId].rowIds, curr->array[j].rowId1) == 0) return 0;
            rList[predicates[currentPredicate]->leftSide->rowId].relationId = relationId1;
            rList[predicates[currentPredicate]->leftSide->rowId].num_of_rowIds++;
            if (insertIntoRowIdList(rList[predicates[currentPredicate]->rightSide->rowId].rowIds, curr->array[j].rowId2) == 0) return 0;
            rList[predicates[currentPredicate]->rightSide->rowId].relationId = relationId2;
            rList[predicates[currentPredicate]->rightSide->rowId].num_of_rowIds++;
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
	return list;
}

// Insert strings into the list (helps while reading files)
int insertIntoRowIdList(rowIdNode* list, int rowId) {
	rowIdNode *currentNode, *newNode;
	if (list->isEmptyList == 1){
		list->rowId = rowId;
		list->isEmptyList = 0;
		return 1;
	}
	currentNode = list;
	while (currentNode->next != NULL) {
		currentNode = currentNode->next;
	}
	if ((newNode = malloc(sizeof(rowIdNode))) == NULL) return 0;
	newNode->rowId = rowId;
    newNode->isEmptyList = 0;
	newNode->next = NULL;
	currentNode->next = newNode;
	return 1;
}


// Delete above list
void deleteRowIdList(rowIdNode* list) {
	rowIdNode *currentNode;
	while (list->next != NULL){
		currentNode = list;
		list = list->next;
		free(currentNode);
	}
	free(list);
}

void printRowIdsList(rowIdsList* rowIdsList, int noOfRelations){
	for(int i=0;i<noOfRelations;i++){
		printf("----Relation: %d----\n",rowIdsList[i].relationId);
		rowIdNode* temp = rowIdsList[i].rowIds;
		while(temp != NULL){
			printf("%d ",temp->rowId);
			temp = temp->next;
		}
	}
}