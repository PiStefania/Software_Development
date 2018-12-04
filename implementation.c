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
			break;
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
                rList = malloc(predicatesSize * sizeof(rowIdsList));
				for(int i=0;i<predicatesSize;i++){
                    // Create the list to store the rowIds that satisfy each predicates
                    rList[i].rowIds = createRowIdList();
					// Compare
					if(predicates[i]->kind == 0){
						printf("predicate: %d.%d %c %d\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
									predicates[i]->comparator, predicates[i]->rightSide->rowId);
                        int relationId1 = relations[predicates[i]->leftSide->rowId];
                        int relColumn = predicates[i]->leftSide->value;
                        for (int j = 0; j < initRelations[relationId1].num_of_rows; j++) {
                            if (predicates[i]->comparator == '=') {
                                if (initRelations[relationId1].Rarray[relColumn][j] == predicates[i]->rightSide->rowId) {
                                    if (insertIntoRowIdList(rList[i].rowIds, j) == 0) return 0;
                                    rList[i].relationId1 = relationId1;
                                    rList[i].relationId2 = -1;
                                    //printf("Row: %d, Value: %ld\n", j, initRelations[relationId1].Rarray[relColumn][j]);
                                }
                            }
                            else if (predicates[i]->comparator == '>') {
                                if (initRelations[relationId1].Rarray[relColumn][j] > predicates[i]->rightSide->rowId) {
                                    if (insertIntoRowIdList(rList[i].rowIds, j) == 0) return 0;
                                    rList[i].relationId1 = relationId1;
                                    rList[i].relationId2 = -1;
                                    //printf("Row: %d, Value: %ld\n", j, initRelations[relationId1].Rarray[relColumn][j]);
                                }
                            }
                            if (predicates[i]->comparator == '<') {
                                if (initRelations[relationId1].Rarray[relColumn][j] < predicates[i]->rightSide->rowId) {
                                    if (insertIntoRowIdList(rList[i].rowIds, j) == 0) return 0;
                                    rList[i].relationId1 = relationId1;
                                    rList[i].relationId2 = -1;
                                    //printf("Row: %d, Value: %ld\n", j, initRelations[relationId1].Rarray[relColumn][j]);
                                }
                            }
                        }
					}else{	// Join
						printf("predicate: %d.%d %c %d.%d\n", predicates[i]->leftSide->rowId, predicates[i]->leftSide->value,
									predicates[i]->comparator, predicates[i]->rightSide->rowId, predicates[i]->rightSide->value);
					}
				}
			}
		}
        // Print rowIds found
        /*for (int i = 0; i < predicatesSize; i++) {
            rowIdNode* current;
            current = rList[i].rowIds;
            do {
                if (current->isEmptyList == 0) {
                    printf("Rel1: %d, Rel2: %d, RowId: %d\n", rList[i].relationId1, rList[i].relationId2, current->rowId);
                }
                current = current->next;
            } while (current != NULL) ;
        }*/

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
					printf("rel: %d, col: %d\n",projections[i].rowId,projections[i].value);
				}
			}
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

	if(failed)
		return 0;
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
