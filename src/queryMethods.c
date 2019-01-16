#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "../include/queryMethods.h"
#include "../include/radixHashJoin.h"


int* getRelationsFromLine(char* relationsStr, int* relationsSize){
	if(relationsStr == NULL){
		*relationsSize = 0;
		return NULL;
	}
	char* tempRelations = malloc((strlen(relationsStr)+1)*sizeof(char));
	strcpy(tempRelations,relationsStr);
	// Count number of relations
	int counter = 1;
	char* token = strtok(tempRelations," \t");
	while(token!=NULL){
		token = strtok(NULL, " \t");
		if(token != NULL){
			counter++;
		}
	}

	// Free temporary variable used for counter
	free(tempRelations);
	tempRelations = NULL;

	int* relations = NULL;

	*relationsSize = counter;
	// Convert str to int
	int position = 0;
	token = strtok(relationsStr," \t");
	if(isNumeric(token)){
		relations = malloc(counter*sizeof(int));
		relations[position] = atoi(token);
	}else{
		*relationsSize = 0;
		return NULL;
	}

	position++;
	while(position<counter){
		if(position==counter-1){
			token = strtok(NULL,"");
		}else{
			token = strtok(NULL, " \t");
		}
		if(token != NULL){
			if(isNumeric(token)){
				relations[position] = atoi(token);
			}else{
				free(relations);
				*relationsSize = 0;
				relations = NULL;
				return NULL;
			}
		}
		position++;
	}
	return relations;
}


tuple* getProjectionsFromLine(char* projectionsStr, int* projectionsSize){
	if(projectionsStr == NULL){
		*projectionsSize = 0;
		return NULL;
	}
	char* tempProjections = malloc((strlen(projectionsStr)+1)*sizeof(char));
	strcpy(tempProjections,projectionsStr);
	// Count number of projections
	int counter = 1;
	char* token = strtok(tempProjections," \t");
	while(token!=NULL){
		token = strtok(NULL, " \t");
		if(token != NULL){
			counter++;
		}
	}

	// Free temporary variable used for counter
	free(tempProjections);
	tempProjections = NULL;

	// Create return variable
	tuple* projections = NULL;
	*projectionsSize = counter;
	int position = 0;
	token = strtok(projectionsStr," \t");
	char* remainingLine = strtok(NULL,"");
	char* rel = strtok(token,".");
	char* col = strtok(NULL,"");
	if(rel == NULL || col == NULL){
		*projectionsSize = 0;
		return NULL;
	}
	// Check if relation and column are numeric values
	if(isNumeric(rel) && isNumeric(col)){
		projections = malloc(counter*sizeof(tuple));
		projections[position].rowId = atoi(rel);
		projections[position].value = atoi(col);
	}else{
		*projectionsSize = 0;
		return NULL;
	}

	position++;
	while(position<counter){
		if(position==counter-1){
			token = strtok(remainingLine,"");
		}else{
			token = strtok(remainingLine, " \t");
		}
		remainingLine = strtok(NULL,"");
		if(token != NULL){
			char* rel = strtok(token,".");
			// Check if relation is NULL or not a number
			if(rel == NULL || !isNumeric(rel)){
				free(projections);
				projections = NULL;
				*projectionsSize = 0;
				return NULL;
			}

			projections[position].rowId = atoi(rel);
			char* col = strtok(NULL,"");
			// Check if column is NULL or not a number
			if(col == NULL || !isNumeric(col)){
				free(projections);
				projections = NULL;
				*projectionsSize = 0;
				return NULL;
			}
			projections[position].value = atoi(col);
		}
		position++;
	}
	return projections;
}



predicate** getPredicatesFromLine(char* predicatesStr, int* predicatesSize) {
	if(predicatesStr == NULL){
		*predicatesSize = 0;
		return NULL;
	}
	char* tempPredicates = malloc((strlen(predicatesStr)+1)*sizeof(char));
	strcpy(tempPredicates,predicatesStr);
	// Count number of relations
	int counter = 1;
	char* token = strtok(tempPredicates,"&");
	while(token!=NULL){
		token = strtok(NULL, "&");
		if(token != NULL){
			counter++;
		}
	}

	// Free temporary variable used for counter
	free(tempPredicates);
	tempPredicates = NULL;

	predicate** predicates = NULL;

	// Create returned variable
	*predicatesSize = counter;
	int finalCounter = counter;
	int position = 0;
	predicates = createPredicate(counter);

	// Get first predicate
	token = strtok(predicatesStr,"&");
	char* remainingLine = strtok(NULL,"");
	if(token == NULL){
		*predicatesSize = 0;
		return NULL;
	}

	// Set approriate predicate
	setPredicate(token, &predicates[position],NULL,0);
	position++;
	int exists = 0;
	while(position<counter){
		if(position==counter-1){
			token = strtok(remainingLine,"");
		}else{
			token = strtok(remainingLine, "&");
		}
		remainingLine = strtok(NULL,"");
		if(token != NULL){
			exists = setPredicate(token, &predicates[position], predicates, position);
			//delete last predicate checked
			if(exists){
				predicates[position]->needsToBeDeleted = 1;
				finalCounter--;
			}
		}
		position++;
	}

	// Find out the compare predicates and place them in the front, so as to be executed first
	int comparePredicatesIndex = 0;
	predicate** comparePredicates = createPredicate(*predicatesSize);
	for (int i = 0; i < counter; i++) {
		if (predicates[i]->kind == 0) {
			comparePredicates[comparePredicatesIndex]->kind = predicates[i]->kind;
			comparePredicates[comparePredicatesIndex]->comparator = predicates[i]->comparator;
			comparePredicates[comparePredicatesIndex]->leftSide->rowId = predicates[i]->leftSide->rowId;
			comparePredicates[comparePredicatesIndex]->leftSide->value = predicates[i]->leftSide->value;
			comparePredicates[comparePredicatesIndex]->rightSide->rowId = predicates[i]->rightSide->rowId;
			comparePredicates[comparePredicatesIndex]->rightSide->value = predicates[i]->rightSide->value;
			comparePredicates[comparePredicatesIndex]->needsToBeDeleted = predicates[i]->needsToBeDeleted;
			comparePredicatesIndex++;
		}
	}

	// Now copy the rest of predicates (join) to the rest empty positions of comparePredicates array
	for (int i = 0; i < *predicatesSize; i++) {
		if (predicates[i]->kind != 0) {
			comparePredicates[comparePredicatesIndex]->kind = predicates[i]->kind;
			comparePredicates[comparePredicatesIndex]->comparator = predicates[i]->comparator;
			comparePredicates[comparePredicatesIndex]->leftSide->rowId = predicates[i]->leftSide->rowId;
			comparePredicates[comparePredicatesIndex]->leftSide->value = predicates[i]->leftSide->value;
			comparePredicates[comparePredicatesIndex]->rightSide->rowId = predicates[i]->rightSide->rowId;
			comparePredicates[comparePredicatesIndex]->rightSide->value = predicates[i]->rightSide->value;
			comparePredicates[comparePredicatesIndex]->needsToBeDeleted = predicates[i]->needsToBeDeleted;
			comparePredicatesIndex++;
		}
	}
	// After compare predicates, place a join predicate with a relation that took part in the previous comparison (for optimization)
	for (int i = 0; i < *predicatesSize; i++) {
		if (comparePredicates[i]->kind != 0) {
			int previousCompareRelation = comparePredicates[i-1]->leftSide->rowId;
			if (comparePredicates[i]->leftSide->rowId != previousCompareRelation && comparePredicates[i]->rightSide->rowId != previousCompareRelation) {
				//printPredicate(comparePredicates[i]);
				predicate** temp = createPredicate(1);
				temp[0]->leftSide->rowId = comparePredicates[i]->leftSide->rowId;
				temp[0]->leftSide->value = comparePredicates[i]->leftSide->value;
				temp[0]->rightSide->rowId = comparePredicates[i]->rightSide->rowId;
				temp[0]->rightSide->value = comparePredicates[i]->rightSide->value;
				for (int j = i; j < *predicatesSize; j++) {
					if (comparePredicates[j]->leftSide->rowId == previousCompareRelation || comparePredicates[j]->rightSide->rowId == previousCompareRelation) {
						comparePredicates[i]->leftSide->rowId = comparePredicates[j]->leftSide->rowId;
						comparePredicates[i]->leftSide->value = comparePredicates[j]->leftSide->value;
						comparePredicates[i]->rightSide->rowId = comparePredicates[j]->rightSide->rowId;
						comparePredicates[i]->rightSide->value = comparePredicates[j]->rightSide->value;
						comparePredicates[j]->leftSide->rowId = temp[0]->leftSide->rowId;
						comparePredicates[j]->leftSide->value = temp[0]->leftSide->value;
						comparePredicates[j]->rightSide->rowId = temp[0]->rightSide->rowId;
						comparePredicates[j]->rightSide->value = temp[0]->rightSide->value;
						deletePredicate(&temp[0]);
						free(temp);
						temp = NULL;
						break;
					}
				}
				if (temp != NULL) {
					deletePredicate(&temp[0]);
					free(temp);
				}
			}
			break;
		}
		else if (comparePredicates[i]->kind == 0) continue;
	}


	predicate** finalPredicates = createPredicate(finalCounter);
	int tempCounter = 0;
	for(int i=0;i<counter;i++){
		if(!comparePredicates[i]->needsToBeDeleted){
			if (comparePredicates[i]->kind != 0) {
				finalPredicates[tempCounter]->kind = comparePredicates[i]->kind;
				finalPredicates[tempCounter]->comparator = comparePredicates[i]->comparator;
				finalPredicates[tempCounter]->leftSide->rowId = comparePredicates[i]->leftSide->rowId;
				finalPredicates[tempCounter]->leftSide->value = comparePredicates[i]->leftSide->value;
				finalPredicates[tempCounter]->rightSide->rowId = comparePredicates[i]->rightSide->rowId;
				finalPredicates[tempCounter]->rightSide->value = comparePredicates[i]->rightSide->value;
				finalPredicates[tempCounter]->needsToBeDeleted = comparePredicates[i]->needsToBeDeleted;
			}else{
				finalPredicates[tempCounter]->kind = comparePredicates[i]->kind;
				finalPredicates[tempCounter]->comparator = comparePredicates[i]->comparator;
				finalPredicates[tempCounter]->leftSide->rowId = comparePredicates[i]->leftSide->rowId;
				finalPredicates[tempCounter]->leftSide->value = comparePredicates[i]->leftSide->value;
				finalPredicates[tempCounter]->rightSide->rowId = comparePredicates[i]->rightSide->rowId;
				finalPredicates[tempCounter]->rightSide->value = comparePredicates[i]->rightSide->value;
				finalPredicates[tempCounter]->needsToBeDeleted = comparePredicates[i]->needsToBeDeleted;
			}
			tempCounter++;
		}
	}
	*predicatesSize = finalCounter;
	// Delete the old predicates array
	for (int i = 0; i < counter; i++) {
		deletePredicate(&predicates[i]);
		deletePredicate(&comparePredicates[i]);
	}
	free(predicates);
	predicates = NULL;
	free(comparePredicates);
	comparePredicates = NULL;
	return finalPredicates;
}

// Create predicate struct for query handling
predicate** createPredicate(int size){
	if (size <= 0) {
		return NULL;
	}
	predicate** p = malloc(size*sizeof(predicate*));
	for(int i=0;i<size;i++){
		p[i] = malloc(sizeof(predicate));
		p[i]->leftSide = malloc(sizeof(tuple));
		p[i]->leftSide->rowId = -1;
		p[i]->leftSide->value = -1;
		p[i]->rightSide = malloc(sizeof(tuple));
		p[i]->rightSide->rowId = -1;
		p[i]->rightSide->value = -1;
		p[i]->comparator = '0';
		p[i]->kind = -1;
		p[i]->needsToBeDeleted = 0;
	}
	return p;
}

// Delete a predicate
void deletePredicate(predicate** p){
	free((*p)->leftSide);
	(*p)->leftSide = NULL;
	free((*p)->rightSide);
	(*p)->rightSide = NULL;
	free((*p));
	*p = NULL;
}

int setPredicate(char* str, predicate** p, predicate** allPredicates,int size){
	if(str == NULL){
		*p = NULL;
		return -1;
	}
	// Check for all comparators
	// Check for '>'
	char* strTemp1 = malloc((strlen(str)+1)*sizeof(char));
	strcpy(strTemp1,str);
	char* token = strtok(strTemp1,">");
	if(strcmp(str,token) != 0){
		(*p)->kind = 0;
		char* remainingLine = strtok(NULL,"");
		token = strtok(token,".");
		(*p)->leftSide->rowId = atoi(token);
		token = strtok(NULL,"");
		(*p)->leftSide->value = atoi(token);
		(*p)->comparator = '>';
		(*p)->rightSide->rowId = atoi(remainingLine);
		free(strTemp1);
		//check predicate for same
		return checkIfSamePredicateExists((*p),allPredicates,size);
	}
	free(strTemp1);

	// Check for '<'
	char* strTemp2 = malloc((strlen(str)+1)*sizeof(char));
	strcpy(strTemp2,str);
	token = strtok(strTemp2,"<");
	if(strcmp(str,token) != 0){
		(*p)->kind = 0;
		char* remainingLine = strtok(NULL,"");
		token = strtok(token,".");
		(*p)->leftSide->rowId = atoi(token);
		token = strtok(NULL,"");
		(*p)->leftSide->value = atoi(token);
		(*p)->comparator = '<';
		(*p)->rightSide->rowId = atoi(remainingLine);
		free(strTemp2);
		//check predicate for same
		return checkIfSamePredicateExists((*p),allPredicates,size);
	}

	free(strTemp2);

	// Check for '='
	char* strTemp3 = malloc((strlen(str)+1)*sizeof(char));
	strcpy(strTemp3,str);
	token = strtok(strTemp3,"=");
	if(strcmp(str,token) != 0){
		char* remainingLine = strtok(NULL,"");
		token = strtok(token,".");
		(*p)->leftSide->rowId = atoi(token);
		token = strtok(NULL,"");
		(*p)->leftSide->value = atoi(token);
		(*p)->comparator = '=';
		char* tempRemainingLine = malloc((strlen(remainingLine)+1)*sizeof(char));
		strcpy(tempRemainingLine,remainingLine);
		// Check for filer or join predicate
		token = strtok(tempRemainingLine,".");
		if(strcmp(token,remainingLine) == 0){
			(*p)->rightSide->rowId = atoi(remainingLine);
			(*p)->kind = 0;
		}else{
			(*p)->rightSide->rowId = atoi(token);
			token = strtok(NULL,"");
			(*p)->rightSide->value = atoi(token);
			(*p)->kind = 1;
		}
		free(tempRemainingLine);
		free(strTemp3);
		//check predicate for same
		return checkIfSamePredicateExists((*p),allPredicates,size);
	}
	free(strTemp3);
	return 0;
}


// Check whether a string is number or not, return 0 if not
int isNumeric(char* s){
	if(s == NULL)
		return 0;
	if(s[0] == '\0' || isspace(s[0]))
		return 0;
	for(int i = 0; i < strlen(s) ; i++){
        if (isdigit(s[i]) == 0)
            return 0;
	}
    return 1;
}


//check if same predicate exists in predicates array
int checkIfSamePredicateExists(predicate* p,predicate** allPredicates,int size){
	if(p == NULL || allPredicates == NULL || size == 0){
		return 0;
	}
	for(int i=0;i<size;i++){
		if(checkPredicate(p,allPredicates[i])){
			return 1;
		}
	}
	return 0;
}

//return true if all equals
int checkPredicate(predicate* p1,predicate* p2){
	if(p1 == NULL || p2 == NULL){
		return -1;
	}
	if(p2->needsToBeDeleted){
		return 0;
	}
	int returnValue = (p1->kind == p2->kind && p1->comparator==p2->comparator);
	if(returnValue){
		if(p1->kind==0){
			if(p1->leftSide->rowId == p2->leftSide->rowId && p1->leftSide->value == p2->leftSide->value){
				if(p1->rightSide->rowId == p2->rightSide->rowId){
					return 1;
				}
			}
		}else{
			if(p1->leftSide->rowId == p2->leftSide->rowId && p1->leftSide->value == p2->leftSide->value){
				if(p1->rightSide->rowId == p2->rightSide->rowId && p1->rightSide->value == p2->rightSide->value){
					return 1;
				}
			}else if(p1->leftSide->rowId == p2->rightSide->rowId && p1->leftSide->value == p2->rightSide->value){
				if(p1->rightSide->rowId == p2->leftSide->rowId && p1->rightSide->value == p2->leftSide->value){
					return 1;
				}
			}
		}
	}
	return 0;
}

void printPredicate(predicate* p){
	printf("Printing predicate... ");
	if(p->kind == 0){
		printf("%ld.%ld %c %ld with del: %d\n",p->leftSide->rowId,p->leftSide->value,p->comparator,p->rightSide->rowId,p->needsToBeDeleted);
	}else{
		printf("%ld.%ld %c %ld.%ld with del: %d\n",p->leftSide->rowId,p->leftSide->value,p->comparator,p->rightSide->rowId,p->rightSide->value,p->needsToBeDeleted);
	}
}
