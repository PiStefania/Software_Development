#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "queryMethods.h"
#include "radixHashJoin.h"


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

predicate** getPredicatesFromLine(char* predicatesStr, int* predicatesSize){
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
	setPredicate(token, &predicates[position]);
	position++;
	while(position<counter){
		if(position==counter-1){
			token = strtok(remainingLine,"");
		}else{
			token = strtok(remainingLine, "&");
		}
		remainingLine = strtok(NULL,"");
		if(token != NULL){
			setPredicate(token, &predicates[position]);
		}
		position++;
	}

	// Find out the compare predicates and place them in the front, so as to be executed first
	int comparePredicatesIndex = 0;
	predicate** comparePredicates = createPredicate(*predicatesSize);
	for (int i = 0; i < *predicatesSize; i++) {
		if (predicates[i]->kind == 0) {
			comparePredicates[comparePredicatesIndex]->kind = predicates[i]->kind;
			comparePredicates[comparePredicatesIndex]->comparator = predicates[i]->comparator;
			comparePredicates[comparePredicatesIndex]->leftSide->rowId = predicates[i]->leftSide->rowId;
			comparePredicates[comparePredicatesIndex]->leftSide->value = predicates[i]->leftSide->value;
			comparePredicates[comparePredicatesIndex]->rightSide->rowId = predicates[i]->rightSide->rowId;
			comparePredicates[comparePredicatesIndex]->rightSide->value = predicates[i]->rightSide->value;
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
			comparePredicatesIndex++;
		}
	}
	// Delete the old predicates array
	for (int i = 0; i < *predicatesSize; i++) {
		deletePredicate(&predicates[i]);
	}
	free(predicates);
	predicates = NULL;

	return comparePredicates;
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

void setPredicate(char* str, predicate** p){
	if(str == NULL){
		*p = NULL;
		return;
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
		return;
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
		return;
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
		return;
	}
	free(strTemp3);
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
