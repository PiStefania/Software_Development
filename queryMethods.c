#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "queryMethods.h"
#include "auxMethods.h"
#include "radixHashJoin.h"

int getQueryLines(FILE* file){
	char *line = NULL;
	size_t len = 0;
	int read;
	int failed = 0;
	
	// If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		printf("Please input queries:\n");
		file = stdin;
	}
		
	while ((read = getline(&line, &len, file)) != -1) {
		// Get line and each section
		char* lineStr = strtok(line, "\n");
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
				break;
			}else{
				for(int i=0;i<relationsSize;i++){
					printf("rel: %d\n",relations[i]);
				}
			}
		}
		
		// Get predicates
		
		// Get projections
		tuple* projections = NULL;
		int projectionsSize = 0;
		if(projectionsStr != NULL){
			projections = getProjectionsFromLine(projectionsStr,&projectionsSize);
			if(projections == NULL){
				printf("Projections are incorrect!\n");
				failed = 1;
				break;
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
		if(projections){
			free(projections);
			projections=NULL;
		}
	}
	
	// Free vars
	if(line){
		free(line);
		line=NULL;
	}
	
	
	
	if(failed)
		return 0;
	return 1;
}

int* getRelationsFromLine(char* relationsStr, int* relationsSize){
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
		return NULL;
	}
	// TODO Check if token is number before conversion
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
				relations = NULL;
				return NULL;
			}
		}
		position++;
	}
	
	return relations;
}


tuple* getProjectionsFromLine(char* projectionsStr, int* projectionsSize){
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

	tuple* projections = NULL;
	
	*projectionsSize = counter;
	// Convert str to int
	int position = 0;
	token = strtok(projectionsStr," \t");
	char* remaningLine = strtok(NULL,"");
	projections = malloc(counter*sizeof(tuple));
	char* rel = strtok(token,".");
	projections[position].rowId = atoi(rel);
	char* col = strtok(NULL,"");
	projections[position].value = atoi(col);
	
	// TODO Check if token is number before conversion
	position++;
	while(position<counter){
		if(position==counter-1){
			token = strtok(remaningLine,"");
		}else{
			token = strtok(remaningLine, " \t");
		}
		if(token != NULL){
			char* rel = strtok(token,".");
			projections[position].rowId = atoi(rel);
			char* col = strtok(NULL,"");
			projections[position].value = atoi(col);
			remaningLine = strtok(NULL,"");
		}
		position++;
	}
	
	return projections;
}



