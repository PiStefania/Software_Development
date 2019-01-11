#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include "../include/relationMethods.h"


relationsInfo* getRelationsData(FILE* file, char* initPath, int* num_of_initRelations) {
    char *line = NULL;
	size_t len = 0;
    int read;

    // If file is not provided as an argument, get lines from stdin
  	if (file == NULL){
    	//printf("Please input init files (End input with the word 'Done'):\n");
    	file = stdin;
  	}
    stringNode *filenamesList = createNameList();

    // Find the path until .init file, so as to open properly the files r0, r1.. that .init points to
    char *token, dataFileName[PATH_MAX], s[2] = "/";
    int numOfTokens = 0;
    strcpy(dataFileName, "");
    strcpy(dataFileName, initPath);
    token = strtok(dataFileName, s);
    while (token != NULL) {
        token = strtok(NULL, s);
        if (token != NULL) numOfTokens++;
    }
    strcpy(dataFileName, "./");
    token = strtok(initPath, s);
    for (int i = 0; i < numOfTokens-1; i++) {
        token = strtok(NULL, s);
        strcat(dataFileName, token);
        strcat(dataFileName, "/");
    }
    // Change directory in order to read the data files, and keep the current one so as to come back later
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) == NULL) return NULL;
    chdir(dataFileName);

    // Read the data files that .init points to
    while ((read = getline(&line, &len, file)) != -1) {
        // Get the binary files, as each of them is a relation
        char* lineStr = strtok(line,"\n");
        if (lineStr == NULL) continue;
        if (strcmp(lineStr, "Done") == 0) break;
        if (insertIntoNameList(filenamesList, lineStr) == 0) return NULL;
        (*num_of_initRelations)++;
    }

    relationsInfo *initRelations;
    initRelations = malloc(*num_of_initRelations * sizeof(relationsInfo));

    for (int i = 0; i < *num_of_initRelations; i++) {
        // Read all relations and their data
        char* tempStr = findNameByIndex(filenamesList, i);
        initRelations[i].relName = malloc((strlen(tempStr)+1)*sizeof(char));
        strcpy(initRelations[i].relName, tempStr);

        // Open the relation binary file
        FILE* relFile = NULL;
    	relFile = fopen(initRelations[i].relName, "rb");
        // Read the number of rows and columns of current relation
        fread(&initRelations[i].num_of_rows, sizeof(uint64_t), 1, relFile);
        fread(&initRelations[i].num_of_columns, sizeof(uint64_t), 1, relFile);

        // Fill in the arrays with the values from relation File and their metadata
        initRelations[i].Rarray = malloc(initRelations[i].num_of_columns * sizeof(uint64_t*));
        initRelations[i].MDCols = malloc(initRelations[i].num_of_columns * sizeof(metadataCol));

        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            initRelations[i].Rarray[j] = malloc(initRelations[i].num_of_rows * sizeof(uint64_t));

            uint64_t min = 0, max = 0;
            for (int k = 0; k < initRelations[i].num_of_rows; k++) {
                fread(&initRelations[i].Rarray[j][k], sizeof(uint64_t), 1, relFile);
                // Find min, max for each column
                if (k == 0) {                                       //first element of current column
                    min = initRelations[i].Rarray[j][k];
                    max = min;
                }
                else {                                              //every other element of current column
                    if (min > initRelations[i].Rarray[j][k]) {
                        min = initRelations[i].Rarray[j][k];
                    }
                    else if (max < initRelations[i].Rarray[j][k]) {
                        max = initRelations[i].Rarray[j][k];
                    }
                }
            }
            initRelations[i].MDCols[j].num_of_data = initRelations[i].num_of_rows;
            initRelations[i].MDCols[j].min = (uint32_t)min;
            initRelations[i].MDCols[j].max = (uint32_t)max;

            // Find the number of discrete values
            int valuesOffset = max - min + 1;
            if (valuesOffset > MAX_DISCRETE_VALUES) {
                valuesOffset = MAX_DISCRETE_VALUES;
            }
            //printf("%d\n", valuesOffset);
            char *discreteItems = malloc(valuesOffset * sizeof(char));
            for (int k = 0; k < valuesOffset; k++) {
                discreteItems[k] = 0;
            }
            uint32_t discreteValues = 0;
            for (int k = 0; k < initRelations[i].num_of_rows; k++) {
                int item = (initRelations[i].Rarray[j][k] - min) % valuesOffset;
                if (discreteItems[item] == 0) {
                    discreteItems[item] = 1;
                    discreteValues++;
                }
            }
            initRelations[i].MDCols[j].discrete_values = discreteValues;
            free(discreteItems);

            //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", i, j, initRelations[i].MDCols[j].num_of_data,
            //                initRelations[i].MDCols[j].min, initRelations[i].MDCols[j].max, initRelations[i].MDCols[j].discrete_values);
        }
        fclose(relFile);
    }
    // Go back to initial directory
    chdir(cwd);

    // Free dynamically allocated structures
    deleteNameList(&filenamesList);
  	if (line) free(line);
  	if (file != NULL && file != stdin) fclose(file);            // Close file

    return initRelations;
}


// Delete relations array
void deleteRelationsData(relationsInfo* initRelations, int* num_of_initRelations) {
    if (initRelations == NULL || num_of_initRelations <= 0){
      return;
    }
    for (int i = 0; i < *num_of_initRelations; i++) {
        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            free(initRelations[i].Rarray[j]);
        }
        free(initRelations[i].relName);
        free(initRelations[i].Rarray);
        free(initRelations[i].MDCols);
    }
    free(initRelations);
    *num_of_initRelations = -1;
    initRelations = NULL;
}


// Create a list to store the names of data files
stringNode* createNameList() {
  	stringNode* nameList;
  	if ((nameList = malloc(sizeof(stringNode))) == NULL) return NULL;
  	nameList->isEmptyList = 1;
  	nameList->next = NULL;
    nameList->name = NULL;
  	return nameList;
}

// Insert strings into the list (helps while reading files)
int insertIntoNameList(stringNode* nameList, char* name) {
    if(name == NULL || nameList == NULL){
      return 0;
    }
  	stringNode *currentNode, *newNode;
  	if (nameList->isEmptyList == 1){
        nameList->name = malloc((strlen(name)+1)*sizeof(char));
    		strcpy(nameList->name, name);
    		nameList->isEmptyList = 0;
    		return 1;
  	}
  	currentNode = nameList;
  	while (currentNode->next != NULL) {
  		  currentNode = currentNode->next;
  	}
  	if ((newNode = malloc(sizeof(stringNode))) == NULL) return 0;
    newNode->name = malloc((strlen(name)+1)*sizeof(char));
  	strcpy(newNode->name, name);
  	newNode->next = NULL;
  	currentNode->next = newNode;
  	return 1;
}

// Look for the "index" node and retrieve the name value
char* findNameByIndex(stringNode* nameList, int index) {
    if(nameList == NULL || index < 0){
      return NULL;
    }
  	stringNode *currentNode = nameList;
  	int currentIndex = 0;
  	do {
    	if (currentIndex == index) {
    		return currentNode->name;
    	}
    	currentNode = currentNode->next;
    	currentIndex++;
  	} while (currentNode != NULL);
  	return NULL;
}

// Delete above list
void deleteNameList(stringNode** nameList) {
  	stringNode *currentNode = *nameList;
    stringNode *tempNode;
  	while (currentNode != NULL){
  		tempNode = currentNode;
  		currentNode = currentNode->next;
        free(tempNode->name);
  		free(tempNode);
  	}
    *nameList = NULL;
}
