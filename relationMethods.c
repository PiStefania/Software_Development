#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "relationMethods.h"


relationsInfo* getRelationsData(FILE* file, int* num_of_initRelations) {
    char *line = NULL;
	size_t len = 0;
    int read;

    // If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		printf("Please input init files (End input with the word 'Done'):\n");
		file = stdin;
	}
    stringNode *filenamesList = createNameList();

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
        strcpy(initRelations[i].relName, findNameByIndex(filenamesList, i));

        // Open the relation binary file
        char filePath[32];
        sprintf(filePath, "./workloads/%s", initRelations[i].relName);
        FILE* relFile;
    	relFile = fopen(filePath, "rb");

        // Read the number of rows and columns of current relation
        fread(&initRelations[i].num_of_rows, sizeof(uint64_t), 1, relFile);
        //printf("%ld\n", initRelations[i].num_of_rows);
        //fseek(relFile, sizeof(uint64_t), SEEK_SET);
        fread(&initRelations[i].num_of_columns, sizeof(uint64_t), 1, relFile);
        //printf("%ld\n", initRelations[i].num_of_columns);

        //TODO: malloc num_of_columns for MDCols
        if ((initRelations[i].MDCols = malloc(initRelations[i].num_of_columns * sizeof(metadataCol))) == NULL) {
          //critical error, not enough memory for metadata
          return NULL;
        }

        // Fill in the arrays with the values from relation File
        initRelations[i].Rarray = malloc(initRelations[i].num_of_columns * sizeof(uint64_t*));
        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            initRelations[i].Rarray[j] = malloc(initRelations[i].num_of_rows * sizeof(uint64_t));
            long int min = -1 , max = -1, discrete_values = -1, y;
            for (int k = 0; k < initRelations[i].num_of_rows; k++) {
                fread(&initRelations[i].Rarray[j][k], sizeof(uint64_t), 1, relFile);
                //if (i == 0) printf("%ld\n", initRelations[i].Rarray[j][k]);

                //find min,max and discrete_values
                if (min == -1) {
                  min = initRelations[i].Rarray[j][k];
                  max = min;
                  discrete_values = 1;
                }
                else if (min > initRelations[i].Rarray[j][k]) {
                  min = initRelations[i].Rarray[j][k];
                }
                else if (max < initRelations[i].Rarray[j][k]) {
                  max = initRelations[i].Rarray[j][k];
                }

                //calculate discrete_values
                for (y = 1; y < k; y++) {
                  if (initRelations[i].Rarray[j][k] == initRelations[i].Rarray[j][y]) {
                    break;
                  }
                }
                if (k == y) {
                  discrete_values++;
                }
            }
            initRelations[i].MDCols[j].num_of_rows = initRelations[i].num_of_rows;
            initRelations[i].MDCols[j].min = (uint32_t)min;
            initRelations[i].MDCols[j].max = (uint32_t)max;
            initRelations[i].MDCols[j].discrete_values = (uint32_t)discrete_values;
        }
        fclose(relFile);
    }

    // Free dynamically allocated structures
    deleteNameList(filenamesList);
	if (line) free(line);
	if (file != NULL && file != stdin) fclose(file);            // Close file

    return initRelations;
}


// Delete relations array
void deleteRelationsData(relationsInfo* initRelations, int num_of_initRelations) {
    for (int i = 0; i < num_of_initRelations; i++) {
        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            free(initRelations[i].Rarray[j]);
        }
        free(initRelations[i].Rarray);
        free(initRelations[i].MDCols);
    }
    free(initRelations);
}


// Create a list to store the names of data files
stringNode* createNameList() {
	stringNode* nameList;
	if ((nameList = malloc(sizeof(stringNode))) == NULL) return NULL;
	nameList->isEmptyList = 1;
	nameList->next = NULL;
	return nameList;
}

// Insert strings into the list (helps while reading files)
int insertIntoNameList(stringNode* nameList, char* name) {
	stringNode *currentNode, *newNode;
	if (nameList->isEmptyList == 1){
		strcpy(nameList->name, name);
		nameList->isEmptyList = 0;
		return 1;
	}
	currentNode = nameList;
	while (currentNode->next != NULL) {
		currentNode = currentNode->next;
	}
	if ((newNode = malloc(sizeof(stringNode))) == NULL) return 0;
	strcpy(newNode->name, name);
	newNode->next = NULL;
	currentNode->next = newNode;
	return 1;
}

// Look for the "index" node and retrieve the name value
char* findNameByIndex(stringNode* nameList, int index) {
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
void deleteNameList(stringNode* nameList) {
	stringNode *currentNode;
	while (nameList->next != NULL){
		currentNode = nameList;
		nameList = nameList->next;
		free(currentNode);
	}
	free(nameList);
}
