#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "relationMethods.h"
#include "auxMethods.h"


relationsInfo* getRelationsData(FILE* file, int* num_of_initRelations) {
    char *line = NULL;
	size_t len = 0;
    int read;

    // If file is not provided as an argument, get lines from stdin
	if (file == NULL){
		printf("Please input init files:\n");
		file = stdin;
	}
    stringNode *filenamesList = createNameList();

    while ((read = getline(&line, &len, file)) != -1) {
        // Get the binary files, as each of them is a relation
        char* lineStr = strtok(line,"\n");
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

        // Fill in the arrays with the values from relation File
        initRelations[i].Rarray = malloc(initRelations[i].num_of_columns * sizeof(uint64_t*));
        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            initRelations[i].Rarray[j] = malloc(initRelations[i].num_of_rows * sizeof(uint64_t));
            for (int k = 0; k < initRelations[i].num_of_rows; k++) {
                fread(&initRelations[i].Rarray[j][k], sizeof(uint64_t), 1, relFile);
                //if (i == 0) printf("%ld\n", initRelations[i].Rarray[j][k]);
            }
        }
        fclose(relFile);
    }

    // Free dynamically allocated structures
    deleteNameList(filenamesList);
	if (line) free(line);
	if (file != NULL) fclose(file);            // Close file

    return initRelations;
}


// Delete relations array
void deleteRelationsData(relationsInfo* initRelations, int num_of_initRelations) {
    for (int i = 0; i < num_of_initRelations; i++) {
        for (int j = 0; j < initRelations[i].num_of_columns; j++) {
            free(initRelations[i].Rarray[j]);
        }
        free(initRelations[i].Rarray);
    }
    free(initRelations);
}
