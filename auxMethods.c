#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "auxMethods.h"

#define COLUMNS 1							// Columns of arrays (for this program, only 1)

int* getColumnOfArray(int** array, int rows, int col){
	if(col<COLUMNS && col>=0){
		int* column = malloc(rows*sizeof(int));
		for(int i=0;i<rows;i++){
			column[i] = array[i][col];
		}
		return column;
	}
	return NULL;
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
