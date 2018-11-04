#include <stdio.h>
#include <stdlib.h>
#include "auxMethods.h"

#define COLUMNS 1							// Colums of arrays (for this program, only 1)

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
