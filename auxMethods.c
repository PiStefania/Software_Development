#include <stdio.h>
#include <stdlib.h>
#include "auxMethods.h"

int* getColumnOfArray(int** array, int rows, int col){
	int* column = malloc(rows*sizeof(int));
	for(int i=0;i<rows;i++){
		column[i] = array[i][col];
	}
	return column;
}
