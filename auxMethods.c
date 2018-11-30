#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
	int number = atoi(s);
	if (number == 0 && s[0] != '0')
		return 0;
	else
		return 1;
}


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
