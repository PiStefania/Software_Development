#ifndef _AUXMETHODS_H_
#define _AUXMETHODS_H_

#define STRINGLEN 5


typedef struct stringNode {
    char isEmptyList;               // 1 true, 0 false
    char name[STRINGLEN];
    struct stringNode* next;
} stringNode;


int* getColumnOfArray(int** array, int rows, int cols);
int isNumeric(char* s);

stringNode* createNameList();
int insertIntoNameList(stringNode* nameList, char* name);
char* findNameByIndex(stringNode* nameList, int index);
void deleteNameList(stringNode* nameList);

#endif
