#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "statisticsMethods.h"
#include "relationMethods.h"
#include "queryMethods.h"


// Initial check for compare statistics
int checkCompareStatistics(predicate** predicates, metadataCol** queryMetadata, metadataCol* oldMetadata, int currentPredicate, int relationId, int relColumn) {
    // Print initial statistics (optional)
    //printf("Old - Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId, relColumn, queryMetadata[relationId][relColumn].num_of_data,
    //            queryMetadata[relationId][relColumn].min, queryMetadata[relationId][relColumn].max, queryMetadata[relationId][relColumn].discrete_values);
    uint64_t compareValue = predicates[currentPredicate]->rightSide->rowId;
    int outOfBoundaries = 0;
    // Keep some previous statistics that may be used in the calculation of the new ones later
    oldMetadata->discrete_values = queryMetadata[relationId][relColumn].discrete_values;
    oldMetadata->num_of_data = queryMetadata[relationId][relColumn].num_of_data;
    oldMetadata->min = queryMetadata[relationId][relColumn].min;
    oldMetadata->max = queryMetadata[relationId][relColumn].max;
    // Check if we need to check the array, and update some statistics
    if (predicates[currentPredicate]->comparator == '=') {
        if (compareValue < queryMetadata[relationId][relColumn].min || compareValue > queryMetadata[relationId][relColumn].max) {
            outOfBoundaries = 1;
        }
        queryMetadata[relationId][relColumn].min = compareValue;
        queryMetadata[relationId][relColumn].max = compareValue;
    }
    else if (predicates[currentPredicate]->comparator == '>') {
        if (compareValue > queryMetadata[relationId][relColumn].max) {
            outOfBoundaries = 1;
        }
        queryMetadata[relationId][relColumn].min = compareValue;
    }
    else if (predicates[currentPredicate]->comparator == '<') {
        if (compareValue < queryMetadata[relationId][relColumn].min) {
            outOfBoundaries = 1;
        }
        queryMetadata[relationId][relColumn].max = compareValue;
    }
    return outOfBoundaries;
}



void updateCompareStatistics(predicate** predicates, relationsInfo* initRelations, metadataCol** queryMetadata, metadataCol* oldMetadata,
                                        int currentPred, int relationId, int relColumn, char foundVal) {
    // Update the rest of statistics for compare predicates
    uint64_t compareValue = predicates[currentPred]->rightSide->rowId;
    // For each kind of compare, there are different types of statistics calculations
    if (predicates[currentPred]->comparator == '=') {
        if (foundVal == 1) {
            float fraction = (float)oldMetadata->num_of_data / (float)queryMetadata[relationId][relColumn].discrete_values;
            queryMetadata[relationId][relColumn].num_of_data = (uint32_t)roundf(fraction);
            queryMetadata[relationId][relColumn].discrete_values = 1;
        }
        else if (foundVal == 0) {
            queryMetadata[relationId][relColumn].num_of_data = 0;
            queryMetadata[relationId][relColumn].discrete_values = 0;
            //rList[predicates[currentPred]->leftSide->rowId].num_of_rowIds = -1;
        }
    }
    else if (predicates[currentPred]->comparator == '>') {
        if (compareValue < oldMetadata->min) {
            compareValue = oldMetadata->min;
        }
        float fraction = (float)(oldMetadata->max - compareValue) / (float)(oldMetadata->max - oldMetadata->min);
        queryMetadata[relationId][relColumn].discrete_values = (uint32_t)roundf((float)oldMetadata->discrete_values * fraction);
        queryMetadata[relationId][relColumn].num_of_data = (uint32_t)roundf((float)oldMetadata->num_of_data * fraction);
    }
    else if (predicates[currentPred]->comparator == '<') {
        if (compareValue > oldMetadata->max) {
            compareValue = oldMetadata->max;
        }
        float fraction = (float)(compareValue - oldMetadata->min) / (float)(oldMetadata->max - oldMetadata->min);
        queryMetadata[relationId][relColumn].discrete_values = (uint32_t)roundf((float)oldMetadata->discrete_values * fraction);
        queryMetadata[relationId][relColumn].num_of_data = (uint32_t)roundf((float)oldMetadata->num_of_data * fraction);
    }
    for (int j = 0; j < initRelations[relationId].num_of_columns; j++) {
        if (j != relColumn) {
            float fraction = (float)queryMetadata[relationId][relColumn].num_of_data / (float)oldMetadata->num_of_data;
            float exponent = (float)queryMetadata[relationId][j].num_of_data / (float)queryMetadata[relationId][j].discrete_values;
            float new_discrete_values = (float)queryMetadata[relationId][j].discrete_values * (float)(1.0 - (float)pow((1.0 - fraction), exponent));
            //printf("New Discrete: %f - Fraction: %f - Exponent: %f\n", new_discrete_values, fraction, exponent);
            queryMetadata[relationId][j].discrete_values = (uint32_t)roundf(new_discrete_values);
            queryMetadata[relationId][j].num_of_data = queryMetadata[relationId][relColumn].num_of_data;
        }
        //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId, j, queryMetadata[relationId][j].num_of_data,
        //        queryMetadata[relationId][j].min, queryMetadata[relationId][j].max, queryMetadata[relationId][j].discrete_values);
    }
}


void updateJoinStatistics(predicate** predicates, relationsInfo* initRelations, int* relations, metadataCol** queryMetadata, int currentPredicate) {
    // Find each side's relationId and column
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    int relationId2 = relations[predicates[currentPredicate]->rightSide->rowId];
    int relColumn2 = predicates[currentPredicate]->rightSide->value;
    // Print initial statistics (optional)
    /*printf("Old1 - Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId1, relColumn1,
            queryMetadata[relationId1][relColumn1].num_of_data, queryMetadata[relationId1][relColumn1].min,
            queryMetadata[relationId1][relColumn1].max, queryMetadata[relationId1][relColumn1].discrete_values);
    printf("Old2 - Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId2, relColumn2,
            queryMetadata[relationId2][relColumn2].num_of_data, queryMetadata[relationId2][relColumn2].min,
            queryMetadata[relationId2][relColumn2].max, queryMetadata[relationId2][relColumn2].discrete_values);*/
    // Check if the filter consists of columns from the same relation
    if (relationId1 == relationId2) {
        // Maybe there is a selfJoin, aka same relation and same columns
        if (relColumn1 == relColumn2) {
            //printf("self Join\n");
            // Update only the number of data here
            uint32_t old_num_of_data1 = queryMetadata[relationId1][relColumn1].num_of_data;
            uint32_t nValue = queryMetadata[relationId1][relColumn1].max - queryMetadata[relationId1][relColumn1].min + 1;
            float resultFraction = (float)(old_num_of_data1 * old_num_of_data1) / (float)nValue;
            queryMetadata[relationId1][relColumn1].num_of_data = (uint32_t)roundf(resultFraction);
            for (int j = 0; j < initRelations[relationId1].num_of_columns; j++) {
                if (j != relColumn1) {
                    queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
                }
                //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId1, j, queryMetadata[relationId1][j].num_of_data,
                //        queryMetadata[relationId1][j].min, queryMetadata[relationId1][j].max, queryMetadata[relationId1][j].discrete_values);
            }
        } else {    // Update minimum values
            //printf("Same Relation\n");
            uint32_t maxLower = queryMetadata[relationId1][relColumn1].min;
            if (queryMetadata[relationId2][relColumn2].min > maxLower) {
                maxLower = queryMetadata[relationId2][relColumn2].min;
            }
            queryMetadata[relationId1][relColumn1].min = maxLower;
            queryMetadata[relationId2][relColumn2].min = maxLower;
            // Now update maximum values
            uint32_t minUpper = queryMetadata[relationId1][relColumn1].max;
            if (queryMetadata[relationId2][relColumn2].max < minUpper) {
                minUpper = queryMetadata[relationId2][relColumn2].max;
            }
            queryMetadata[relationId1][relColumn1].max = minUpper;
            queryMetadata[relationId2][relColumn2].max = minUpper;
            // Update number of data
            uint32_t old_num_of_data1 = queryMetadata[relationId1][relColumn1].num_of_data;
            uint32_t nValue = queryMetadata[relationId1][relColumn1].max - queryMetadata[relationId1][relColumn1].min + 1;
            float resultFraction = (float)old_num_of_data1 / (float)nValue;
            queryMetadata[relationId1][relColumn1].num_of_data = (uint32_t)roundf(resultFraction);
            queryMetadata[relationId2][relColumn2].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
            // Update discrete values
            float fraction = (float)queryMetadata[relationId1][relColumn1].num_of_data / (float)old_num_of_data1;
            float exponent = (float)old_num_of_data1 / (float)queryMetadata[relationId1][relColumn1].discrete_values;
            float new_discrete_values1 = (float)queryMetadata[relationId1][relColumn1].discrete_values * (float)(1.0 - (float)pow((1.0 - fraction), exponent));
            queryMetadata[relationId1][relColumn1].discrete_values = (uint32_t)roundf(new_discrete_values1);
            queryMetadata[relationId2][relColumn2].discrete_values = queryMetadata[relationId1][relColumn1].discrete_values;
            // Update the rest columns of relation
            for (int j = 0; j < initRelations[relationId1].num_of_columns; j++) {
                if (j != relColumn1 && j != relColumn2) {
                    float fraction = (float)queryMetadata[relationId1][relColumn1].num_of_data / (float)old_num_of_data1;
                    float exponent = (float)queryMetadata[relationId1][j].num_of_data / (float)queryMetadata[relationId1][j].discrete_values;
                    float new_discrete_values = (float)queryMetadata[relationId1][j].discrete_values * (float)(1.0 - (float)pow((1.0 - fraction), exponent));
                    //printf("New Discrete: %f - Fraction: %f - Exponent: %f\n", new_discrete_values, fraction, exponent);
                    queryMetadata[relationId1][j].discrete_values = (uint32_t)roundf(new_discrete_values);
                    queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
                }
                //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId1, j, queryMetadata[relationId1][j].num_of_data,
                //        queryMetadata[relationId1][j].min, queryMetadata[relationId1][j].max, queryMetadata[relationId1][j].discrete_values);
            }
        }
    } else {    // Normal Join
        //printf("Normal Join\n");
        // Update minimum values
        uint32_t maxLower = queryMetadata[relationId1][relColumn1].min;
        if (queryMetadata[relationId2][relColumn2].min > maxLower) {
            maxLower = queryMetadata[relationId2][relColumn2].min;
        }
        queryMetadata[relationId1][relColumn1].min = maxLower;
        queryMetadata[relationId2][relColumn2].min = maxLower;
        // Now update maximum values
        uint32_t minUpper = queryMetadata[relationId1][relColumn1].max;
        if (queryMetadata[relationId2][relColumn2].max < minUpper) {
            minUpper = queryMetadata[relationId2][relColumn2].max;
        }
        queryMetadata[relationId1][relColumn1].max = minUpper;
        queryMetadata[relationId2][relColumn2].max = minUpper;
        // Update number of data
        uint32_t old_num_of_data1 = queryMetadata[relationId1][relColumn1].num_of_data;
        uint32_t old_num_of_data2 = queryMetadata[relationId2][relColumn2].num_of_data;
        uint32_t nValue = queryMetadata[relationId1][relColumn1].max - queryMetadata[relationId1][relColumn1].min + 1;
        float resultFraction = (float)(old_num_of_data1 * old_num_of_data2) / (float)nValue;
        queryMetadata[relationId1][relColumn1].num_of_data = (uint32_t)roundf(resultFraction);
        queryMetadata[relationId2][relColumn2].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
        // Update number of data
        uint32_t old_discrete_values1 = queryMetadata[relationId1][relColumn1].discrete_values;
        uint32_t old_discrete_values2 = queryMetadata[relationId2][relColumn2].discrete_values;
        resultFraction = (float)(old_discrete_values1 * old_discrete_values2) / (float)nValue;
        queryMetadata[relationId1][relColumn1].discrete_values = (uint32_t)roundf(resultFraction);
        queryMetadata[relationId2][relColumn2].discrete_values = queryMetadata[relationId1][relColumn1].discrete_values;
        // Update the rest columns of relation 1
        for (int j = 0; j < initRelations[relationId1].num_of_columns; j++) {
            if (j != relColumn1) {
                float fraction = (float)queryMetadata[relationId1][relColumn1].discrete_values / (float)old_discrete_values1;
                float exponent = (float)queryMetadata[relationId1][j].num_of_data / (float)queryMetadata[relationId1][j].discrete_values;
                float new_discrete_values = (float)queryMetadata[relationId1][j].discrete_values * (float)(1.0 - (float)pow((1.0 - fraction), exponent));
                //printf("New Discrete: %f - Fraction: %f - Exponent: %f\n", new_discrete_values, fraction, exponent);
                queryMetadata[relationId1][j].discrete_values = (uint32_t)roundf(new_discrete_values);
                queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
            }
            //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId1, j, queryMetadata[relationId1][j].num_of_data,
            //        queryMetadata[relationId1][j].min, queryMetadata[relationId1][j].max, queryMetadata[relationId1][j].discrete_values);
        }
        // Now update the rest columns of relation 2
        for (int j = 0; j < initRelations[relationId2].num_of_columns; j++) {
            if (j != relColumn2) {
                float fraction = (float)queryMetadata[relationId2][relColumn2].discrete_values / (float)old_discrete_values2;
                float exponent = (float)queryMetadata[relationId2][j].num_of_data / (float)queryMetadata[relationId2][j].discrete_values;
                float new_discrete_values = (float)queryMetadata[relationId2][j].discrete_values * (float)(1.0 - (float)pow((1.0 - fraction), exponent));
                //printf("New Discrete: %f - Fraction: %f - Exponent: %f\n", new_discrete_values, fraction, exponent);
                queryMetadata[relationId2][j].discrete_values = (uint32_t)roundf(new_discrete_values);
                queryMetadata[relationId2][j].num_of_data = queryMetadata[relationId2][relColumn2].num_of_data;
            }
            //printf("Rel: %d.%d - Num Of Data: %d - Min: %d - Max: %d - Discrete Values: %d\n", relationId2, j, queryMetadata[relationId2][j].num_of_data,
            //        queryMetadata[relationId2][j].min, queryMetadata[relationId2][j].max, queryMetadata[relationId2][j].discrete_values);
        }
    }
}


// Copy values from one metadata structure to another
void copyMetadata(relationsInfo* initRelations, int num_of_initRelations, int* relations, int relationsSize, metadataCol** queryMetadata) {
    // If we do not copy for the first time, copy only the metadata changed in this query
    if (relations != NULL) {
        for (int i = 0; i < relationsSize; i++) {
            for (int j = 0; j < initRelations[relations[i]].num_of_columns; j++) {
                queryMetadata[relations[i]][j].min = initRelations[relations[i]].MDCols[j].min;
                queryMetadata[relations[i]][j].max = initRelations[relations[i]].MDCols[j].max;
                queryMetadata[relations[i]][j].num_of_data = initRelations[relations[i]].MDCols[j].num_of_data;
                queryMetadata[relations[i]][j].discrete_values = initRelations[relations[i]].MDCols[j].discrete_values;
            }
        }       // Below: Copy statistics for the first time, just before the first query implementation
    } else {
        for (int i = 0; i < num_of_initRelations; i++) {
            for (int j = 0; j < initRelations[i].num_of_columns; j++) {
                queryMetadata[i][j].min = initRelations[i].MDCols[j].min;
                queryMetadata[i][j].max = initRelations[i].MDCols[j].max;
                queryMetadata[i][j].num_of_data = initRelations[i].MDCols[j].num_of_data;
                queryMetadata[i][j].discrete_values = initRelations[i].MDCols[j].discrete_values;
            }
        }
    }
}
