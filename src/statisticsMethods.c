#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "../include/statisticsMethods.h"
#include "../include/relationMethods.h"
#include "../include/queryMethods.h"


// Initial check for compare statistics
int checkCompareStatistics(predicate** predicates, metadataCol** queryMetadata, metadataCol* oldMetadata, int currentPredicate, int relationId, int relColumn) {
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



uint32_t updateCompareStatistics(predicate** predicates, relationsInfo* initRelations, metadataCol** queryMetadata, metadataCol* oldMetadata,
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
            queryMetadata[relationId][j].discrete_values = (uint32_t)roundf(new_discrete_values);
            queryMetadata[relationId][j].num_of_data = queryMetadata[relationId][relColumn].num_of_data;
        }
    }
    return queryMetadata[relationId][relColumn].num_of_data;
}


uint32_t updateJoinStatistics(predicate** predicates, relationsInfo* initRelations, int* relations, metadataCol** queryMetadata, int currentPredicate) {
    // Find each side's relationId and column
    int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
    int relColumn1 = predicates[currentPredicate]->leftSide->value;
    int relationId2 = relations[predicates[currentPredicate]->rightSide->rowId];
    int relColumn2 = predicates[currentPredicate]->rightSide->value;
    // Check if the filter consists of columns from the same relation
    if (relationId1 == relationId2) {
        // Maybe there is a selfJoin, aka same relation and same columns
        if (relColumn1 == relColumn2) {
            // Update only the number of data here
            uint32_t old_num_of_data1 = queryMetadata[relationId1][relColumn1].num_of_data;
            uint32_t nValue = queryMetadata[relationId1][relColumn1].max - queryMetadata[relationId1][relColumn1].min + 1;
            float resultFraction = (float)(old_num_of_data1 * old_num_of_data1) / (float)nValue;
            queryMetadata[relationId1][relColumn1].num_of_data = (uint32_t)roundf(resultFraction);
            for (int j = 0; j < initRelations[relationId1].num_of_columns; j++) {
                if (j != relColumn1) {
                    queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
                }
            }
        } else {    // Update minimum values
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
                    queryMetadata[relationId1][j].discrete_values = (uint32_t)roundf(new_discrete_values);
                    queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
                }
            }
        }
    } else {    // Normal Join
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
                queryMetadata[relationId1][j].discrete_values = (uint32_t)roundf(new_discrete_values);
                queryMetadata[relationId1][j].num_of_data = queryMetadata[relationId1][relColumn1].num_of_data;
            }
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
        }
    }
    return queryMetadata[relationId1][relColumn1].num_of_data;
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



int joinEnumeration(predicate** predicates, int predicatesSize, relationsInfo* initRelations, int num_of_initRelations, int* relations, metadataCol** queryMetadata) {
    // Find the cost summary of all compare predicates
    uint32_t totalCost = 0;
    int currentPredicate = 0;
    while (predicates[currentPredicate]->kind == 0) {
        // Get relation and column that we need to compare, from predicate
        int relationId1 = relations[predicates[currentPredicate]->leftSide->rowId];
        int relColumn = predicates[currentPredicate]->leftSide->value;
        // Check if the compare values are legitimate for this column and update the first statistics if so
        metadataCol *oldMetadata = malloc(sizeof(metadataCol));
        int outOfBoundaries = checkCompareStatistics(predicates, queryMetadata, oldMetadata, currentPredicate, relationId1, relColumn);
        if (outOfBoundaries == 1) {
            free(oldMetadata);
            return 0;
        }
        // Now update the current compare statistics
        totalCost += updateCompareStatistics(predicates, initRelations, queryMetadata, oldMetadata, currentPredicate, relationId1, relColumn, 1);
        free(oldMetadata);
        currentPredicate++;
    }
    // Create an array that shows the final order of each predicate (until a predicate is examined, its value is -1)
    int* predicatesFinalPosition = malloc(predicatesSize * sizeof(int));
    for (int i = 0; i < predicatesSize; i++) {
        if (i < currentPredicate) {
            predicatesFinalPosition[i] = i;
        } else {
            predicatesFinalPosition[i] = -1;
        }
    }
    // Find the best path by a semi-recursive function
    totalCost += findBestPath(predicates, predicatesSize, initRelations, num_of_initRelations, relations, queryMetadata, predicatesFinalPosition);
    // Rearrange the predicates array
    int rearrangedIndex = 0;
	predicate** copiedPredicates = createPredicate(predicatesSize);
	for (int i = 0; i < predicatesSize; i++) {
		copiedPredicates[i]->kind = predicates[i]->kind;
		copiedPredicates[i]->comparator = predicates[i]->comparator;
		copiedPredicates[i]->leftSide->rowId = predicates[i]->leftSide->rowId;
		copiedPredicates[i]->leftSide->value = predicates[i]->leftSide->value;
		copiedPredicates[i]->rightSide->rowId = predicates[i]->rightSide->rowId;
		copiedPredicates[i]->rightSide->value = predicates[i]->rightSide->value;
		copiedPredicates[i]->needsToBeDeleted = predicates[i]->needsToBeDeleted;
	}
    for (int i = 0; i < predicatesSize; i++) {
        rearrangedIndex = predicatesFinalPosition[i];
        // Check if the expected predicate (from joinEnumeration), which will take place after the predicate, is refered to the same relation
        if (predicates[i]->kind == 1 && predicates[i-1]->kind == 0) {
            int foundIndex = 0;
            for (int j = 0; j < predicatesSize; j++) {
                if (predicatesFinalPosition[j] == i) {
                    foundIndex = j;
                    break;              // If it does not consist the same relation as the compare predicate, then do not use join Enumeration results
                }                       // We do this as it is sure that a relation same to the compare one, uses less values than any other relation
            }
            if (predicates[foundIndex]->leftSide->rowId != predicates[i-1]->leftSide->rowId &&
                    predicates[foundIndex]->rightSide->rowId != predicates[i-1]->leftSide->rowId) {
                        break;
                    }                   // Sometimes the statistics are not accurate at all, as a result we push our own improvements, like this one
        }
		predicates[rearrangedIndex]->kind = copiedPredicates[i]->kind;
		predicates[rearrangedIndex]->comparator = copiedPredicates[i]->comparator;
		predicates[rearrangedIndex]->leftSide->rowId = copiedPredicates[i]->leftSide->rowId;
		predicates[rearrangedIndex]->leftSide->value = copiedPredicates[i]->leftSide->value;
		predicates[rearrangedIndex]->rightSide->rowId = copiedPredicates[i]->rightSide->rowId;
		predicates[rearrangedIndex]->rightSide->value = copiedPredicates[i]->rightSide->value;
		predicates[rearrangedIndex]->needsToBeDeleted = copiedPredicates[i]->needsToBeDeleted;
	}
    // Delete dynamically allocated arrays and return
    for (int i = 0; i < predicatesSize; i++) {
		deletePredicate(&copiedPredicates[i]);
	}
	free(copiedPredicates);
    free(predicatesFinalPosition);

    return 1;
}



// Find every time the least predicted number of row ids and keep that predicate's order, then look for the rest
int findBestPath(predicate** predicates, int predicatesSize, relationsInfo* initRelations, int num_of_initRelations, int* relations,
                        metadataCol** queryMetadata, int* predicatesFinalPosition) {
    // Copy metadata for each predicate, in order to use it if needed for future predictions
    metadataCol*** totalQueryMetadata = malloc(predicatesSize * sizeof(metadataCol**));
    int* joinCosts = malloc(predicatesSize * sizeof(int));
    for (int k = 0; k < predicatesSize; k++) {
        totalQueryMetadata[k] = malloc(num_of_initRelations * sizeof(metadataCol*));
        for (int i = 0; i < num_of_initRelations; i++) {
            totalQueryMetadata[k][i] = malloc(initRelations[i].num_of_columns * sizeof(metadataCol));
            for (int j = 0; j < initRelations[i].num_of_columns; j++) {
                totalQueryMetadata[k][i][j].min = queryMetadata[i][j].min;
                totalQueryMetadata[k][i][j].max = queryMetadata[i][j].max;
                totalQueryMetadata[k][i][j].num_of_data = queryMetadata[i][j].num_of_data;
                totalQueryMetadata[k][i][j].discrete_values = queryMetadata[i][j].discrete_values;
            }
        }
        joinCosts[k] = -1;      // Keep the costs found in each update join procedure in an array (initialized with -1)
    }
    // Calculate the number of rows estimation for each predicate not estimated yet
    for (int i = 0; i < predicatesSize; i++) {
        if (predicatesFinalPosition[i] == -1) {
            joinCosts[i] = updateJoinStatistics(predicates, initRelations, relations, totalQueryMetadata[i], i);
        }
    }
    // Find which is the lowest cost (less number of row ids after possible join) and the index of this predicate
    int minCost = -1, minIndex = -1;
    for (int i = 0; i < predicatesSize; i++) {
        if (joinCosts[i] == -1) continue;
        if (minCost == -1) {
            minCost = joinCosts[i];
            minIndex = i;
        } else {
            if (joinCosts[i] < minCost) {
                minCost = joinCosts[i];
                minIndex = i;
            }
        }
    }
    // If all predicates have been ordered, return 0 as cost
    if (minIndex != -1) {
        // Update the array with ordered predicates
        int itemsSorted = 0;
        for (int i = 0; i < predicatesSize; i++) {
            if (predicatesFinalPosition[i] != -1) itemsSorted++;
        }
        predicatesFinalPosition[minIndex] = itemsSorted;
        // Now check for the rest predicates
        minCost += findBestPath(predicates, predicatesSize, initRelations, num_of_initRelations, relations, totalQueryMetadata[minIndex], predicatesFinalPosition);
    }
    else minCost = 0;

    // Delete current allocated -to copy each predicate's metadata- queryMetadata array
    for (int k = 0; k < predicatesSize; k++) {
        for (int i = 0; i < num_of_initRelations; i++) {
            free(totalQueryMetadata[k][i]);
        }
        free(totalQueryMetadata[k]);
    }
    free(totalQueryMetadata);
    free(joinCosts);

    return minCost;
}
