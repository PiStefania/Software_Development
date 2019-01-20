#ifndef _STATISTICS_METHODS_H_
#define _STATISTICS_METHODS_H_

#include "relationMethods.h"
#include "queryMethods.h"



int checkCompareStatistics(predicate** predicates, metadataCol** queryMetadata, metadataCol* oldMetadata, int currentPredicate, int relationId, int relColumn);
uint32_t updateCompareStatistics(predicate** predicates, relationsInfo* initRelations, metadataCol** queryMetadata, metadataCol* oldMetadata,
                                    int currentPred, int relationId, int relColumn, char foundVal);
uint32_t updateJoinStatistics(predicate** predicates, relationsInfo* initRelations, int* relations, metadataCol** queryMetadata, int currentPredicate);
void copyMetadata(relationsInfo* initRelations, int num_of_initRelations, int* relations, int relationsSize, metadataCol** queryMetadata);

int joinEnumeration(predicate** predicates, int predicatesSize, relationsInfo* initRelations, int num_of_initRelations, int* relations, metadataCol** queryMetadata);
int findBestPath(predicate** predicates, int predicatesSize, relationsInfo* initRelations, int num_of_initRelations, int* relations,
                        metadataCol** queryMetadata, int* predicatesFinalPosition);


#endif
