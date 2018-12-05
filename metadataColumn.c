#include <stdlib.h>
#include "metadataColumn.h"

metadataCol * initializeMDCol(relation * rel) {
  metadataCol * mdCol = malloc(sizeof(metadataCol));
  if (mdCol == NULL) {
    return NULL;
  }

  //initialize each metadata value
  mdCol->num_of_rows = rel->num_tuples;
  mdCol->column = rel->tuples;

  //find min,max and discrete_values
  int i,j;
  uint32_t min, max;
  uint32_t discrete_values;

  min = rel->tuples[0];
  max = min;
  discrete_values = 1;

  for (i = 1; i < rel->num_tuples; i++) {
      //TODO: get metadata from fread, del this function
    //calculate min,max
    if (rel->tuples[i] > max) {
      max = rel->tuples[i];
    }
    else if (rel->tuples[i] < min) {
      min = rel->tuples[i];
    }

    //calculate discrete_values
    for (j = 1; j < i; j++) {
      if (rel->tuples[i] == rel->tuples[j]) {
        break;
      }
    }
    if (i == j) {
      discrete_values++;
    }
  }

  mdCol->min = min;
  mdCol->max = max;
  mdCol->discrete_values = discrete_values;

  return mdCol;
}

int getDiscreteValues(metadataCol * md) {
  return (int)md->discrete_values;
}

int getMinValue(metadataCol * md) {
  return (int)md->min;
}

int getMaxValue(metadataCol * md) {
  return (int)md->max;
}

int getNumOfRows(metadataCol * md) {
  return (int)md->num_of_rows;
}

void deleteMDCol(metadataCol ** md) {
  (*md)->min = -1;
  (*md)->max = -1;
  (*md)->num_of_rows = -1;
  (*md)->discrete_values = -1;
  (*md)->column = NULL

  free(*md);
  *md = NULL;
}
