#include <stdint.h>
#include "radixHashJoin.h"

typedef struct metadataCol {
  uint32_t num_of_rows;
  uint32_t min, max;
  uint32_t discrete_values;

  //tuple * column; //this points to an array of tuples
} metadataCol;

metadataCol * initializeMDCol(relation * rel);

int getDiscreteValues(metadataCol * md);

int getMinValue(metadataCol * md);

int getMaxValue(metadataCol * md);

int getNumOfRows(metadataCol * md);

void deleteMDCol(metadataCol * md);
