// select.C
//
// This file implements the selection operator for Minirel.
//
// A selection is implemented using a filtered HeapFileScan. The result of the selection is stored in the result relation called
// result (a heapfile with this name will be created by the parser before QU_Select() is called). The project list is defined by the parameters projCnt
// and projNames. Projection should be performed on the fly as each result tuple is being appended to the result table.
//
// The search value is always supplied as the character string attrValue. You should convert it to the proper type based on the type of attr.
// You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.
//
// If attr is NULL, an unconditional scan of the input table should be performed.
//
// This implementation sets up a filtered scan for a selection predicate and
// performs on‐the‐fly projection via a helper function, ScanSelect().
#include "catalog.h"
#include "query.h"

const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

// return rows that correspond to given filter
// result: name of relation to store output in
// projCnt: number of attributes to project
// projNames: array of attrInfo structs describing attributes to project
// attr: attribute used for filtering (if NULL, perform an unconditional scan)
// op: operator defining the filtering condition
// attrValue: search value supplied as character string
const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
  // Qu_Select sets up things and then calls ScanSelect to do the actual work
  cout << "Doing QU_Select " << endl;
  Status status;

  // Create an array of AttrDesc structures, one for each attribute in the projection list.
  // The projection list (projNames) is provided as an array of attrInfo structures that
  // have basic attribute details (such as relation and attribute names). Here we convert
  // these into complete attribute descriptions by retrieving additional information (offset,
  // type, length) from the catalog.
  AttrDesc projDesc[projCnt]; // each element corresponds to an attribute in the output
  for (int i = 0; i < projCnt; i++)
  {
    status = attrCat->getInfo(projNames[i].relName,
                              projNames[i].attrName,
                              projDesc[i]);
    if (status != OK)
      return status;
  }

  // If a filtering condition is specified (indicated by attrValue being non-NULL),
  // then we need to retrieve the detailed attribute description for the filtering attribute.
  // This descriptor (attrDesc) contains metadata such as the offset within a record, type,
  // and length, all of which are required for performing the filtered scan later.
  AttrDesc attrDesc; // corresponds to a single element used for filtering
  if (attr != NULL)
  {
    status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
    if (status != OK)
      return status;
  }

  // Calculate the length of the output record by summing the lengths of the projected attributes.
  int outputRecordLength = 0; // the total number of bytes required for each output tuple.
  for (int i = 0; i < projCnt; i++)
  {
    outputRecordLength += projDesc[i].attrLen;
  }

  // If no filtering attribute is specified, perform an unconditional scan. i.e. no WHERE clause provided by the user and we should process every tuple.
  if (attr == NULL)
  {
    return ScanSelect(result, projCnt, projDesc, &projDesc[0], EQ, NULL, outputRecordLength);
  }
  // If a filtering attribute is provided, then call ScanSelect with the detailed filtering information obtained in attrDesc,
  // along with the appropriate operator (op) and the provided filter value (attrValue).
  return ScanSelect(result, projCnt, projDesc, &attrDesc, op, attrValue, outputRecordLength);
}

// This function sets up a filtered scan on the input table and performs on-the-fly projection into the output relation.
const Status ScanSelect(const string &result,
#include "stdio.h"
#include "stdlib.h"
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
  cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
  Status status;
  int outputTupleCount = 0; // number of tuples inserted into the output relation

  // Result relation is a heapfile which has already been created by the parser.
  // We use InsertFileScan to insert the projected tuples into this result relation.
  InsertFileScan resultRel(result, status);
  if (status != OK)
    return status;

  // The table to scan is identified by the relName field in the attribute descriptor.
  // HeapFileScan will allow us to iterate over tuples in the table.
  HeapFileScan scan(string(attrDesc->relName), status);
  if (status != OK)
    return status;

  // Start the scan on the input table:
  // 1. If filter is NULL, perform an unconditional scan.
  if (filter == NULL)
  {
    status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, NULL, EQ);
  }

  // 2. If filter is provided, perform a filtered scan using the supplied value.
  // Determine attribute's data type and convert the filter string accordingly.
  else
  {
    Datatype type;
    void *updatedTypeFilter;
    if (attrDesc->attrType == STRING)
    {
      type = STRING;
      updatedTypeFilter = (void *)filter; // No conversion needed for strings
    }
    else if (attrDesc->attrType == INTEGER)
    {
      type = INTEGER;
      int tempInt = atoi(filter); // Convert the filter to an integer
      updatedTypeFilter = &tempInt;
    }
    else
    {
      type = FLOAT;
      float tempFloat = atof(filter); // Convert the filter to an float
      updatedTypeFilter = &tempFloat;
    }
    status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, type, (char *)updatedTypeFilter, op);
    if (status != OK)
      return status;
  }

  // Prep a temporary buffer to build each output tuple. The output tuple will consist of only the projected attributes and must
  // be exactly reclen bytes long.
  char outputData[reclen];
  Record outputRec;
  outputRec.data = (void *)outputData;
  outputRec.length = reclen;

  // Process each tuple returned by the scan:
  // For each matching tuple, copy out each projected attribute (using its offset and length)
  // into the output buffer, and insert the newly formed tuple into the result relation.
  RID rid;
  while (scan.scanNext(rid) == OK)
  {
    Record rec;
    status = scan.getRecord(rec);
    ASSERT(status == OK);

    int outputOffset = 0;
    // For a match, copy data into output record
    for (int i = 0; i < projCnt; i++)
    {
      memcpy(outputData + outputOffset, (char *)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
      outputOffset += projNames[i].attrLen;
    }

    // Insert the constructed record into the result relation.
    RID outRID;
    status = resultRel.insertRecord(outputRec, outRID);
    ASSERT(status == OK);
    outputTupleCount++;
  }
  return OK;
}
