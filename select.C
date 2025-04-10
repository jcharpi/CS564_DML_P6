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
#include "stdio.h"
#include "stdlib.h"

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
 *   OK on success
 *   an error code otherwise
 */
const Status QU_Select(const string &result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
  // QU_Select sets up things and then calls ScanSelect to do the work.
  cout << "Doing QU_Select " << endl;
  Status status = OK;
  AttrDesc projDesc[projCnt];          // Each element corresponds to an attribute in the output (the projection list)
  AttrDesc *attrDesc = new AttrDesc(); // Will store the detailed attribute descriptor for filtering, if needed.
  int reclen = 0;                      // Total output record length in bytes.
  const char *filter;

  // Convert basic attribute information (from projNames) into full attribute descriptors (projDesc) by retrieving metadata from the catalog.
  for (int i = 0; i < projCnt; i++)
  {
    status = attrCat->getInfo(projNames[i].relName,
                              projNames[i].attrName,
                              projDesc[i]);
    if (status != OK)
      return status;
    reclen += projDesc[i].attrLen;
  }

  // If no filtering attribute is specified (i.e., no WHERE clause), perform an unconditional scan.
  if (attr == NULL)
  {
    attrDesc = NULL;
    status = ScanSelect(result, projCnt, projDesc, attrDesc, op, NULL, reclen);
    if (status != OK)
    {
      return status;
    }
  }
  else
  {
    int intConversion;
    float floatConversion;
    // Convert the filter string to the proper type based on the attribute's type.
    switch (attr->attrType)
    {
    case INTEGER:
      intConversion = atoi(attrValue);
      filter = (char *)&intConversion;
      break;
    case FLOAT:
      floatConversion = atof(attrValue);
      filter = (char *)&floatConversion;
      break;
    case STRING:
      filter = attrValue;
      break;
    }
    // Retrieve the complete attribute descriptor for the filtering attribute.
    status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
    if (status != OK)
    {
      return status;
    }
    status = ScanSelect(result, projCnt, projDesc, attrDesc, op, filter, reclen);
    if (status != OK)
    {
      return status;
    }
  }
  return OK;
}

// This function sets up a filtered scan on the input table and performs on-the-fly projection into the output relation.
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
  cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
  
  Status status;
  string relName = projNames[0].relName;
  HeapFileScan *heapScan = new HeapFileScan(relName, status);

  if (attrDesc != NULL)
  {
    // Start a filtered scan using the provided filter value.
    if (attrDesc->attrType == INTEGER)
    {
      heapScan->startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, filter, op);
    }
    else if (attrDesc->attrType == FLOAT)
    {
      heapScan->startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, filter, op);
    }
    else
    {
      heapScan->startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, filter, op);
    }
  }
  else
  {
    // Unconditional scan
    heapScan->startScan(0, 0, STRING, NULL, EQ);
  }
  if (status != OK)
  {
    return status;
  }

  // Open the result table where the output tuples will be inserted.
  InsertFileScan outTable(result, status);
  if (status != OK)
  {
    return status;
  }

  // Process each tuple returned by the scan.
  RID rid;
  while (heapScan->scanNext(rid) == OK)
  {
    // Retrieve the current tuple from the input table.
    Record rec;
    status = heapScan->HeapFile::getRecord(rid, rec);
    ASSERT(status == OK);

    // Allocate a buffer for the output tuple.
    char *data = new char[reclen];
    Record tempRecord = {data, reclen};
    int offset = 0;

    // Copy each projected attribute's data from the input tuple to the output buffer.
    for (int i = 0; i < projCnt; i++)
    {
      // point to the next free byte in the output buffer where the current attribute should be copied.
      char *destPtr = data + offset;
      // point to the location in the input record where the attribute's value begins.
      char *srcPtr = ((char *)rec.data) + projNames[i].attrOffset;
      int len = projNames[i].attrLen;
      memcpy(destPtr, srcPtr, len);
      offset += len;
    }

    // Insert the newly constructed output tuple into the result relation.
    RID tempRID;
    status = outTable.insertRecord(tempRecord, tempRID);
    if (status != OK)
    {
      return status;
    }
  }
  return status;
}
