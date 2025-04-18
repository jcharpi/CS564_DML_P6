// An insertion is implemented using the helper InsertFileScan. The tuple is 
// constructed from the attrList. Each value is placed at the appropriate
// offset. Once the record is fully constructed, it is inserted into the 
// relation using InsertFileScan::insertRecord().
//
//
// This implementation retrieves the schema, builds the record buffer,
// inserts the tuple into the heap file, and frees temporary memory.


#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	Status status = OK;
	int relAttrCnt;
	AttrDesc *relAttrs;
	status = attrCat->getRelInfo(relation, relAttrCnt, relAttrs);
	if(status != OK) 
	{
		return status;
	}
	
	//Calculating total record length
	int recL = 0;
	for (int i=0; i<relAttrCnt; i++)
	{
		recL += relAttrs[i].attrLen;
	}

	//Allocated buffer space for new record
	char *recData = new char[recL];

	//Populating buffer with values by matching attributes to attrList
	for (int i=0; i<relAttrCnt; i++)
	{
		bool found = false;
		for(int j = 0; j < attrCnt; j++) 
		{
			
            if (strcmp(relAttrs[i].attrName, attrList[j].attrName) == 0) {
                if (attrList[j].attrValue == nullptr) {
                    delete[] recData;
                    delete[] relAttrs;
                    return status;
                }
				// Convert and copy the value into the record buffer, similar
				// to how it is done in Select.c
				int intConversion;
				float floatConversion;
				switch (relAttrs[i].attrType) {
					case INTEGER:
						intConversion = atoi((char *)attrList[j].attrValue);
						memcpy(recData + relAttrs[i].attrOffset, &intConversion, sizeof(int));
						break;
					case FLOAT:
						floatConversion = atof((char *)attrList[j].attrValue);
						memcpy(recData + relAttrs[i].attrOffset, &floatConversion, sizeof(float));
						break;
					case STRING:
						memcpy(recData + relAttrs[i].attrOffset, attrList[j].attrValue, relAttrs[i].attrLen);
						break;
					default:
						delete[] recData;
						delete[] relAttrs;
						return BADCATPARM;
				}
                found = true;
                break;
            }
        }
	// If value missing in input, reject
        if (!found) {
            delete[] recData;
            delete[] relAttrs;
            return status;
        }
    }

	// This functions helps insert records into a heapfile. Provides access to insertRecord.
	InsertFileScan insertScan(relation, status);
	if (status != OK) {
		delete[] recData;
		delete[] relAttrs;
		return status;
	}

	// Creating record to insert
	RID rid;
	Record rec = { recData, recL };
	status = insertScan.insertRecord(rec, rid);

	// Clean up previously allocated mem
	delete[] recData;
	delete[] relAttrs;

	return status;
}


