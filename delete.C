#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

 const Status QU_Delete(const string & relation, 
  const string & attrName, 
  const Operator op,
  const Datatype type, 
  const char *attrValue)
{
std::cout << "Doing QU_Delete " << std::endl;
Status status;
HeapFileScan* hfs = new HeapFileScan(relation, status);
if(status != OK)
return status;

AttrDesc attrDesc;
RID rid;

attrCat->getInfo(relation, attrName, attrDesc);

int offset = attrDesc.attrOffset;
int length = attrDesc.attrLen;

int intValue;
float floatValue;

switch(type)
{
case STRING:
status = hfs->startScan(offset, length, type, attrValue, op);
break;

case INTEGER:
intValue = atoi(attrValue);
status = hfs->startScan(offset, length, type, (char *)&intValue, op);
break;

case FLOAT:
floatValue = atof(attrValue);
status = hfs->startScan(offset, length, type, (char *)&floatValue, op);
break;
}

if (status != OK)
{
delete hfs;
return status;
}

while((status = hfs->scanNext(rid)) == OK) 
{
if ((status = hfs->deleteRecord()) != OK)
return status;
}

hfs->endScan();
delete hfs;

return OK;
}
