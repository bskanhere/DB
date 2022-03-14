#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

typedef enum {heap, sorted, tree} fType;
typedef struct {OrderMaker *myOrder; int runLength;} SortedInfo;
// stub DBFile header..replace it with your own DBFile.h 


class GenericDBFile{
public:
    virtual int Create (const char *fpath, fType f_type,  void *startup) = 0;
    virtual int Open (const char *fpath) = 0;
    virtual int Close () = 0;

    virtual void Load (Schema &myschema, const char *loadpath) = 0;

    virtual void MoveFirst () = 0;
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};


class DBFile {
private:
    GenericDBFile* myInernalVar;

public:
	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
