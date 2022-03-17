#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "DBFile.h"

class HeapDBFile : public GenericDBFile {
	File *file;
	Page *writePage, *readPage;
	bool isWriteMode;
	off_t readPageNumber;
	ComparisonEngine *comparisonEngine;

public:
	HeapDBFile (); 
	~HeapDBFile();

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
