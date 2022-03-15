#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class SortedDBFile : public GenericDBFile {
    friend class DBFile;
private:
    File diskFile;
    Page bufferPage;
    off_t pageIndex;
    bool isWriteMode;
    const char* out_path = nullptr;
    int boundCalculated = 0;
    int lowerBound;
    int higherBound;

    Pipe* in = new Pipe(100);
    Pipe* out = new Pipe(100);
    pthread_t* thread = nullptr;

    OrderMaker* orderMaker = nullptr;
    int runLength;

    void addRecordsToSortedFile();
    static void *consumer (void *arg);
    int Run (Record *left, Record *literal, Comparison *c);
public:
    SortedDBFile();

    int Create (const char *fpath, fType f_type, void *startup);
    int Open (const char *fpath);
    int Close ();

    void Load (Schema &myschema, const char *loadpath);

    void MoveFirst ();
    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};


#endif
