#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H
#include "DBFile.h"
#include <queue>
#include "BigQ.h"


class SortedDBFile : public GenericDBFile {
    friend class DBFile;
private:
    File file;
    Page bufferPage;
    off_t pageIndex;
    bool isWriteMode;
    const char* path = nullptr;
    bool boundSet;
    int lowerBound;
    int higherBound;

    Pipe* in = new Pipe(100);
    Pipe* out = new Pipe(100);
    pthread_t* thread = nullptr;
    OrderMaker* orderMaker = nullptr;
    int runLength;

    void addRecordsToSortedFile();
    int compRec(Record *left, Record *literal, Comparison *c);
public:
    SortedDBFile();
    ~SortedDBFile();
    int Create(const char *fpath, fType f_type, void *startup);
    int Open(const char *fpath);
    int Close();

    void Load(Schema &myschema, const char *loadpath);

    void MoveFirst();
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};
#endif
