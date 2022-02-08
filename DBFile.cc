#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <iostream>


DBFile::DBFile () {
    file = new File();
    writePage = new Page();
    readPageNumber = 0;
    readPage = new Page();
    isWriteMode = true;
    comparisonEngine = new ComparisonEngine();
}

DBFile::~DBFile() {
    delete(file);
    delete(readPage);
    delete(writePage);
    delete(comparisonEngine);
}

int DBFile::Create ( const char *fPath, fType f_type, void *startup) {
    if (fPath == NULL || fPath[0] == '\0' || f_type != heap) {
        return 0;
    }
    file->Open(0, const_cast<char *>(fPath));
    return 1;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    Record *temp = new Record();
    while(temp->SuckNextRecord(&f_schema, tableFile)) {
        Add(*(temp));
    }
    delete temp;
    fclose(tableFile);
}


int DBFile::Open (const char *fPath) {
     if (fPath == NULL || fPath[0] == '\0') {
        return 0;
    }
    file->Open(1, const_cast<char *>(fPath));
    readPageNumber = 0;
    return 1;
}

void DBFile::MoveFirst () {
    readPageNumber = 0;
    file->GetPage(readPage, 0);
}

int DBFile::Close () {
    int writePageNumber = (file->GetLength() <= 0) ? 0 : file->GetLength();
    file->AddPage(writePage, writePageNumber);
    return file->Close();
}

void DBFile::Add (Record &rec) {
    int writePageNumber = (file->GetLength() <= 0) ? 0 : file->GetLength();
    if(!isWriteMode) {
        writePage->EmptyItOut();
        isWriteMode = true;
    }

    if (writePage->Append(&rec)) return;
    file->AddPage(writePage, writePageNumber++);
    writePage->EmptyItOut();
    writePage->Append(&rec);
}

int DBFile::GetNext (Record &fetchme) {
    if(isWriteMode) {
        int writePageNumber = (file->GetLength() <= 0) ? 0 : file->GetLength();
        file->AddPage(writePage, writePageNumber++);
        isWriteMode = false;
        if(readPageNumber == 0) {
            if(file->GetLength() <= 0)
                return 0;
            readPage->EmptyItOut();
            file->GetPage(readPage, readPageNumber++);
        }
    }
    if(readPage->GetFirst(&fetchme))
        return 1;
    if(readPageNumber < 0 || readPageNumber > file->GetLength()-2)
        return 0;
    readPage->EmptyItOut();
    file->GetPage(readPage, readPageNumber++);
    return GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    while (true) {
        int result = GetNext(fetchme);
        if(!result) 
            return 0;
        result = comparisonEngine->Compare(&fetchme, &literal, &cnf);
        if(result) 
            break;
    }
    return 1;
}

