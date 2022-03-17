#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapDBFile.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>

DBFile::DBFile() {

}

DBFile::~DBFile() {

}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    char meta_data_file_name[100];      //Metadata file to store the Type of File
    sprintf(meta_data_file_name, "%s.metadata", f_path);
    ofstream meta_data_file;
    meta_data_file.open(meta_data_file_name);
    if(f_type == heap) {
        meta_data_file << "heap" << endl;
        myInternalVar = new HeapDBFile();
    }
    else if(f_type == sorted) {
        meta_data_file << "sorted"<< endl;
        myInternalVar = new SortedDBFile();
    }
    else if(f_type == tree) {

    }
    meta_data_file.close();
    int res = myInternalVar->Create(f_path, f_type, startup);
    return res;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) {
    char meta_data_file_name[100];      //Using Metadata to get Type to Open respective DBFile
    sprintf(meta_data_file_name, "%s.metadata", f_path);
    ifstream meta_data_file(meta_data_file_name);

    string s;
    getline(meta_data_file, s);
    if(s.compare("heap") == 0) {
        myInternalVar = new HeapDBFile();
    }
    else if(s.compare("sorted") == 0) {
        myInternalVar = new SortedDBFile();
    }
    else if(s.compare("tree")==0){

    }
    meta_data_file.close();
    int res = myInternalVar->Open(f_path);
    return res;
}

void DBFile::MoveFirst() {
    myInternalVar->MoveFirst();
}

int DBFile::Close() {
    int res = myInternalVar->Close();
    delete myInternalVar;
    return res;
}

void DBFile::Add(Record &rec) {
    myInternalVar->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}
