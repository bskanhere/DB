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

DBFile::DBFile () {

}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
//    cout<< "DBFile Create" << endl;
    char meta_data_file_name[100];
    sprintf(meta_data_file_name, "%s.metadata", f_path);
    ofstream meta_data_file;
    meta_data_file.open(meta_data_file_name);
    // write in file type
    if(f_type == heap) {
        meta_data_file << "heap" << endl;
        myInernalVar = new HeapDBFile();
    }
    else if(f_type == sorted) {
        meta_data_file << "sorted"<< endl;
        myInernalVar = new SortedDBFile();
    }
    else if(f_type == tree) {

    }
    meta_data_file.close();
    int res = myInernalVar->Create(f_path, f_type, startup);
    return res;
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInernalVar->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {
    char meta_data_file_name[100];
    sprintf (meta_data_file_name, "%s.metadata", f_path);
    ifstream meta_data_file(meta_data_file_name);

    string s;
    getline(meta_data_file, s);
    if(s.compare("heap") == 0) {
        myInernalVar = new HeapDBFile();
    }
    else if(s.compare("sorted") == 0) {
        myInernalVar = new SortedDBFile();
    }
    else if(s.compare("tree")==0){

    }
    meta_data_file.close();
    int res = myInernalVar->Open(f_path);
    return res;
}

void DBFile::MoveFirst () {
    myInernalVar->MoveFirst();
}

int DBFile::Close () {
    int res = myInernalVar->Close();
    delete myInernalVar;
    return res;
}

void DBFile::Add (Record &rec) {
    myInernalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return myInernalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInernalVar->GetNext(fetchme, cnf, literal);
}
