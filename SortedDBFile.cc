#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>
#include <iostream>
#include <fstream>


SortedDBFile::SortedDBFile() {
    isWriteMode = false;
    pageIndex = 0;
    boundSet = false;
}

SortedDBFile::~SortedDBFile() {

}

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {
    char meta_data_file_name[100];      //Metadata file - adding runlength and sorting order to metadata file
    sprintf(meta_data_file_name, "%s.metadata", f_path);
    ofstream meta_data_file;
    meta_data_file.open(meta_data_file_name, std::ios_base::app);
    OrderMaker* orderMaker = nullptr;
    int runLength = 0;
    if(startup != nullptr) {
        SortedInfo* sortedInfo = ((SortedInfo*)startup);
        orderMaker = sortedInfo->myOrder;
        runLength = sortedInfo->runLength;
        meta_data_file << runLength << endl;
        meta_data_file << orderMaker->numAtts << endl;
        for(int i = 0; i < orderMaker->numAtts; i++) {
            meta_data_file << orderMaker->whichAtts[i] << endl;
            if(orderMaker->whichTypes[i] == Int)
                meta_data_file << "Int" << endl;
            else if(orderMaker->whichTypes[i] == Double)
                meta_data_file << "Double" << endl;
            else if(orderMaker->whichTypes[i] == String)
                meta_data_file << "String" << endl;
        }
        this->orderMaker = orderMaker;
        this->runLength = runLength;
    }
    meta_data_file.close();

    file.Open(0, const_cast<char *>(f_path));
    path = f_path;
    pageIndex = 0;
    isWriteMode = false;
    MoveFirst();
    return 1;
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while(temp.SuckNextRecord(&f_schema, tableFile) == 1) {
        this->Add(temp);        //Calling Add to add record to Input Pipe
    }

    fclose(tableFile);
}

int SortedDBFile::Open(const char *f_path) {
    OrderMaker* orderMaker = new OrderMaker();
    char meta_data_file_name[100];
    sprintf(meta_data_file_name, "%s.metadata", f_path);
    ifstream meta_data_file(meta_data_file_name);
    string temp;
    getline(meta_data_file, temp);
    temp.clear();
    getline(meta_data_file, temp);
    int runLength = stoi(temp);
    temp.clear();
    getline(meta_data_file, temp);
    orderMaker->numAtts = stoi(temp);
    for(int i=0; i<orderMaker->numAtts; i++) {
        temp.clear();
        getline(meta_data_file, temp);
        orderMaker->whichAtts[i] = stoi(temp);
        temp.clear();
        getline(meta_data_file, temp);
        if(temp.compare("Int") == 0) {
            orderMaker->whichTypes[i] = Int;
        }
        else if(temp.compare("Double") == 0) {
            orderMaker->whichTypes[i] = Double;
        }
        else if(temp.compare("String") == 0) {
            orderMaker->whichTypes[i] = String;
        }
    }
    this->orderMaker = orderMaker;
    this->runLength = runLength;
    meta_data_file.close();

    file.Open(1, const_cast<char *>(f_path));
    pageIndex = 0;
    path = f_path;
    isWriteMode = false;
    MoveFirst();
    return 1;
}

void SortedDBFile::MoveFirst() {
    addRecordsToSortedFile();
    pageIndex = 0;
    bufferPage.EmptyItOut();
    if (file.GetLength() > 0) {
        file.GetPage(&bufferPage, pageIndex);
    }
}

int SortedDBFile::Close() {
    addRecordsToSortedFile();
    bufferPage.EmptyItOut();
    file.Close();
    if(in != nullptr)
        delete in;
    if(out != nullptr)
        delete out;
    return 1;
}

void SortedDBFile::Add(Record &rec) {
    boundSet = false;
    if(!isWriteMode) {
        isWriteMode = true;
        WorkerThreadArgs *workerThreadArgs = new WorkerThreadArgs;      //Creating In and Out Pipe of BigQ to add Records to sort
        workerThreadArgs->in = in;
        workerThreadArgs->out = out;
        workerThreadArgs->order = orderMaker;
        workerThreadArgs->runlen = runLength;
        thread = new pthread_t();
        pthread_create(thread, NULL, TPMMSAlgo, (void *) workerThreadArgs);
    }
    in->Insert(&rec);       //Adding Records to Input Pipe
}

int SortedDBFile::GetNext(Record &fetchme) {
    addRecordsToSortedFile();
    if(bufferPage.GetFirst(&fetchme) == 0) {
        pageIndex++;
        if(pageIndex >= file.GetLength() - 1) {
            return 0;
        }
        bufferPage.EmptyItOut();
        file.GetPage(&bufferPage, pageIndex);
        bufferPage.GetFirst(&fetchme);
    }
    return 1;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if(!boundSet) {
        boundSet = true;
        set<int> set;
        for (int i = 0; i < orderMaker->numAtts; i++) {
            set.insert(orderMaker->whichAtts[i]);
        }
        int lowerLimit = 0, upperLimit = file.GetLength() - 2;
        for(int i = 0; i < cnf.numAnds; i++) {
            for(int j = 0; j < cnf.orLens[i]; j++) {
                if (set.find(cnf.orList[i][j].whichAtt1) == set.end()) continue; //Attribute Not part of Sort Ordering
                if (cnf.orList[i][j].op == LessThan) {     //Updating the Upper Limit
                    int left = 0, right = file.GetLength() - 2;
                    Record rec;
                    while (left < right) {
                        int mid = (right + left + 1) / 2;
                        file.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = compRec(&rec, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            left = mid;
                        } else {
                            right = mid - 1;
                        }
                    }
                    upperLimit = min(upperLimit, right);
                } else if (cnf.orList[i][j].op == GreaterThan) {     //Updating the Lower Limit
                    int left = 0, right = file.GetLength() - 2;
                    Record rec;
                    while (left < right) {
                        int mid = (right + left) / 2;
                        file.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = compRec(&rec, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            right = mid;
                        } else {
                            left = mid + 1;
                        }
                    }
                    lowerLimit = max(lowerLimit, left);
                }
            }
        }
        lowerLimit = lowerLimit - 1;
        lowerLimit = max(0, lowerLimit);
        lowerBound = lowerLimit;
        higherBound = upperLimit;
        pageIndex = lowerLimit;
    }

    ComparisonEngine comp;

    while (GetNext(fetchme) == 1) {
        if (pageIndex > higherBound) return 0;
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

int SortedDBFile :: compRec(Record *left, Record *literal, Comparison *c) {

    char *valLeft, *valRight;

    char *left_bits = left->GetBits();
    char *lit_bits = literal->GetBits();

    if(c->operand1 == Left) {
        valLeft = left_bits + ((int *) left_bits)[c->whichAtt1 + 1];
    } else {
        valLeft = lit_bits + ((int *) lit_bits)[c->whichAtt1 + 1];
    }

    if(c->operand2 == Left) {
        valRight = left_bits + ((int *) left_bits)[c->whichAtt2 + 1];
    } else {
        valRight = lit_bits + ((int *) lit_bits)[c->whichAtt2 + 1];
    }


    int valLeftInt, valRightInt, compareResult;
    double valLeftDouble, valRightDouble;
    if(c->attType == Int) {     //Checking type and Comparison Operation
        valLeftInt = *((int *) valLeft);
        valRightInt = *((int *) valRight);
        if(c->op == LessThan) {
            return (valLeftInt < valRightInt);
        } else if (c->op == GreaterThan) {
            return (valLeftInt > valRightInt);
        } else {
            return (valLeftInt == valRightInt);
            }
    } else if(c->attType == Double) {
        valLeftDouble = *((double *) valLeft);
        valRightDouble = *((double *) valRight);
        if(c->op == LessThan) {
            return (valLeftDouble < valRightDouble);
        } else if (c->op == GreaterThan) {
            return (valLeftDouble > valRightDouble);
        } else {
            return (valLeftDouble == valRightDouble);
        }
    } else {
        compareResult = strcmp (valLeft, valRight);
        if(c->op == LessThan) {
            return compareResult < 0;
        } else if (c->op == GreaterThan) {
            return compareResult > 0;
        } else {
            return compareResult == 0;
        }
    }
}


void SortedDBFile::addRecordsToSortedFile() {       //Method to add records in BigQ while changing to Read Mode
    if(isWriteMode){
        boundSet = false;
        isWriteMode = false;

        in->ShutDown();     //Shutting Down input for BigQ to sort Records and push in Out Pipe

        DBFile mergedFile;      //Temporary File to Merge Records from Current File and Out Pipe
        mergedFile.Create("mergedFile.bin", heap, nullptr);

        this->MoveFirst();
        Record left;
        Record right;
        ComparisonEngine comparisonEngine;
        int successLeft = out->Remove(&left);       //2-Way Merge Algorithm
        int successRight = this->GetNext(right);
        while(successLeft && successRight){
            if(comparisonEngine.Compare(&left, &right, orderMaker) < 0) {
                mergedFile.Add(left);
                successLeft = out->Remove(&left);
            } else {
                mergedFile.Add(right);
                successRight = this->GetNext(right);
            }
        }
        while(successLeft) {
            mergedFile.Add(left);
            successLeft = out->Remove(&left);
        }
        while(successRight) {
            mergedFile.Add(right);
            successRight = this->GetNext(right);
        }
        if(thread!= nullptr) {      //Deleting BigQ Worker Thread
            pthread_join (*thread, NULL);
            delete thread;
        }
        mergedFile.Close();
        file.Close();
        remove(path);
        rename("mergedFile.bin", path);     //Renaming Temporary File to Current File
    }
}