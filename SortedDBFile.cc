#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <chrono>
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>


SortedDBFile::SortedDBFile() {
    isWriteMode = false;
    pageIndex = 0;
}

int SortedDBFile::Create (const char *f_path, fType f_type, void *startup) {
    cout<<"begin c Create"<<endl;
    diskFile.Open(0, const_cast<char *>(f_path));
    out_path = f_path;
    pageIndex = 0;
    isWriteMode = false;
    MoveFirst();
    cout<<"end Create" << endl;
    return 1;
}

void SortedDBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen (loadpath, "r");
    Record temp;
    ComparisonEngine comp;

    while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
        this->Add(temp);
    }
    fclose(tableFile);
}

int SortedDBFile::Open (const char *f_path) {
    diskFile.Open(1, const_cast<char *>(f_path));
    pageIndex = 0;
    out_path = f_path;
    isWriteMode = false;
    MoveFirst();
    return 1;
}

void SortedDBFile::MoveFirst () {
    addRecordsToSortedFile();
    pageIndex = 0;
    bufferPage.EmptyItOut();
    if (diskFile.GetLength() > 0) {
        diskFile.GetPage(&bufferPage, pageIndex);
    }
}

int SortedDBFile::Close () {
    addRecordsToSortedFile();
    bufferPage.EmptyItOut();
    diskFile.Close();
    if(in != nullptr)
        delete in;
    if(out != nullptr)
        delete out;
    return 1;
}

void SortedDBFile::Add (Record &rec) {
    boundCalculated = 0;
    if(!isWriteMode) {
        cout << "Change from Read to Write Mode " << endl;
        isWriteMode = true;
        //Construct arguement used for worker thread
        WorkerThreadArgs *workerThreadArgs = new WorkerThreadArgs;
        workerThreadArgs->in = in;
        workerThreadArgs->out = out;
        workerThreadArgs->order = orderMaker;
        workerThreadArgs->runlen = runLength;
        thread = new pthread_t();
        pthread_create(thread, NULL, TPMMSAlgo, (void *) workerThreadArgs);
        cout << "In Write Mode" << endl;
    }
    in->Insert(&rec);
}

int SortedDBFile::GetNext (Record &fetchme) {
    //If file is writing, then write page into disk based file, redirect page ot first page
    addRecordsToSortedFile();
    //If reach the end of page
//    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    if (bufferPage.GetFirst(&fetchme) == 0) {
//        cout<<diskFile.GetLength()<<endl;
        pageIndex++;
        //If reach the end of file
        if (pageIndex >= diskFile.GetLength() - 1) {
            return 0;
        }
        //Else get next page
        bufferPage.EmptyItOut();
        diskFile.GetPage(&bufferPage, pageIndex);
        bufferPage.GetFirst(&fetchme);
    }
    return 1;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if(!boundCalculated) {
        boundCalculated = 1;
        set<int> attSet;
        for (int i = 0; i < orderMaker->numAtts; i++) {
            attSet.insert(orderMaker->whichAtts[i]);
        }
        int global_lower = 0, global_higher = diskFile.GetLength() - 2;
        cout << "original lower bound: " << global_lower << endl;
        cout << "original higher bound: " << global_higher << endl;
        for (int i = 0; i < cnf.numAnds; i++) {
            for (int j = 0; j < cnf.orLens[i]; j++) {
                if (attSet.find(cnf.orList[i][j].whichAtt1) == attSet.end()) continue;
                //calculate the lower bound and upper bound by Binary Search
                // calculate the upper bound for a LessThan
                if (cnf.orList[i][j].op == LessThan) {
                    int left = 0, right = diskFile.GetLength() - 2;
                    Record rec;
                    while (left < right) {
                        int mid = left + (right - left + 1) / 2;
                        diskFile.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = Run(&rec, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            left = mid;
                        } else {
                            right = mid - 1;
                        }
                    }
                    //update the global lower and upper bound
                    global_higher = min(global_higher, right);
                }
                    // calculate the lower bound for a UpperThan
                else if (cnf.orList[i][j].op == GreaterThan) {
                    int left = 0, right = diskFile.GetLength() - 2;
                    Record rec;
                    while (left < right) {
                        int mid = left + (right - left) / 2;
                        diskFile.GetPage(&bufferPage, mid);
                        bufferPage.GetFirst(&rec);
                        int result = Run(&rec, &literal, &cnf.orList[i][j]);
                        if (result != 0) {
                            right = mid;
                        } else {
                            left = mid + 1;
                        }
                    }
                    //update the global lower and upper bound
                    global_lower = max(global_lower, left);
                }
            }
        }
        global_lower = global_lower - 1; // left shift for a page
        global_lower = max(0, global_lower);

        cout << "updated lower bound by Binary Search: " << global_lower << endl;
        cout << "updated higher bound by Binary Search: " << global_higher << endl;
        lowerBound = global_lower;
        higherBound = global_higher;
        pageIndex = global_lower;
    }

    ComparisonEngine comp;
    //If not reach the end of file

    while (GetNext(fetchme) == 1) {
        if (pageIndex > higherBound) return 0;
        if (comp.Compare(&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

void SortedDBFile::addRecordsToSortedFile(){
    if(isWriteMode){
        boundCalculated = 0;
        isWriteMode = false;
        in->ShutDown();

        DBFile mergedFile;
        mergedFile.Create("mergedFile.bin", heap, nullptr);

        this->MoveFirst();
        Record left;
        Record right;
        ComparisonEngine comparisonEngine;
        int successLeft = out->Remove(&left);
        int successRight = this->GetNext(right);
        while(successLeft && successRight){
            if (comparisonEngine.Compare(&left, &right, orderMaker) < 0){
                cout << "Removded From Pipe" << endl;
                mergedFile.Add(left);
                successLeft = out->Remove(&left);
            }
            else{
                mergedFile.Add(right);
                successRight = this->GetNext(right);
            }
        }
        while(successLeft){
            cout << "Removded From Pipe" << endl;
            mergedFile.Add(left);
            successLeft = out->Remove(&left);
        }
        while(successRight){
            mergedFile.Add(right);
            successRight = this->GetNext(right);
        }
        if(thread!= nullptr) {
            pthread_join (*thread, NULL);
            delete thread;
        }
        mergedFile.Close();
        diskFile.Close();
        remove(out_path);
        rename("mergedFile.bin", out_path);
    }
}

//void* DBFileSorted::consumer(void *arg2) {
//    cout<< "begin Consumer Thread" << endl;
//    ThreadArg* arg = (ThreadArg *)arg2;
//    char bigQPath[100];
//    sprintf(bigQPath, "%s.bigq", arg->out_path);
////    cout<< "=====bigQPath" << bigQPath << endl;
//    DBFile dbFileHeap;
//    cout<< "1" << endl;
//    dbFileHeap.Create(bigQPath, heap, nullptr);
//    cout<< "2" << endl;
//    Record rec;
//    while(arg->out->Remove(&rec)){
//        cout<< "5" << endl;
//        dbFileHeap.Add(rec);
//    }
//    cout<< "3" << endl;
//    dbFileHeap.Close();
//    cout<< "4" << endl;
//    cout<< "end Consumer Thread" << endl;
//}

int SortedDBFile :: Run (Record *left, Record *literal, Comparison *c) {

    char *val1, *val2;

    char *left_bits = left->GetBits();
    char *lit_bits = literal->GetBits();

    // first get a pointer to the first value to compare
    if (c->operand1 == Left) {
        val1 = left_bits + ((int *) left_bits)[c->whichAtt1 + 1];
    } else {
        val1 = lit_bits + ((int *) lit_bits)[c->whichAtt1 + 1];
    }

    // next get a pointer to the second value to compare
    if (c->operand2 == Left) {
        val2 = left_bits + ((int *) left_bits)[c->whichAtt2 + 1];
    } else {
        val2 = lit_bits + ((int *) lit_bits)[c->whichAtt2 + 1];
    }


    int val1Int, val2Int, tempResult;
    double val1Double, val2Double;

    // now check the type and the comparison operation
    switch (c->attType) {

        // first case: we are dealing with integers
        case Int:

            val1Int = *((int *) val1);
            val2Int = *((int *) val2);

            // and check the operation type in order to actually do the comparison
            switch (c->op) {

                case LessThan:
                    return (val1Int < val2Int);
                    break;

                case GreaterThan:
                    return (val1Int > val2Int);
                    break;

                default:
                    return (val1Int == val2Int);
                    break;
            }
            break;

            // second case: dealing with doubles
        case Double:
            val1Double = *((double *) val1);
            val2Double = *((double *) val2);

            // and check the operation type in order to actually do the comparison
            switch (c->op) {

                case LessThan:
                    return (val1Double < val2Double);
                    break;

                case GreaterThan:
                    return (val1Double > val2Double);
                    break;

                default:
                    return (val1Double == val2Double);
                    break;
            }
            break;

            // final case: dealing with strings
        default:

            // so check the operation type in order to actually do the comparison
            tempResult = strcmp (val1, val2);
            switch (c->op) {

                case LessThan:
                    return tempResult < 0;
                    break;

                case GreaterThan:
                    return tempResult > 0;
                    break;

                default:
                    return tempResult == 0;
                    break;
            }
            break;
    }

}