#include "RelOp.h"
#include <iostream>
#include "BigQ.h"


//SelectPipe
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} SelectPipeWorkerArg;

void* selectPipeWorker(void* arg) {
    ComparisonEngine comparisonEngine;
    SelectPipeWorkerArg* workerArg = (SelectPipeWorkerArg*) arg;
    Record rec;
    while(workerArg->inPipe.Remove(&rec)) {
        if(comparisonEngine.Compare(&rec, &workerArg->literal, &workerArg->selOp)) {
            workerArg->outPipe.Insert(&rec);
        }
    }
    workerArg->outPipe.ShutDown();
    return NULL;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectPipeWorkerArg* workerArg = new SelectPipeWorkerArg{inPipe, outPipe, selOp, literal};
    pthread_create(&workerThread, NULL, selectPipeWorker, (void*) workerArg);
}

void SelectPipe::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {

}


//SelectFile
typedef struct {
    DBFile &inFile;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} SelectFileWorkerArg;

void* selectFileWorker(void* arg) {
    SelectFileWorkerArg* workerArg = (SelectFileWorkerArg*) arg;
    Record rec;
    while(workerArg->inFile.GetNext(rec, workerArg->selOp, workerArg->literal)) {
        workerArg->outPipe.Insert(&rec);
    }
    workerArg->outPipe.ShutDown();
    return NULL;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SelectFileWorkerArg* workerArg = new SelectFileWorkerArg{inFile, outPipe, selOp, literal};
    pthread_create(&workerThread, NULL, selectFileWorker, (void*) workerArg);
}

void SelectFile::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {

}


//Project
typedef struct {
	Pipe* inPipe;
	Pipe* outPipe;
	int* keepMe;
	int numAttsInput;
	int numAttsOutput;
} ProjectWorkerArg;

void* projectWorker (void* arg) {
    ProjectWorkerArg* workerArg = (ProjectWorkerArg*) arg;
    Record rec;
    while (workerArg->inPipe->Remove(&rec) == 1) {
        Record* temp = new Record;
        temp->Consume(&rec);
        temp->Project(workerArg->keepMe, workerArg->numAttsOutput, workerArg->numAttsInput);
        workerArg->outPipe->Insert(temp);     
    }
    workerArg->outPipe->ShutDown();
    return NULL;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectWorkerArg* workerArg = new ProjectWorkerArg;
    workerArg->inPipe = &inPipe;
    workerArg->outPipe = &outPipe;
    workerArg->keepMe = keepMe;
    workerArg->numAttsInput = numAttsInput;
    workerArg->numAttsOutput = numAttsOutput;

    pthread_create(&workerThread, NULL, projectWorker, (void*) workerArg);
}

void Project::WaitUntilDone() {
    pthread_join(workerThread, NULL);
}

void Project::Use_n_Pages(int n) {

}


//Sum
typedef struct {
    Pipe* inputPipe;
    Pipe* outputPipe;
    Function* computeMe;
} SumWorkerArg;

void* sumWorker(void* arg) {
    SumWorkerArg* workerArg = (SumWorkerArg*) arg;
    int intVal = 0;
    double doubleVal = 0;
    double sum = 0;
    Record temp;
    while (workerArg->inputPipe->Remove(&temp)) {
        intVal = 0;
        doubleVal = 0;
        workerArg->computeMe->Apply(temp, intVal, doubleVal);
        sum += (intVal + doubleVal);
    }
    temp.ComposeRecord(&sumSchema, (std::to_string(sum) + "|").c_str());
    workerArg->outputPipe->Insert(&temp);
    workerArg->outputPipe->ShutDown();
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    SumWorkerArg* workerArg = new SumWorkerArg;

    workerArg->inputPipe = &inPipe;
    workerArg->outputPipe = &outPipe;
    workerArg->computeMe = &computeMe;

    pthread_create(&workerThread, NULL, sumWorker, (void*) workerArg);
}

void Sum::WaitUntilDone() {
    pthread_join(workerThread, NULL);
}

void Sum::Use_n_Pages(int n) {

}

//Join
typedef struct {
    Pipe* leftInputPipe;
    Pipe* rightInputPipe;
    Pipe* outputPipe;
    CNF* cnf;
    Record* literal;
    int runLength;
} JoinWorkerArg;

void* joinWorker(void* arg) {
    JoinWorkerArg *workerArg = (JoinWorkerArg *) arg;

    OrderMaker orderMakerL, ordermakeR;
    workerArg->cnf->GetSortOrders(orderMakerL, ordermakeR);

    if (orderMakerL.numAtts != 0 && ordermakeR.numAtts != 0) {
        Pipe sortedPipeL(DEFAULT_PIPE_SIZE), sortedPipeR(DEFAULT_PIPE_SIZE);
        BigQ(*workerArg->leftInputPipe, sortedPipeL, orderMakerL, workerArg->runLength);
        BigQ(*workerArg->rightInputPipe, sortedPipeR, ordermakeR, workerArg->runLength);
        sortMerge(&sortedPipeL, &sortedPipeR, workerArg->outputPipe, &orderMakerL, &ordermakeR);
    } else {
        nestedBlock(workerArg->leftInputPipe, workerArg->rightInputPipe, workerArg->outputPipe, workerArg->runLength);
    }

    workerArg->outputPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinWorkerArg* workerArg = new JoinWorkerArg;

    workerArg->leftInputPipe = &inPipeL;
    workerArg->rightInputPipe = &inPipeR;
    workerArg->outputPipe = &outPipe;
    workerArg->cnf = &selOp;
    workerArg->literal = &literal;
    workerArg->runLength = this->runLen;

    pthread_create(&workerThread, NULL, joinWorker, (void*) workerArg);
}

void sortMerge(Pipe* inPipeL, Pipe* inPipeR, Pipe* outPipe, OrderMaker* orderMakerL, OrderMaker* orderMakerR) {

    Record recordL, recordR;
    bool isLeft = inPipeL->Remove(&recordL);
    bool isRight = inPipeR->Remove(&recordR);

    ComparisonEngine comparisonEngine;

    while (isLeft && isRight) {
        int comparisonValue = comparisonEngine.Compare(&recordL, orderMakerL, &recordR, orderMakerR);
        if (comparisonValue == 0) {
            vector<Record*> recordsL, recordsR;
            Record* tempLeft = new Record();
            tempLeft->Consume(&recordL);
            recordsL.push_back(tempLeft);

            int index = 0;

            while ((isLeft = inPipeL->Remove(&recordL)) && comparisonEngine.Compare(recordsL[index], &recordL, orderMakerL) == 0) {
                tempLeft = new Record();
                tempLeft->Consume(&recordL);
                recordsL.push_back(tempLeft);
            }

            Record *tempRight = new Record();
            tempRight->Consume(&recordR);
            recordsR.push_back(tempRight);
            index = 0;

            while ((isRight = inPipeR->Remove(&recordR)) && comparisonEngine.Compare(recordsR[index++], &recordR, orderMakerR) == 0) {
                tempRight = new Record();
                tempRight->Consume(&recordR);
                recordsR.push_back(tempRight);
            }
            Record mergedRecord;
            for (Record *left : recordsL) {
                for (Record *right : recordsR) {
                    mergedRecord.MergeRecords(left, right);
                    outPipe->Insert(&mergedRecord);
                }
            }
        } else if (comparisonValue < 0) {
            isLeft = inPipeL->Remove(&recordL);
        } else {
            isRight = inPipeR->Remove(&recordR);
        }
    }
    while (isLeft) {
        isLeft = inPipeL->Remove(&recordL);
    }
    while (isRight) {
        isRight = inPipeR->Remove(&recordR);
    }
}

void nestedBlock(Pipe *inPipeL, Pipe *inPipeR, Pipe *outPipe, int runLength) {
  
    HeapDBFile dBFileL;
    HeapDBFile dBFileR;

    char *fileNameL = "tempDBfileL.bin";
    char *fileNameR = "tempDBfileR.bin";

    dBFileL.Create(fileNameL, heap, NULL);
    dBFileR.Create(fileNameR, heap, NULL);

  
    Record temp;
    while (inPipeL->Remove(&temp)) {
        dBFileL.Add(temp);
    }
    while (inPipeR->Remove(&temp)) {
        dBFileR.Add(temp);
    }

    Record recordL, recordR, mergedRecord;
    Page *recordsBlock = new(std::nothrow) Page[runLength];
    if (recordsBlock == NULL) {
        exit(1);
    }

    dBFileL.MoveFirst();
    bool isLeft = dBFileL.GetNext(recordL);

    while (isLeft) {
        int blockPageIndex = 0;

        while (isLeft) {
            if (!recordsBlock[blockPageIndex].Append(&recordL)) {
                if (blockPageIndex + 1 < runLength) {
                    blockPageIndex++;
                    recordsBlock[blockPageIndex].Append(&recordL);
                } else {
                    break;
                }
            }
            isLeft = dBFileL.GetNext(recordL);
        }
        vector<Record *> blockRecordsL;
        Record *temp = new Record();
            for (int i = 0; i <= blockPageIndex; i++) {
                while (recordsBlock[i].GetFirst(temp)) {
                    blockRecordsL.push_back(temp);
                    temp = new Record();
                }
            }
        dBFileR.MoveFirst();
        bool isRight = dBFileR.GetNext(recordR);

        while (isRight) {
            blockPageIndex = 0;
            while (isRight) {
                if (!recordsBlock[blockPageIndex].Append(&recordR)) {
                    if (blockPageIndex + 1 < runLength) {
                        blockPageIndex++;
                        recordsBlock[blockPageIndex].Append(&recordR);
                    } else {
                        break;
                    }
                }
                isRight = dBFileR.GetNext(recordR);
            }
            vector<Record *> blockRecordsR;
            Record *temp = new Record();
            for (int i = 0; i <= blockPageIndex; i++) {
                while (recordsBlock[i].GetFirst(temp)) {
                    blockRecordsR.push_back(temp);
                    temp = new Record();
                }
            }
            Record *mergedRecord = new Record();
            for (Record *leftBlockRecord : blockRecordsL) {
                for (Record *rightBlockRecord : blockRecordsR) {
                    mergedRecord->MergeRecords(leftBlockRecord, rightBlockRecord);
                    outPipe->Insert(mergedRecord);
                }
            }
        }
    }
    remove(fileNameL);
    remove(fileNameR);
}

void Join::WaitUntilDone() { 
    pthread_join(workerThread, NULL);
}
    
void Join::Use_n_Pages(int n) { 
    this->runLen = n;
}



//========================================GroupBy
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    OrderMaker &groupAtts;
    Function &computeMe;
    int runLen;
} GorupByWorkerArg;

void* groupByWorker(void*arg){
    GorupByWorkerArg* workerArg = (GorupByWorkerArg*) arg;
    Pipe sorted(DEFAULT_PIPE_SIZE);
    BigQ bigQ(workerArg->inPipe, sorted, workerArg->groupAtts, workerArg->runLen);
    int intRes = 0, intSum = 0;
    double doubleRes = 0.0, doubleSum = 0.0;
    ComparisonEngine cmp;
    Record prev;
    Record cur;
    Type t;
    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    bool firstTime = true;
    while(sorted.Remove(&cur)){
        if(!firstTime && cmp.Compare(&cur, &prev, &workerArg->groupAtts)!=0){
            Record res;
            char charsRes[100];
            if(t==Int){
                sprintf(charsRes, "%d|", intSum);
            }
            else {
                sprintf(charsRes, "%lf|", doubleSum);
            }
            res.ComposeRecord(&out_sch, charsRes);
            workerArg->outPipe.Insert(&res);
            intSum = 0;
            doubleSum = 0.0;
        }
        firstTime = false;
        t = workerArg->computeMe.Apply(cur, intRes, doubleRes);
        if(t==Int){
            intSum += intRes;
        }
        else {
            doubleSum += doubleRes;
        }
        prev.Copy(&cur);
    }
    Record res;
    char charsRes[100];
    if(t==Int){
        sprintf(charsRes, "%d|", intSum);
    }
    else {
        sprintf(charsRes, "%lf|", doubleSum);
    }
    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();
    return NULL;
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
    GorupByWorkerArg* workerArg = new GorupByWorkerArg{inPipe, outPipe, groupAtts, computeMe, runLen};
    pthread_create(&worker, NULL, groupByWorker, (void*) workerArg);
}

void GroupBy::WaitUntilDone (){
    pthread_join(worker, NULL);
}

void GroupBy::Use_n_Pages (int n){
    runLen = n;
}

//Duplicate Removal
typedef struct {
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
} DuplicateRemovalWorkerArg;

void* duplicateRemovalWorker(void* arg) {
    DuplicateRemovalWorkerArg* wokerArg = (DuplicateRemovalWorkerArg*) arg;
    ComparisonEngine comparisonEngine;
    Record current, previous;
    Pipe* sortedPipe = new Pipe(DEFAULT_PIPE_SIZE);

    
    BigQ* bq = new BigQ(*(wokerArg->inPipe), *sortedPipe, *(wokerArg->order), wokerArg->runLen);
    sortedPipe->Remove(&previous);

    while (sortedPipe->Remove(&current) == 1) {
        if (comparisonEngine.Compare(&previous, &current, wokerArg->order) != 0) {
            Record* temp = new Record;
            temp->Consume(&previous);
            wokerArg->outPipe->Insert(temp);
            previous.Consume(&current);
        }
    }
    wokerArg->outPipe->Insert(&previous);
    wokerArg->outPipe->ShutDown();
    return NULL;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    DuplicateRemovalWorkerArg* wokerArg = new DuplicateRemovalWorkerArg;
    wokerArg->inPipe = &inPipe;
    wokerArg->outPipe = &outPipe;
    OrderMaker* order = new OrderMaker(&mySchema);
    wokerArg->order = order;
    wokerArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, duplicateRemovalWorker, (void*) wokerArg);
}

void DuplicateRemoval::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
    this->runLen = n;
}


//WriteOut
typedef struct {
	Pipe *inPipe;
	FILE *outFile;
	Schema *schema;
} WriteOutWorkerArg;

void* WriteOutWorker(void* arg) {
    WriteOutWorkerArg* workerArg = (WriteOutWorkerArg*) arg;
    Record cur;
    while (workerArg->inPipe->Remove(&cur) == 1) {
        int numOfAtts = workerArg->schema->GetNumAtts();
        Attribute *attribute = workerArg->schema->GetAtts();
        for (int i = 0; i < numOfAtts; i++) {
            fprintf(workerArg->outFile, "%s:", attribute[i].name);
            int pointer = ((int *) cur.bits)[i + 1];
            fprintf(workerArg->outFile, "[");
            if (attribute[i].myType == Int) {
                int *writeOutInt = (int*) &(cur.bits[pointer]);
                fprintf(workerArg->outFile, "%d", *writeOutInt);
            }
            else if (attribute[i].myType == Double) {
                double *writeOutDouble = (double*) &(cur.bits[pointer]);
                fprintf(workerArg->outFile, "%f", *writeOutDouble);
            }
            else if (attribute[i].myType == String) {
                char* writeOutString = (char*) &(cur.bits[pointer]);
                fprintf(workerArg->outFile, "%s", writeOutString);
            }
            fprintf(workerArg->outFile, "]");
            if (i != numOfAtts - 1)
                fprintf(workerArg->outFile, ", ");
        }
        fprintf(workerArg->outFile, "\n");
    }
    return NULL;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WriteOutWorkerArg* workerArg = new WriteOutWorkerArg;
    workerArg->inPipe = &inPipe;
    workerArg->outFile = outFile;
    workerArg->schema = &mySchema;
    pthread_create(&workerThread, NULL, WriteOutWorker, (void*) workerArg);
}

void WriteOut::WaitUntilDone () {
    pthread_join(workerThread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 

}