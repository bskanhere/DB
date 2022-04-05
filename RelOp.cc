#include "RelOp.h"
#include <iostream>
#include "BigQ.h"

static char *SUM_ATT_NAME = "SUM";
static Attribute doubleAtt = {SUM_ATT_NAME, Double};
static Schema sumSchema("sum_schema", 1, &doubleAtt);


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

    if (orderMakerL.isEmpty() || ordermakeR.isEmpty()) {
        NestedBlockJoin(workerArg->leftInputPipe, workerArg->rightInputPipe, workerArg->outputPipe, workerArg->runLength);
    } else {
        Pipe sortedPipeL(DEFAULT_PIPE_SIZE), sortedPipeR(DEFAULT_PIPE_SIZE);
        BigQ(*workerArg->leftInputPipe, sortedPipeL, orderMakerL, workerArg->runLength);
        BigQ(*workerArg->rightInputPipe, sortedPipeR, ordermakeR, workerArg->runLength);
        sortMerge(&sortedPipeL, &sortedPipeR, workerArg->outputPipe, &orderMakerL, &ordermakeR);
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

void NestedBlockJoin(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe, int runLength) {
    // Create temporary heap DBFiles for left and right input pipe.
    HeapDBFile leftDBFile;
    HeapDBFile rightDBFile;

    char *leftDBFileName = "tempLeftDBfile.bin";
    char *rightDBFileName = "tempRightDBfile.bin";

    leftDBFile.Create(leftDBFileName, heap, NULL);
    rightDBFile.Create(rightDBFileName, heap, NULL);

    // Add all left input pipe's records into DBfile.
    Record temp;
    while (leftInputPipe->Remove(&temp)) {
        leftDBFile.Add(temp);
    }
    // Add all right input pipe's records into DBfile.
    while (rightInputPipe->Remove(&temp)) {
        rightDBFile.Add(temp);
    }

    // Nested join to merge all records from left DBFile and right DBFile.
    Record leftDBFileRecord, rightDBFileRecord, mergedRecord;
    // Create buffer page array to store current runs' records
    Page *recordsBlock = new(std::nothrow) Page[runLength];
    if (recordsBlock == NULL) {
        exit(1);
    }

    leftDBFile.MoveFirst();
    bool leftDBFileNotFullyConsumed = leftDBFile.GetNext(leftDBFileRecord);

    while (leftDBFileNotFullyConsumed) {
        int blockPageIndex = 0;

        while (leftDBFileNotFullyConsumed) {
            if (!recordsBlock[blockPageIndex].Append(&leftDBFileRecord)) {
                if (blockPageIndex + 1 < runLength) {
                    blockPageIndex++;
                    recordsBlock[blockPageIndex].Append(&leftDBFileRecord);
                } else {
                    break;
                }
            }
            leftDBFileNotFullyConsumed = leftDBFile.GetNext(leftDBFileRecord);
        }
        vector<Record *> leftBlockRecords;
        LoadVectorFromBlock(&leftBlockRecords, recordsBlock, blockPageIndex);

        rightDBFile.MoveFirst();
        bool rightDBFileNotFullyConsumed = rightDBFile.GetNext(rightDBFileRecord);

        while (rightDBFileNotFullyConsumed) {
            blockPageIndex = 0;
            while (rightDBFileNotFullyConsumed) {
                if (!recordsBlock[blockPageIndex].Append(&rightDBFileRecord)) {
                    if (blockPageIndex + 1 < runLength) {
                        blockPageIndex++;
                        recordsBlock[blockPageIndex].Append(&rightDBFileRecord);
                    } else {
                        break;
                    }
                }
                rightDBFileNotFullyConsumed = rightDBFile.GetNext(rightDBFileRecord);
            }
            vector<Record *> rightBlockRecords;
            LoadVectorFromBlock(&rightBlockRecords, recordsBlock, blockPageIndex);
            // Join left block and right block of pages.
            JoinTableBlocks(&leftBlockRecords, &rightBlockRecords, outputPipe);
        }
    }


    // Delete both temporary files.
    remove(leftDBFileName);
    remove(rightDBFileName);
}

void LoadVectorFromBlock(vector<Record *> *loadMe, Page *block, int blockLength) {
    Record *temp = new Record();
    for (int i = 0; i <= blockLength; i++) {
        while (block[i].GetFirst(temp)) {
            loadMe->push_back(temp);
            temp = new Record();
        }
    }
}

void JoinTableBlocks(vector<Record *> *leftBlockRecords, vector<Record *> *rightBlockRecords, Pipe *outputPipe) {
    Record *mergedRecord = new Record();
    for (Record *leftBlockRecord : *leftBlockRecords) {
        for (Record *rightBlockRecord : *rightBlockRecords) {
            mergedRecord->MergeTheRecords(leftBlockRecord, rightBlockRecord);
            outputPipe->Insert(mergedRecord);
        }
    }
}

void sortMerge(Pipe *inPipeL, Pipe *inPipeR, Pipe *outPipe,
                        OrderMaker *orderMakerL, OrderMaker *orderMakerR) {

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

            while ((isLeft = inPipeL->Remove(&recordL)) &&
                   comparisonEngine.Compare(recordsL[index], &recordL, orderMakerL) == 0) {
                tempLeft = new Record();
                tempLeft->Consume(&recordL);
                recordsL.push_back(tempLeft);
            }

            Record *tempRight = new Record();
            tempRight->Consume(&recordR);
            recordsR.push_back(tempRight);
            index = 0;

            while ((isRight = inPipeR->Remove(&recordR)) &&
                   comparisonEngine.Compare(recordsR[index++], &recordR, orderMakerR) == 0) {
                tempRight = new Record();
                tempRight->Consume(&recordR);
                recordsR.push_back(tempRight);
            }
            Record mergedRecord;
            for (Record *left : recordsL) {
                for (Record *right : recordsR) {
                    mergedRecord.MergeTheRecords(left, right);
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

void Join::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}
    
void Join::Use_n_Pages (int n) { 
    this->runLen = n;
}







//========================================GroupBy
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    OrderMaker &groupAtts;
    Function &computeMe;
    int use_n_pages;
} WorkerArg4;

void* workerMain4(void*arg){
    WorkerArg4* workerArg = (WorkerArg4*) arg;
    Pipe sorted(100);
    BigQ bigQ(workerArg->inPipe, sorted, workerArg->groupAtts, workerArg->use_n_pages);
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
            // different, this is a new group
//            cur.Print(&workerArg->groupAtts);
//            prev.Print(&workerArg->groupAtts);
            cout<<"==="<<endl;
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
        // add to the previous group
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
    // for the last group
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
    WorkerArg4* workerArg = new WorkerArg4{inPipe, outPipe, groupAtts, computeMe, use_n_pages};
    pthread_create(&worker, NULL, workerMain4, (void*) workerArg);
}

void GroupBy::WaitUntilDone (){
    pthread_join(worker, NULL);
}

void GroupBy::Use_n_Pages (int n){
    use_n_pages = n;
}

//Duplicate Removal
void* duplicateRemovalWorker (void* arg) {
    DuplicateRemovalArg* duplicateRemovalArg = (DuplicateRemovalArg*) arg;
    ComparisonEngine comparisonEngine;
    Record current, previous;
    Pipe* sortedPipe = new Pipe(1000);

    //Using BigQ to sort the records and put sorted records into a new pipe
    BigQ* bq = new BigQ(*(duplicateRemovalArg->inPipe), *sortedPipe, *(duplicateRemovalArg->order), duplicateRemovalArg->runLen);
    sortedPipe->Remove(&previous);

    //Check duplicate records by using sorted pipe, only forward distinct records to output pipe
    while (sortedPipe->Remove(&current) == 1) {
        if (comparisonEngine.Compare(&previous, &current, duplicateRemovalArg->order) != 0) {
            Record* temp = new Record;
            temp->Consume(&previous);
            duplicateRemovalArg->outPipe->Insert(temp);
            previous.Consume(&current);
        }
    }
    duplicateRemovalArg->outPipe->Insert(&previous);
    duplicateRemovalArg->outPipe->ShutDown();
    return NULL;
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    DuplicateRemovalArg* duplicateRemovalArg = new DuplicateRemovalArg;
    duplicateRemovalArg->inPipe = &inPipe;
    duplicateRemovalArg->outPipe = &outPipe;
    OrderMaker* order = new OrderMaker(&mySchema);
    duplicateRemovalArg->order = order;
    duplicateRemovalArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, duplicateRemovalWorker, (void*) duplicateRemovalArg);
}

void DuplicateRemoval::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int n) {
    this->runLen = n;
}


//WriteOut
void* WriteOutWorker (void* arg) {
    //Write records into file according to format defined in Print() method of Record
    WriteOutArg* writeOutArg = (WriteOutArg*) arg;
    Record cur;
    while (writeOutArg->inPipe->Remove(&cur) == 1) {
        int numOfAtts = writeOutArg->schema->GetNumAtts();
        Attribute *attribute = writeOutArg->schema->GetAtts();
        for (int i = 0; i < numOfAtts; i++) {
            fprintf(writeOutArg->outFile, "%s:", attribute[i].name);
            int pointer = ((int *) cur.bits)[i + 1];
            fprintf(writeOutArg->outFile, "[");
            if (attribute[i].myType == Int) {
                int *writeOutInt = (int*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%d", *writeOutInt);
            }
            else if (attribute[i].myType == Double) {
                double *writeOutDouble = (double*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%f", *writeOutDouble);
            }
            else if (attribute[i].myType == String) {
                char* writeOutString = (char*) &(cur.bits[pointer]);
                fprintf(writeOutArg->outFile, "%s", writeOutString);
            }
            fprintf(writeOutArg->outFile, "]");
            if (i != numOfAtts - 1)
                fprintf(writeOutArg->outFile, ", ");
        }
        fprintf(writeOutArg->outFile, "\n");
    }
    return NULL;
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    //Construct parameters and send them to main worker
    WriteOutArg* writeOutArg = new WriteOutArg;
    writeOutArg->inPipe = &inPipe;
    writeOutArg->outFile = outFile;
    writeOutArg->schema = &mySchema;
    pthread_create(&workerThread, NULL, WriteOutWorker, (void*) writeOutArg);
}

void WriteOut::WaitUntilDone () {
    pthread_join(workerThread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 

}