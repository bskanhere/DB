#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    WorkerThreadArgs* workerThreadArgs = new WorkerThreadArgs;
    workerThreadArgs->in = &in;
    workerThreadArgs->out = &out;
    workerThreadArgs->order = &sortorder;
    workerThreadArgs->runlen = runlen;

    pthread_t workerThread;
    pthread_create(&workerThread, NULL, TPMMSAlgo, (void*) workerThreadArgs);
}

void* TPMMSAlgo(void* arg) {
    
    //cout << "Begin Worker Thread" << endl;

    WorkerThreadArgs* workerThreadArgs = (WorkerThreadArgs*) arg;
    priority_queue<Run*, vector<Run*>, RunComparator> runHeap(workerThreadArgs->order);
    priority_queue<Record*, vector<Record*>, RecordComparator> recordHeap (workerThreadArgs->order);
    vector<Record* > recBuff;
    
	char randomString[8];
    static const char alphaNum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < 8; ++i) {
        randomString[i] = alphaNum[rand() % (sizeof(alphaNum) - 1)];
    }
    randomString[8] = 0;
    std::string fileName("tpmms");
    fileName = fileName + randomString + ".bin";
    File tpmmsTempFile;
    tpmmsTempFile.Open(0, const_cast<char*>(fileName.c_str())); //Disk based file for storing sorted Runs 

    Page bufferPage;
    int pageIndex = 0;
    int pageCounter = 0;
    Record curRecord;
  
    while (workerThreadArgs->in->Remove(&curRecord) == 1) {//Phase 1 - Retrieve all records from input pipe
        Record* tempRecord = new Record;
        tempRecord->Copy(&curRecord);
        if (bufferPage.Append(&curRecord) == 0) {//If current page is increase Page Counter
            pageCounter++;
            bufferPage.EmptyItOut();

            if (pageCounter == workerThreadArgs->runlen) {//If Runlen Pages are in memory, Create Run, sort and store it on disk
                bufferPage.EmptyItOut();
                int startIndex = pageIndex;
                while (!recordHeap.empty()) {
                    Record* tempRecord1 = new Record;
                    tempRecord1->Copy(recordHeap.top());
                    recordHeap.pop();
                    if (bufferPage.Append(tempRecord1) == 0) {
                        tpmmsTempFile.AddPage(&bufferPage, pageIndex++);
                        bufferPage.EmptyItOut();
                        bufferPage.Append(tempRecord1);
                    }
                }
                tpmmsTempFile.AddPage(&bufferPage, pageIndex++);
                bufferPage.EmptyItOut();
                Run* run = new Run(&tpmmsTempFile, startIndex, pageIndex - startIndex);
                runHeap.push(run);
                recordHeap = priority_queue<Record*, vector<Record*>, RecordComparator> (workerThreadArgs->order);
                pageCounter = 0;
            }
            bufferPage.Append(&curRecord);
        }
        recordHeap.push(tempRecord);
    }
    
    if (!recordHeap.empty()) {// Handle Pages in Memory not yet added to a Disk
        bufferPage.EmptyItOut();
        int startIndex = pageIndex;
        while (!recordHeap.empty()) {
            Record* tempRecord = new Record;
            tempRecord->Copy(recordHeap.top());
            recordHeap.pop();
            if (bufferPage.Append(tempRecord) == 0) {
                tpmmsTempFile.AddPage(&bufferPage, pageIndex++);
                bufferPage.EmptyItOut();
                bufferPage.Append(tempRecord);
            }
        }
        tpmmsTempFile.AddPage(&bufferPage, pageIndex++);
        bufferPage.EmptyItOut();
        Run* run = new Run(&tpmmsTempFile, startIndex, pageIndex - startIndex);
        runHeap.push(run);
        recordHeap = priority_queue<Record*, vector<Record*>, RecordComparator> (workerThreadArgs->order);
    }
    int count = 0;
    while (!runHeap.empty()) {//Phase 2 -  Merging Runs to Produce Sorted Records
        Run* run = runHeap.top();
        runHeap.pop();
        workerThreadArgs->out->Insert(run->currRecord);
        count++;
        if (run->getNextRecord() == 1) {
            runHeap.push(run);
        }
    }
    cout << "BigQ output - " << count << endl;
    workerThreadArgs->out->ShutDown();
    tpmmsTempFile.Close();
    return NULL;
}

Run::Run(File* file, int startPageIndex, int runLength) {
    runFile = file;
    currPageIndex = startPageIndex;
    maxPageIndex = startPageIndex + runLength;
    runFile->GetPage(&bufferPage, currPageIndex);
    currRecord = new Record();
    getNextRecord();
}

int Run::getNextRecord() { //move to the next record of the run
    if (bufferPage.GetFirst(currRecord) == 0) {
        currPageIndex++;
        if (currPageIndex == maxPageIndex) {
            return 0;
        }
        bufferPage.EmptyItOut();
        runFile->GetPage(&bufferPage, currPageIndex);
        bufferPage.GetFirst(currRecord);
    }
    return 1;
}

RecordComparator::RecordComparator(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool RecordComparator::operator() (Record* left, Record* right) {
    if (comparisonEngine.Compare(left, right, order) >= 0)
        return true;
    return false;
}

RunComparator::RunComparator(OrderMaker* orderMaker) {
    order = orderMaker;
}

bool RunComparator::operator() (Run* left, Run* right) {
    if (comparisonEngine.Compare(left->currRecord, right->currRecord, order) >= 0)
        return true;
    return false;
}

BigQ::~BigQ () {

}
