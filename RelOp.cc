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
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
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
    Pipe *inputPipe;
    Pipe *outputPipe;
    Function *computeMe;
}SumWorkerArg;

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

    pthread_create(&workerThread, nullptr, sumWorker, (void*) workerArg);
}

void Sum::WaitUntilDone() {
    pthread_join(workerThread, NULL);
}

void Sum::Use_n_Pages(int n) {

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


//Join

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JoinData *my_data = new JoinData();

    my_data->leftInputPipe = &inPipeL;
    my_data->rightInputPipe = &inPipeR;
    my_data->outputPipe = &outPipe;
    my_data->cnf = &selOp;
    my_data->literal = &literal;
    my_data->runLength = this->runLen;

    pthread_create(&workerThread, nullptr, JoinThreadMethod, (void *) my_data);
}

void *JoinThreadMethod(void *threadData) {
    JoinData *my_data = (JoinData *) threadData;

    OrderMaker leftOrderMaker, rightOrderMaker;
    my_data->cnf->GetSortOrders(leftOrderMaker, rightOrderMaker);

    if (leftOrderMaker.isEmpty() || rightOrderMaker.isEmpty()) {
        NestedBlockJoin(my_data->leftInputPipe, my_data->rightInputPipe, my_data->outputPipe, my_data->runLength);
    } else {
        Pipe leftBigQOutputPipe(DEFAULT_PIPE_SIZE), rightBigQOutputPipe(DEFAULT_PIPE_SIZE);
        BigQ(*my_data->leftInputPipe, leftBigQOutputPipe, leftOrderMaker, my_data->runLength);
        BigQ(*my_data->rightInputPipe, rightBigQOutputPipe, rightOrderMaker, my_data->runLength);
        JoinUsingSortMerge(&leftBigQOutputPipe, &rightBigQOutputPipe, my_data->outputPipe,
                           &leftOrderMaker, &rightOrderMaker);
    }

    my_data->outputPipe->ShutDown();

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

void JoinUsingSortMerge(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe,
                        OrderMaker *leftOrderMaker, OrderMaker *rightOrderMaker) {

    int count = 0;
    int val=0;
    int l = 0;
    int s = 1;
    int countL = 0;
    int countR = 0;
    Record leftOutputPipeRecord, rightOutputPipeRecord;
    bool leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
    bool rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);

    ComparisonEngine comparisonEngine;
    // While there is data in both the input pipes.
    while (leftOutputPipeNotFullyConsumed && rightOutputPipeNotFullyConsumed) {
        //cout << "In Sort Merge " << s++ << endl;
        // Compare the left and right record.
        int comparisonValue = comparisonEngine.Compare(&leftOutputPipeRecord, leftOrderMaker,
                                                       &rightOutputPipeRecord, rightOrderMaker);
        cout << comparisonValue << "Comparison" << endl;
        if (comparisonValue == 0) {
            vector<Record *> leftPipeRecords, rightPipeRecords;
            Record *tempLeft = new Record();
            tempLeft->Consume(&leftOutputPipeRecord);
            leftPipeRecords.push_back(tempLeft);

            int index = 0;
            countL = 1;
            l++;
            while ((leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord)) &&
                   comparisonEngine.Compare(leftPipeRecords[index], &leftOutputPipeRecord, leftOrderMaker) == 0) {
                tempLeft = new Record();
                tempLeft->Consume(&leftOutputPipeRecord);
                leftPipeRecords.push_back(tempLeft);
                countL++;
            }

            Record *tempRight = new Record();
            tempRight->Consume(&rightOutputPipeRecord);
            rightPipeRecords.push_back(tempRight);
            countR = 1;
            index = 0;
            while ((rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord)) &&
                   comparisonEngine.Compare(rightPipeRecords[index++], &rightOutputPipeRecord, rightOrderMaker) == 0) {
                tempRight = new Record();
                tempRight->Consume(&rightOutputPipeRecord);
                rightPipeRecords.push_back(tempRight);
                countR++;
            }
            val += countL * countR;
            Record mergedRecord;
            for (Record *leftPipeRecord : leftPipeRecords) {
                for (Record *rightPipeRecord : rightPipeRecords) {
                    mergedRecord.MergeTheRecords(leftPipeRecord, rightPipeRecord);
                    outputPipe->Insert(&mergedRecord);
                    count++;
                }
            }
            cout << l << " L " << countL << "Left " << countR << " Right " << val << " Val " << count <<" count" << endl;
        } else if (comparisonValue < 0) {
            leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
            cout << "Removing Left" << endl;
        } else {
            rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);
            //cout << "Removing Right" << endl;
        }
    }
    while (leftOutputPipeNotFullyConsumed) {
        leftOutputPipeNotFullyConsumed = leftInputPipe->Remove(&leftOutputPipeRecord);
    }
    while (rightOutputPipeNotFullyConsumed) {
        rightOutputPipeNotFullyConsumed = rightInputPipe->Remove(&rightOutputPipeRecord);
    }
    cout << "Count " << count << endl;
}

void Join::WaitUntilDone () { 
    pthread_join(workerThread, NULL);
}
    
void Join::Use_n_Pages (int n) { 
    this->runLen = n;
}


// void generateRandomString(char *s, int len) {
//     static const char alphaNum[] =
//         "0123456789"
//         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
//         "abcdefghijklmnopqrstuvwxyz";

//     for (int i = 0; i < len; ++i) {
//         s[i] = alphaNum[rand() % (sizeof(alphaNum) - 1)];
//     }
//     s[len] = 0;
// }

// void* Join::Operate (void *arg) {
// 	//RelationalOpThreadMemberHolder *params = (RelationalOpThreadMemberHolder*) arg; // parse thread params
// 	JoinArg* params = (JoinArg*) arg;
//     OrderMaker leftOrderMaker, rightOrderMaker; // left and right ordermaker
// 	if (params->selOp->GetSortOrders(leftOrderMaker, rightOrderMaker)){ // if an acceptable ordering exists
// 		SortMergeJoin(params->inPipeL, &leftOrderMaker, params->inPipeR, &rightOrderMaker, params->outPipe, params->selOp, params->literal, params->runLen); // the perform sort merge join
//         cout<<"Done with Merge"<<endl;
//     } else{
// 		NestedLoopJoin(params->inPipeL, params->inPipeR, params->outPipe, params->selOp, params->literal, params->runLen); // else perform nested loop join
// 	}
//     cout<<"Shutting Down Pipe"<<endl;
// 	params->outPipe->ShutDown(); // shutdown output pipe
// }

// void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
// 	//RelationalOpThreadMemberHolder *params = new RelationalOpThreadMemberHolder(NULL, &inPipeL, NULL, &outPipe, NULL, &selOp, &literal, 0, 0, NULL, this->runLength, NULL, NULL, &inPipeR); // init thread params
// 	JoinArg* joinArg = new JoinArg;
//     joinArg->inPipeL = &inPipeL;
//     joinArg->inPipeR = &inPipeR;
//     joinArg->outPipe = &outPipe;
//     joinArg->selOp = &selOp;
//     joinArg->literal = &literal;
//     joinArg->runLen = this->runLen;
//     pthread_create(&workerThread, NULL, Operate, (void*) joinArg); // create thread
// }

// void Join::SortMergeJoin(Pipe* leftPipe, OrderMaker* leftOrderMaker, Pipe* rightPipe, OrderMaker* rightOrderMaker, Pipe* outPipe, CNF* selOp, Record* literal, int runLength) {
//     cout << "In Sorted"<<endl;
// 	ComparisonEngine comparisonEngine;
// 	Pipe leftSortedPipe(PIPE_SIZE), rightSortedPipe(PIPE_SIZE);
// 	BigQ leftBigQ(*leftPipe, leftSortedPipe, *leftOrderMaker, runLength), rightBigQ(*rightPipe, rightSortedPipe, *rightOrderMaker, runLength); // start BigQ to get the records in sorted order
// 	Record recordFromLeft, recordFromRight, mergedRecord, previousRecord;
// 	FixedSizeRecordBuffer recordBuffer(runLength);
// 	bool leftNotEmpty = leftSortedPipe.Remove(&recordFromLeft), rightNotEmpty = rightSortedPipe.Remove(&recordFromRight);

// 	while(leftNotEmpty && rightNotEmpty) { // while there are records in both the sorted pipes
// 		//cout<<"In Step"<<endl;
//         int comparisonStatusForPipes = comparisonEngine.Compare(&recordFromLeft, leftOrderMaker, &recordFromRight, rightOrderMaker); // compare records based on respective ordermakers
		
// 		if (comparisonStatusForPipes<0){  // if left is smaller than right
// 			leftNotEmpty = leftSortedPipe.Remove(&recordFromLeft); // then advance left
// 		} else if (comparisonStatusForPipes>0){ // if right is smaller than left
// 			rightNotEmpty = rightSortedPipe.Remove(&recordFromRight); // advance right
// 		} else { // if left and right are equal
// 			//cout<<"In Merge"<<endl;
//             recordBuffer.Clear(); // clear buffer
// 			for(previousRecord.Consume(&recordFromLeft); (leftNotEmpty=leftSortedPipe.Remove(&recordFromLeft)) && comparisonEngine.Compare(&previousRecord, &recordFromLeft, leftOrderMaker)==0; previousRecord.Consume(&recordFromLeft)){
// 	  			recordBuffer.Add(previousRecord); // accumulate records of the same value
// 	  		}
// 	  		recordBuffer.Add(previousRecord); // add the last record
// 	  		int comparisonStatusForBufferAndPipe;
// 			do { // Join records from the buffer one by one with the head of the pipe until the records from the buffer match with the head of the pipe
// 			  	for (Record *recordFromBuffer=recordBuffer.buffer; recordFromBuffer!=recordBuffer.buffer+(recordBuffer.numRecords); recordFromBuffer++) {
// 			  		if (comparisonEngine.Compare(recordFromBuffer, &recordFromRight, literal, selOp)) {   // if they match
// 			  			mergedRecord.MergeTheRecords(recordFromBuffer, &recordFromRight); // concatenates left and right by setting up attsToKeep
// 			  			outPipe->Insert(&mergedRecord);
//                         //cout<<"Record Inserted"<<endl;
// 			  		}
// 			  	}
// 			  	rightNotEmpty = rightSortedPipe.Remove(&recordFromRight);
// 			  	comparisonStatusForBufferAndPipe = comparisonEngine.Compare(recordBuffer.buffer, leftOrderMaker, &recordFromRight, rightOrderMaker);
// 	  		} while (rightNotEmpty && comparisonStatusForBufferAndPipe==0);    // read all records from right pipe with equal value
// 		}
// 	}
//     while(leftNotEmpty){
//         leftNotEmpty = leftSortedPipe.Remove(&recordFromLeft);
//     }
//     while(rightNotEmpty){
//         rightNotEmpty = rightSortedPipe.Remove(&recordFromRight);
//     }
// }

// void Join::NestedLoopJoin(Pipe* leftPipe, Pipe* rightPipe, Pipe* outPipe, CNF* selOp, Record* literal, int runLength) {
// 	DBFile rightFile;
// 	PipeToFile(*rightPipe, rightFile);
// 	FixedSizeRecordBuffer leftBuffer(runLength);

// 	Record recordFromLeft;
// 	while(leftPipe->Remove(&recordFromLeft)){
// 		if (!leftBuffer.Add(recordFromLeft)) {  // if buffer is full
// 			JoinBufferWithFile(leftBuffer, rightFile, *outPipe, *literal, *selOp); // join records from buffer and right file
// 			leftBuffer.Clear(); // clear buffer
// 			leftBuffer.Add(recordFromLeft); // add this record now
// 		}
// 	}
// 	JoinBufferWithFile(leftBuffer, rightFile, *outPipe, *literal, *selOp); // join records from buffer and right file
// 	rightFile.Close(); // close rightFile
// }

// void Join::JoinBufferWithFile(FixedSizeRecordBuffer& recordBuffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
// 	ComparisonEngine cmp;
// 	Record merged;

// 	Record recordFromFile;
// 	file.MoveFirst();
// 	while(file.GetNext(recordFromFile)){
// 		for (Record *recordFromBuffer=recordBuffer.buffer; recordFromBuffer!=recordBuffer.buffer+(recordBuffer.numRecords); recordFromBuffer++) {
// 			if (cmp.Compare(recordFromBuffer, &recordFromFile, &literal, &selOp)) {
// 				merged.MergeTheRecords(recordFromBuffer, &recordFromFile); // concatenates left and right by setting up attsToKeep
// 				out.Insert(&merged);
// 			}
// 		}
// 	}
// }

// void Join::PipeToFile(Pipe& inPipe, DBFile& outFile) {
// 	int randomStringLen = 8;
// 	char randomString[randomStringLen];
// 	generateRandomString(randomString, randomStringLen); // generate a randomString of length=randomStringLens

// 	std::string fileName("join");
// 	fileName = fileName + randomString + ".bin"; // filename
// 	outFile.Create((char*)fileName.c_str(), heap, NULL);
// 	Record currentRecord;
// 	while (inPipe.Remove(&currentRecord)){
// 		outFile.Add(currentRecord); // add the record to the file
// 	}
// }

// bool FixedSizeRecordBuffer::Add (Record& addme) {
// 	if((size+=addme.GetLength())>capacity){ // if addMe cannot fit in the buffer
// 		return 0; // dont add to the buffer
//   	}
// 	buffer[numRecords++].Consume(&addme); // else add to the buffer
// 	return 1;
// }

// void FixedSizeRecordBuffer::Clear () {
// 	size = 0;
// 	numRecords = 0;
// }

// FixedSizeRecordBuffer::FixedSizeRecordBuffer(int runLength) {
// 	numRecords = 0;
// 	size = 0;
// 	capacity = PAGE_SIZE*runLength; // capacty = page size * runLength
// 	buffer = new Record[PAGE_SIZE*runLength/sizeof(Record*)]; // allocate [(page size * runLength) / size of the record pointer ] bytes for the buffer
// }

// FixedSizeRecordBuffer::~FixedSizeRecordBuffer() {
// 	delete[] buffer; // free the buffer
// }
