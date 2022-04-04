#include "RelOp.h"
#include <iostream>
#include "BigQ.h"


//SelectFile
typedef struct {
    DBFile &inFile;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg1;

void* selectFileWorker(void* arg) {
    WorkerArg1* workerArg = (WorkerArg1*) arg;
    Record rec;
    while(workerArg->inFile.GetNext(rec, workerArg->selOp, workerArg->literal)) {
        workerArg->outPipe.Insert(&rec);
    }
    workerArg->outPipe.ShutDown();
    return NULL;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    WorkerArg1* workerArg = new WorkerArg1{inFile, outPipe, selOp, literal};
    pthread_create(&workerThread, NULL, selectFileWorker, (void*) workerArg);
}

void SelectFile::WaitUntilDone() {
	pthread_join(workerThread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}


//SelectPipe
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    CNF &selOp;
    Record &literal;
} WorkerArg2;

void* selectPipeWorker(void*arg) {
    ComparisonEngine comparisonEngine;
    WorkerArg2* workerArg = (WorkerArg2*) arg;
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
    WorkerArg2* workerArg = new WorkerArg2{inPipe, outPipe, selOp, literal};
    pthread_create(&workerThread, NULL, selectPipeWorker, (void*) workerArg);
}

void SelectPipe::WaitUntilDone () {
	pthread_join(workerThread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen) {

}


//Project
void* projectWorker (void* arg) {
    ProjectArg* projectArg = (ProjectArg*) arg;
    Record rec;
    while (projectArg->inPipe->Remove(&rec) == 1) {
        Record* temp = new Record;
        temp->Consume(&rec);
        temp->Project(projectArg->keepMe, projectArg->numAttsOutput, projectArg->numAttsInput);
        projectArg->outPipe->Insert(temp);     
    }
    projectArg->outPipe->ShutDown();
    return NULL;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    ProjectArg* projectArg = new ProjectArg;
    projectArg->inPipe = &inPipe;
    projectArg->outPipe = &outPipe;
    projectArg->keepMe = keepMe;
    projectArg->numAttsInput = numAttsInput;
    projectArg->numAttsOutput = numAttsOutput;
    pthread_create(&workerThread, NULL, projectWorker, (void*) projectArg);
}

void Project::WaitUntilDone() {
    pthread_join(workerThread, NULL);
}

void Project::Use_n_Pages(int n) {

}


//Sum
typedef struct {
    Pipe &inPipe;
    Pipe &outPipe;
    Function &computeMe;
} WorkerArg3;

void* sumWorker(void*arg){
    
    int intSum = 0, intResult = 0;
    double doubleSum = 0.0, doubleResult = 0.0;
    ComparisonEngine comp;
    WorkerArg3* workerArg = (WorkerArg3*) arg;
    Record rec;
    Type t;

    while(workerArg->inPipe.Remove(&rec)){
        cout << "In Sum" << endl;
        t = workerArg->computeMe.Apply(rec, intResult, doubleResult);
        if(t==Int){
            intSum += intResult;
        }
        else{
            doubleSum += doubleResult;
        }
    }

    Attribute DA = {"SUM", t};
    Schema out_sch ("out_sch", 1, &DA);
    Record res;
    char charsRes[100];
    if(t==Int){
        sprintf(charsRes, "%d|", intSum);
    }
    else{
        sprintf(charsRes, "%lf|", doubleSum);
    }
    res.ComposeRecord(&out_sch, charsRes);
    workerArg->outPipe.Insert(&res);
    workerArg->outPipe.ShutDown();
    return NULL;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    WorkerArg3* workerArg = new WorkerArg3{inPipe, outPipe, computeMe};
    pthread_create(&workerThread, NULL, sumWorker, (void*) workerArg);
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
    
    //Schema schema ("catalog", "partsupp");
    
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
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    //Construct parameters and send them to main worker
    JoinArg* joinArg = new JoinArg;
    joinArg->inPipeL = &inPipeL;
    joinArg->inPipeR = &inPipeR;
    joinArg->outPipe = &outPipe;
    joinArg->selOp = &selOp;
    joinArg->literal = &literal;
    joinArg->runLen = this->runLen;
    pthread_create(&workerThread, NULL, JoinWorker, (void*) joinArg);
}

void* JoinWorker(void* arg) {
	#ifdef F_DEBUG
		std::cout<<"Join Thread started"<<std::this_thread::get_id()<<endl;
	#endif
	try{
        cout<<"Here"<<endl;
		JoinArg* arg = (JoinArg*)(arg); 
		OrderMaker orderL;
		OrderMaker orderR;
		arg->selOp->GetSortOrders(orderL, orderR);
		if(orderL.numAtts && orderR.numAtts && orderL.numAtts == orderR.numAtts) {
			Pipe *pipeL=new Pipe(100), *pipeR=new Pipe(100);
			BigQ *bigQL = new BigQ(*(arg->inPipeL), *pipeL, orderL, arg->runLen);
			BigQ *bigQR = new BigQ(*(arg->inPipeR), *pipeR, orderR, arg->runLen);
			vector<Record *> vecL;
			Record *rcdLeft = new Record();

			vector<Record *> vecR;
			Record *rcdRight = new Record();
			
			ComparisonEngine comp;
			if(pipeL->Remove(rcdLeft) && pipeR->Remove(rcdRight)) {
				int lAttr = ((int *) rcdLeft->bits)[1] / sizeof(int) -1;
				int rAttr = ((int *) rcdRight->bits)[1] / sizeof(int) -1;
				int totAttr = lAttr + rAttr;
				int attrToKeep[totAttr];
				for(int i = 0; i< lAttr; i++)
					attrToKeep[i] = i;
				for(int i = 0; i< rAttr; i++)
					attrToKeep[i+lAttr] = i;
				int joinNum;
				bool leftOK=true, rightOK=true;
				int num  =0;
				while(leftOK && rightOK) {
					leftOK=false; rightOK=false;
					int cmpRst = comp.Compare(rcdLeft, &orderL, rcdRight, &orderR);
					switch(cmpRst) {
						case 0:{
							num ++;
							Record *rcd1 = new Record(); 
							rcd1->Consume(rcdLeft);
							Record *rcd2 = new Record(); 
							rcd2->Consume(rcdRight);
							vecL.push_back(rcd1);
							vecR.push_back(rcd2);
							while(pipeL->Remove(rcdLeft)) {
								if(!comp.Compare(rcdLeft, rcd1, &orderL)) {
									Record *cLMe = new Record();
									cLMe->Consume(rcdLeft);
									vecL.push_back(cLMe);
								} else {
									leftOK = true;
									break;
								}
							}
							while(pipeR->Remove(rcdRight)) {
								if(!comp.Compare(rcdRight, rcd2, &orderR)) {
									Record *cRMe = new Record();
									cRMe->Consume(rcdRight);
									vecR.push_back(cRMe);
								} 
								else {
									rightOK = true;
									break;
								}
							}
							Record *lr = new Record(), *rr=new Record(), *jr = new Record();
							for(auto itL :vecL) {
								lr->Consume(itL);
								for(auto itR: vecR) {
									if(comp.Compare(lr, itR, arg->literal, arg->selOp)) {
										joinNum++;
										rr->Copy(itR);
										jr->MergeRecords(lr, rr, lAttr, rAttr, attrToKeep, lAttr+rAttr, lAttr);
										arg->outPipe->Insert(jr);
									}
								}
							}
							for(auto it:vecL)
								if(!it)
									delete it; 
							vecL.clear();
							for(auto it : vecR)
								if(!it)
									delete it;
							vecR.clear();
							break;
					}
					case 1:
						leftOK = true;
						if(pipeR->Remove(rcdRight))
							rightOK = true;
						break;
					case -1:
						rightOK = true;
						if(pipeL->Remove(rcdLeft))
							leftOK = true;
						break;
					}
				}
			}
		} 
		else 
		{
			int n_pages = 10;
			Record *rcdLeft = new Record;
			Record *rcdRight = new Record;
			Page pageR;
			DBFile dbFileL;
				fType ft = heap;
				dbFileL.Create((char*)"tmpL", ft, NULL);
				dbFileL.MoveFirst();

			int leftAttr, rightAttr, totalAttr, *attrToKeep;

			if(arg->inPipeL->Remove(rcdLeft) && arg->inPipeR->Remove(rcdRight)) {
				leftAttr = ((int *) rcdLeft->bits)[1] / sizeof(int) -1;
				rightAttr = ((int *) rcdRight->bits)[1] / sizeof(int) -1;
				totalAttr = leftAttr + rightAttr;
				attrToKeep = new int[totalAttr];
				for(int i = 0; i< leftAttr; i++)
					attrToKeep[i] = i;
				for(int i = 0; i< rightAttr; i++)
					attrToKeep[i+leftAttr] = i;
				do {
					dbFileL.Add(*rcdLeft);
				}while(arg->inPipeL->Remove(rcdLeft));
				vector<Record *> vecR;
				ComparisonEngine comp;

				bool rMore = true;
				int joinNum =0;
				while(rMore) {
					Record *first = new Record();
					first->Copy(rcdRight);
					pageR.Append(rcdRight);
					vecR.push_back(first);
					int rPages = 0;
					rMore = false;
					while(arg->inPipeR->Remove(rcdRight)) {
						Record *copyMe = new Record();
						copyMe->Copy(rcdRight);
						if(!pageR.Append(rcdRight)) {
							rPages += 1;
							if(rPages >= n_pages -1) {
								rMore = true;
								break;
							}
							else {
								pageR.EmptyItOut();
								pageR.Append(rcdRight);
								vecR.push_back(copyMe);
							}
						} else {
							vecR.push_back(copyMe);
						}
					}
					dbFileL.MoveFirst();
					int fileRN = 0;
					while(dbFileL.GetNext(*rcdLeft)) {
						for(auto it:vecR) {
							if(comp.Compare(rcdLeft, it, arg->literal, arg->selOp)) {
								joinNum++;
								Record *joinRec = new Record();
								Record *rightRec = new Record();
								rightRec->Copy(it);
								joinRec->MergeRecords(rcdLeft, rightRec, leftAttr, rightAttr, attrToKeep, leftAttr+rightAttr, leftAttr);
								arg->outPipe->Insert(joinRec);
							}
						}
					}
					for(auto it : vecR)
						if(!it)
							delete it;
					vecR.clear();
				}
				dbFileL.Close();
			}
		}
		arg->outPipe->ShutDown();
	}
	catch(std::exception e){
		std::cout<<"Exception in Join Thread\n";
	}
	#ifdef F_DEBUG
		std::cout<<"Join Thread Closed"<<std::this_thread::get_id()<<endl;
	#endif
 }

// void* JoinWorker(void* arg) {
//     JoinArg* joinArg = (JoinArg*) arg;
//     OrderMaker leftOrder, rightOrder;
//     joinArg->selOp->GetSortOrders(leftOrder, rightOrder);
//     //Decide to sort join merge or blocknested join
//     if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
//         cout << "Enter sort merge " << endl;
//         JoinWorker_Merge(joinArg, &leftOrder, & rightOrder);
//     }
//     else {
//         cout << "BlockNestJoin" << endl;
//         JoinWorker_BlockNested(joinArg);
//     }
//     joinArg->outPipe->ShutDown();
//     return NULL;
// }

//This method is used to merge two records into single record
void JoinWorker_AddMergedRecord(Record* leftRecord, Record* rightRecord, Pipe* pipe) {
    cout<<"Initiate Merge"<<endl;
    int numOfAttsLeft = ((((int*) leftRecord->bits)[1]) / sizeof(int)) - 1;
    int numOfAttsRight = ((((int*) rightRecord->bits)[1]) / sizeof(int)) - 1;
    int* attsToKeep = new int[numOfAttsLeft + numOfAttsRight];
    for (int i = 0; i < numOfAttsLeft; i++)
        attsToKeep[i] = i;
    for (int i = numOfAttsLeft; i < numOfAttsLeft + numOfAttsRight; i++)
        attsToKeep[i] = i - numOfAttsLeft;
    Record mergedRecord;
    mergedRecord.MergeRecords(leftRecord, rightRecord, numOfAttsLeft, numOfAttsRight, attsToKeep, numOfAttsLeft + numOfAttsRight, numOfAttsLeft);
    pipe->Insert(&mergedRecord);
    cout<<"Record Inserted"<<endl;
}

//Sort merge Join
void JoinWorker_Merge(JoinArg* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder) {
    //First using BigQ to sort given records in two pipes
    Pipe* sortedLeftPipe = new Pipe(1000);
    Pipe* sortedRightPipe = new Pipe(1000);
    BigQ* tempL = new BigQ(*(joinArg->inPipeL), *sortedLeftPipe, *leftOrder, joinArg->runLen);
    BigQ* tempR = new BigQ(*(joinArg->inPipeR), *sortedRightPipe, *rightOrder, joinArg->runLen);
    cout << "BigQ created" << endl;
    Record leftRecord;
    Record rightRecord;
    bool isFinish = false;
    if (sortedLeftPipe->Remove(&leftRecord) == 0)
        isFinish = true;
    if (sortedRightPipe->Remove(&rightRecord) == 0)
        isFinish = true;
    cout << "BigQ outputed" << endl;
    ComparisonEngine comparisonEngine;
    //Then do the merge part, merge same record together to join
    while (!isFinish) {
        cout<<"In Step" << endl;
        int compareRes = comparisonEngine.Compare(&leftRecord, leftOrder, &rightRecord, rightOrder);
        //If left record equal to right record, we merge them together and insert it into output pipe
        if (compareRes == 0) {
            vector<Record*> vl;
            vector<Record*> vr;
            //Find all idential and continuous records in the left pipe and put them into vector
            while (true) {
                Record* oldLeftRecord = new Record;
                oldLeftRecord->Consume(&leftRecord);
                vl.push_back(oldLeftRecord);
                if (sortedLeftPipe->Remove(&leftRecord) == 0) {
                    isFinish = true;
                    break;
                }
                if (comparisonEngine.Compare(&leftRecord, oldLeftRecord, leftOrder) != 0) {
                    break;
                }
            }
            //Find all idential and continuous records in the right pipe and put them into vector
            while (true) {
                Record* oldRightRecord = new Record;
                oldRightRecord->Consume(&rightRecord);
                // oldRightRecord->Print(&partsupp);
                vr.push_back(oldRightRecord);
                if (sortedRightPipe->Remove(&rightRecord) == 0) {
                    isFinish = true;
                    break;
                }
                if (comparisonEngine.Compare(&rightRecord, oldRightRecord, rightOrder) != 0) {
                    break;
                }
            }
            cout<< "Merging Records"<<endl;
            //Merge every part of them, and join
            for (int i = 0; i < vl.size(); i++) {
                for (int j = 0; j < vr.size(); j++) {
                    JoinWorker_AddMergedRecord(vl[i], vr[j], joinArg->outPipe);
                }
            }
            cout<<"Merging Complete"<<endl;
            vl.clear();
            vr.clear();
            cout<<"Buffer Cleared"<<endl;
        }
        //If left reocrd are larger, then we move right record
        else if (compareRes > 0) {
            if (sortedRightPipe->Remove(&rightRecord) == 0)
                isFinish = true;
        }
        //If right record are larger, then we move left record
        else {
            if (sortedLeftPipe->Remove(&leftRecord) == 0)
                isFinish = true;
        }
    }
    cout << "Finish read fron sorted pipe" << endl;
    while (sortedLeftPipe->Remove(&leftRecord) == 1);
    while (sortedRightPipe->Remove(&rightRecord) == 1);
}

//This is block nested join on default
void JoinWorker_BlockNested(JoinArg* joinArg) {
    //Literlly join every pair of records.
    DBFile tempFile;
    char* fileName = new char[100];
    sprintf(fileName, "BlockNestedTemp%d.bin", pthread_self());
    tempFile.Create(fileName, heap, NULL);
    tempFile.Open(fileName);
    Record record;
    while (joinArg->inPipeL->Remove(&record) == 1)
        tempFile.Add(record);
    
    Record record1, record2;
    ComparisonEngine comparisonEngine;
    while (joinArg->inPipeR->Remove(&record1) == 1) {
        tempFile.MoveFirst();
        while (tempFile.GetNext(record) == 1) {
            if (comparisonEngine.Compare(&record1, &record2, joinArg->literal, joinArg->selOp)) {
                JoinWorker_AddMergedRecord(&record1, &record2, joinArg->outPipe);
            }
        }
    }
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
