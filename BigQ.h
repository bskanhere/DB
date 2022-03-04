#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class BigQ {

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

};

//Class run represent run used for merging
class Run {

public:
	Run(File* file, int startPageIndex, int runLength);
	int getNextRecord();
	Record *currRecord; 

private: 
	File* runFile;
	Page bufferPage;
	int currPageIndex;
	int maxPageIndex;
};

class RecordComparator {

public:
	bool operator() (Record* left, Record* right);
	RecordComparator(OrderMaker *order);

private:
	OrderMaker *order;
	ComparisonEngine comparisonEngine;
};

class RunComparator {

public:
	bool operator() (Run* left, Run* right);
	RunComparator(OrderMaker *order);

private:
	OrderMaker *order;
	ComparisonEngine comparisonEngine;
};

//Struct used as arguement for worker thread
typedef struct {
	
	Pipe *in;
	Pipe *out;
	OrderMaker *order;
	int runlen;
	
} WorkerThreadArgs;

//Main method executed by worker, worker will retrieve records from input pipe, 
//sort records into runs and puting all runs into priority queue, and geting sorted reecords
//from priority queue to output pipe
void* TPMMSAlgo(void* arg);

//Used for take sequences of pages of records, and construct a run to hold such records, and put run
//into priority queue
void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordComparator>& recordQueue, 
    priority_queue<Run*, vector<Run*>, RunComparator>& runQueue, File& file, Page& bufferPage, int& pageIndex);

#endif
