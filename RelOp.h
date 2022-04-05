#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "Function.h"
#include <vector>

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectPipe : public RelationalOp {
    private:
        pthread_t workerThread;

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class SelectFile : public RelationalOp { 

	private:
		pthread_t workerThread;

	public:
		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};


class Project : public RelationalOp { 
	private:
		pthread_t workerThread;
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class Sum : public RelationalOp {
    private:
        pthread_t workerThread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

void NestedBlockJoin(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe, int runLength);

void LoadVectorFromBlock(vector<Record *> *loadMe, Page *block, int blockLength);

void sortMerge(Pipe *leftInputPipe, Pipe *rightInputPipe, Pipe *outputPipe,
                        OrderMaker *leftOrderMaker, OrderMaker *rightOrderMaker);

void JoinTableBlocks(vector<Record *> *leftBlockRecords, vector<Record *> *rightBlockRecords, Pipe *outputPipe);

class Join : public RelationalOp { 
	private:
		pthread_t workerThread;
		int runLen = 100;
	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
	
};

class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t workerThread;
		int runLen = 100;
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

//void* DuplicateRemovalWorker (void* arg);

typedef struct {
	Pipe *inPipe;
	Pipe *outPipe;
	OrderMaker *order;
	int runLen;
} DuplicateRemovalArg;



class GroupBy : public RelationalOp {
    private:
        pthread_t worker;

	public:
		int use_n_pages = 16;
		void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
	private:
		pthread_t workerThread;
	public:
		void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
	
};

typedef struct {
	Pipe *inPipe;
	FILE *outFile;
	Schema *schema;
} WriteOutArg;

#endif
