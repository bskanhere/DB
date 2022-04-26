#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "Function.h"
#include <vector>


static char *SUM_ATT_NAME = "SUM";
static Attribute doubleAtt = {SUM_ATT_NAME, Double};
static Schema sumSchema("sum_schema", 1, &doubleAtt);

class RelationalOp {
	public:
	
	virtual void WaitUntilDone () = 0;

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

class Join : public RelationalOp { 
	private:
		pthread_t workerThread;
		int runLen = 16;
	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
	
};

void sortMerge(Pipe *inPipeL, Pipe *inPipeR, Pipe *outPipe, OrderMaker *orderMakerL, OrderMaker *orderMakerR);

void nestedBlock(Pipe *inPipeL, Pipe *inPipeR, Pipe *outPipe, int runLength);

class DuplicateRemoval : public RelationalOp {
	private:
		pthread_t workerThread;
		int runLen = 16;
	public:
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
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

class GroupBy : public RelationalOp {
    private:
        pthread_t worker;
		int runLen = 16;

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
};


#endif
