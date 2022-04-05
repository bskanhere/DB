#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
		pthread_t workerThread;

	public:
		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
    private:
        pthread_t workerThread;

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
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

typedef struct {
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
} ProjectArg;

class FixedSizeRecordBuffer {
friend class Join;
public:	
	FixedSizeRecordBuffer(int runLength);
	~FixedSizeRecordBuffer();
private:
  	Record* buffer;
  	int numRecords;
  	int size;
  	int capacity;

	bool Add (Record& addme);
  	void Clear ();
};

class Join : public RelationalOp { 
	private:
		pthread_t workerThread;
		int runLen = 100;
	public:
		void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);
		static void* Operate (void *arg); 
		static void SortMergeJoin(Pipe* leftPipe, OrderMaker* leftOrderMaker, Pipe* rightPipe, OrderMaker* rightOrderMaker, Pipe* pout, CNF* selOp, Record* literal, int runLength);
		static void NestedLoopJoin(Pipe* leftPipe, Pipe* rightPipe, Pipe* pout, CNF* selOp, Record* literal, int runLength);
		static void JoinBufferWithFile(FixedSizeRecordBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp);
		static void PipeToFile(Pipe& inPipe, DBFile& outFile);
	
};

typedef struct {
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	int runLen;
} JoinArg;

void* JoinWorker (void* arg);

void JoinWorker_AddMergedRecord(Record* leftRecord, Record* rightRecord, Pipe* pipe);

void JoinWorker_Merge(JoinArg* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder);

void JoinWorker_BlockNested(JoinArg* joinArg);


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


class Sum : public RelationalOp {
    private:
        pthread_t workerThread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

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