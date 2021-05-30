#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <vector>

struct operationParams;

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
	pthread_t thread;
	// Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { };

};

class SelectPipe : public RelationalOp {
	private:
	pthread_t thread;
	
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { }
};

class Project : public RelationalOp {
	private:
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { }
};

class Join : public RelationalOp {
	private:
	pthread_t thread;
	int maxPages = DEFAULT_NO_OF_PAGES;
	size_t blockCapacity, curBlockSize;
	int numberOfRecords = 0;

	// The record array to hold a block of records.
	vector<Record *> *recordBlock;

	static void* joinThread(void* params);

	void ClearBlock();

	// Adds the record to the block. Returns false if block is full.
	bool AddRecordToBlock(Record* record);

	//Performs the join of all records in a block and a file.
	void PerformJoin(DBFile &file, Pipe *out, Record *literal, CNF *selOp);
	
	// Performs the nested block join of the records coming in the 2 input pipes.
	void BlockNestedLoopJoin(operationParams *params);
	void SortedBlockJoin(operationParams *params, OrderMaker &orderL, OrderMaker &orderR);
	
	public:
	Join();
	virtual ~Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n);
};

class DuplicateRemoval : public RelationalOp {
	private:
	pthread_t thread;
	int maxPages = DEFAULT_NO_OF_PAGES;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { maxPages = n; }
};

class Sum : public RelationalOp {
	private:
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	private:
	pthread_t thread;
	int maxPages = DEFAULT_NO_OF_PAGES;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { maxPages = n; }
};
class WriteOut : public RelationalOp {
	private:
	pthread_t thread;

	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone () { pthread_join(thread, NULL); }
	void Use_n_Pages (int n) { }
};

typedef struct operationParams {
	operationParams(Pipe *in, Pipe *out, CNF *cnf, Record *literal) {
		this->inputPipe = in;
		this->file = NULL;
		this->outputPipe = out;
		this->literal = literal;
		this->cnf = cnf;
	}
	operationParams(DBFile *file, Pipe *out, CNF *cnf, Record *literal) {
		this->inputPipe = NULL;
		this->file = file;
		this->outputPipe = out;
		this->literal = literal;
		this->cnf = cnf;
	}
	operationParams(Pipe *in, Pipe *out, int *keepMe, int numAttsInput, int numAttsOutput) {
		this->inputPipe = in;
		this->outputPipe = out;
		this->keepMe = keepMe;
		this->numAttsInput = numAttsInput;
		this->numAttsOutput = numAttsOutput;
	}
	operationParams(Pipe *in, FILE *file, Schema *schema) {
		this->inputPipe = in;
		this->outFile = file;
		this->schema = schema;
	}
	operationParams(Pipe *inL, Pipe *inR, Pipe *out, CNF *cnf, Record *literal, Join* obj) {
		this->inputPipe = inL;
		this->inputPipeR = inR;
		this->outputPipe = out;
		this->cnf = cnf;
		this->literal = literal;
		this->joinObject = obj;
	}
	operationParams(Pipe *in, Pipe *out, Schema *schema, int runLen) {
		this->inputPipe = in;
		this->outputPipe = out;
		this->schema = schema;
		this->runLen = runLen;
	}
	operationParams(Pipe *in, Pipe *out, Function* function) {
		this->inputPipe = in;
		this->outputPipe = out;
		this->function = function;
	}
	operationParams(Pipe *in, Pipe *out, OrderMaker *order, Function* function, int runLen) {
		this->inputPipe = in;
		this->outputPipe = out;
		this->order = order;
		this->function = function;
		this->runLen = runLen;
	}
	Pipe *inputPipe, *inputPipeR, *outputPipe;
	Record *literal;
	CNF *cnf;
	DBFile *file;
	int *keepMe;
	int numAttsInput, numAttsOutput;
	int runLen;
	FILE *outFile;
	Schema *schema;
	Join *joinObject;
	Function *function;
	OrderMaker *order;
} operationParams;

#endif
