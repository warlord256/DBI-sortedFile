#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

typedef struct threadParams {
	threadParams(Pipe *in, Pipe *out, OrderMaker *order, int runLen) {
		this->inputPipe = in;
		this->outputPipe = out;
		this->order = order;
		this->runLen = runLen;
	}
	Pipe *inputPipe, *outputPipe;
	int runLen;
	OrderMaker *order;
} threadParams;

class BigQ {
	pthread_t sortingThread;
	
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

void WriteRunToFile(vector<Record *> &records, File *file);
int SplitToRuns(Pipe &in, int runLen, OrderMaker *order, File *file);
void MergeRuns(File *file, int runLen, int runCount, OrderMaker *order, Pipe &out);
void MergeRunsWithoutQueue(File *file, int runLen, int runCount, OrderMaker *order, Pipe &out);
void *TPMMS (void *params);

#endif
