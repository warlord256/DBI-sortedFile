#include <vector>
#include "BigQ.h"
#include "RelOp.h"

void* selectFileThread(void *params) {
	operationParams *p = (operationParams *)params;
	
	Record r;
	p->file->MoveFirst();

	while(p->file->GetNext(r, *(p->cnf), *(p->literal))) {
		p->outputPipe->Insert(&r);
	}
	p->outputPipe->ShutDown();
	delete p;
	pthread_exit(NULL);
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	operationParams *params = new operationParams(&inFile, &outPipe, &selOp, &literal);
	pthread_create(&this->thread, NULL, selectFileThread, (void *) params);
}

void *selectPipeThread (void *params) {
	operationParams *p = (operationParams *)params;

	ComparisonEngine ce;
	Record r;
	while(p->inputPipe->Remove(&r)) {
		if(ce.Compare(&r, p->literal, p->cnf)) {
			p->outputPipe->Insert(&r);
		}
	}

	p->outputPipe->ShutDown();
	delete p;
	pthread_exit(NULL);
}

void SelectPipe :: Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	operationParams *params = new operationParams(&inPipe, &outPipe, &selOp, &literal);
	pthread_create(&this->thread, NULL, selectPipeThread, (void *) params);
}

void *projectThread (void *params) {
	operationParams *p = (operationParams *)params;

	Record r;
	while(p->inputPipe->Remove(&r)) {
		r.Project(p->keepMe, p->numAttsOutput, p->numAttsInput);
		p->outputPipe->Insert(&r);
	}

	p->outputPipe->ShutDown();
	delete p;
	pthread_exit(NULL);
}

void Project :: Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	operationParams *params = new operationParams(&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput);
	pthread_create(&this->thread, NULL, projectThread, (void *) params);
}

Join :: Join() {
	blockCapacity = PAGE_SIZE; 
	recordBlock = new vector<Record *>();
}

Join :: ~Join() {
	DeleteVectorOfPointers(recordBlock);
}

void Join :: Use_n_Pages (int n) {
	DeletePointersInVector(recordBlock);
	blockCapacity = PAGE_SIZE*n;
}

bool Join :: AddRecordToBlock(Record* record) {
	size_t size = record->Size();
	if(curBlockSize+size < blockCapacity) {
		curBlockSize += size;
		Record *r = new Record();
		r->Consume(record);
		(*recordBlock)[numberOfRecords++] = r;
		return true;
	}
	return false;
}

void Join :: PerformJoin(DBFile &file, Pipe *out, Record *literal, CNF *selOp) {
	
	Record r, joinedRecord;
	ComparisonEngine ce;

	for(int i=0;i<numberOfRecords;i++) {
		file.MoveFirst();
		while(file.GetNext(r)) {
			if(ce.Compare((*recordBlock)[i], &r, literal, selOp)) {
				// Consumes the right record.
				joinedRecord.MergeRecords((*recordBlock)[i], &r);
				out->Insert(&joinedRecord);
			}
		}
	}
	
}

void Join :: BlockNestedLoopJoin(operationParams *params) {
	Pipe *inL = params->inputPipe;
	Pipe *inR = params->inputPipeR;
	Pipe *out = params->outputPipe;
	
	Record l, r;

	// First dump the contents of the right pipe into a file to access repeatedly.
	DBFile file;

	// Generate filename.
	string tempFileName;
	generateRandomString(10, tempFileName);
	tempFileName.append(".tmp");

	// Create the temp file.
	file.Create(tempFileName.c_str(), heap, NULL);
	
	// Dump records into file.
	while(inR->Remove(&r)) {
		file.Add(r);
	}

	// Read blocks of left pipe and perform join of the block over the entire tempFile created.
	while(inL->Remove(&l)) {
		if(!AddRecordToBlock(&l)) {
			PerformJoin(file, out, params->literal, params->cnf);
			ClearBlock();
			AddRecordToBlock(&l);
		}
	}
	PerformJoin(file, out, params->literal, params->cnf);

	// Done with temp file.
	file.Close();
	// Delete temp file.
	remove(tempFileName.c_str());

}

void Join :: SortedBlockJoin(operationParams *params, OrderMaker &orderL, OrderMaker &orderR) {
	Pipe *sortedL = new Pipe(PIPE_BUFFER_SIZE);
	Pipe *sortedR = new Pipe(PIPE_BUFFER_SIZE);

	// TODO: May have to decrease from maxPages to maxPages/2 since 2 threads will run in parallel.
	BigQ qL(*(params->inputPipe), *sortedL, orderL, maxPages);
	BigQ qR(*(params->inputPipeR), *sortedR, orderR, maxPages);

	Record *literal = params->literal;
	CNF *selOp = params->cnf;
	Pipe *out = params->outputPipe;

	Record r, l;
	ComparisonEngine ce;

	bool leftHasMore =  sortedL->Remove(&l);
	bool rightHasMore = sortedR->Remove(&r);

	while(leftHasMore && rightHasMore) {
		int res = ce.Compare(&l, &orderL, &r, &orderR);
		if(res==0) {
			// Join condition satisfied.
			ClearBlock();
			// Load all matching right tuples into the block.
			// TODO: Handle if the number of tuples goes beyond the allowed number of pages.
			recordBlock->push_back(new Record());
			(*recordBlock)[numberOfRecords]->Consume(&r);
			Record *start = (*recordBlock)[numberOfRecords];
			numberOfRecords++;
			rightHasMore = sortedR->Remove(&r);
			while(rightHasMore && ce.Compare(&r, start, &orderR)==0) {
				recordBlock->push_back(new Record());
				(*recordBlock)[numberOfRecords++]->Consume(&r);
				rightHasMore = sortedR->Remove(&r);
			}

			// For each left record that matches the push it to output.
			Record joinedRecord, temp;

			while (leftHasMore && ce.Compare(start, &orderR, &l, &orderL)==0) {
				for(int i=0;i<numberOfRecords;i++) {
					if(ce.Compare(&l, (*recordBlock)[i], literal, selOp)) {
						// Copy the record as it will be consumed by merge.
						temp.Copy((*recordBlock)[i]);
						joinedRecord.MergeRecords(&l, &temp);
						out->Insert(&joinedRecord);
					}
				}
				leftHasMore = sortedL->Remove(&l);
			}
			// Done with the elements of the block.
			ClearBlock();

		} else if (res < 0) {
			leftHasMore =  sortedL->Remove(&l);
		} else {
			rightHasMore = sortedR->Remove(&r);
		}
	}

	// Don't Free the pipe memory. // Done by deconstructor
	// delete sortedL, sortedR;//, qL, qR;
}

void* Join :: joinThread (void *params) {
	operationParams *p = (operationParams *)params;

	Pipe *inL = p->inputPipe;
	Pipe *inR = p->inputPipeR;
	OrderMaker orderL, orderR;
	if(!p->cnf->GetSortOrders(orderL, orderR)) {
		// No matches, perform block nested join.
		p->joinObject->BlockNestedLoopJoin(p);
	} else {
		// There was a match perform sorted join.
		p->joinObject->SortedBlockJoin(p, orderL, orderR);
	}
	// End thread.
	p->outputPipe->ShutDown();
	delete p;
	pthread_exit(NULL);
}

void Join :: Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	operationParams *params = new operationParams(&inPipeL, &inPipeR, &outPipe, &selOp, &literal, this);
	pthread_create(&this->thread, NULL, joinThread, (void *) params);
}

void Join :: ClearBlock() {
	curBlockSize = 0;
	numberOfRecords = 0;
	DeletePointersInVector(recordBlock);
	recordBlock->clear();
}

void* writeoutThread (void* params) {
	operationParams *p = (operationParams *)params;

	Record r;
	while(p->inputPipe->Remove(&r)) {
		r.WriteToFile(p->outFile, p->schema);
	}

	delete p;
	pthread_exit(NULL);
}

void WriteOut :: Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	operationParams *params = new operationParams(&inPipe, outFile, &mySchema);
	pthread_create(&this->thread, NULL, writeoutThread, (void *) params);
}

void* duplicatesThread(void *params) {
	operationParams *p = (operationParams *)params;

	OrderMaker order(p->schema);
	Pipe *sortedIn = new Pipe(PIPE_BUFFER_SIZE);
	BigQ q(*(p->inputPipe), *sortedIn, order, p->runLen);
	Record r, prev;
	ComparisonEngine ce;
	Pipe *out = p->outputPipe;
	
	if(sortedIn->Remove(&prev)) {
		while(sortedIn->Remove(&r)) {
			if(ce.Compare(&prev, &r, &order)) {
				out->Insert(&prev);
				prev.Consume(&r);
			}
		}
		out->Insert(&prev);
	}
	

	out->ShutDown();
	delete sortedIn, p;
	pthread_exit(NULL);
} 

void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	operationParams *params = new operationParams(&inPipe, &outPipe, &mySchema, maxPages);
	pthread_create(&this->thread, NULL, duplicatesThread, (void *) params);
}

void* sumThread(void *params) {
	operationParams *p = (operationParams *)params;
	Pipe *in = p->inputPipe;
	Function *function = p->function;
	Record r, res;
	int intRes = 0;
	double doubleRes;
	Type type;

	// The above vars just holds results of a single tuple, so we have to sum.
	int intSum = 0 ;
	double doubleSum = 0.0;

	// Apply the function for all tuples.
	while(in->Remove(&r)) {
		type = function->Apply(r, intRes, doubleRes);

		// Add the result to the sum;
		if(type == Int) {
			intSum += intRes;
		} else if(type == Double) {
			doubleSum += doubleRes;
		}
	}

	// Check type and create record.
	res.ComposeRecord(type, intSum, doubleSum);

	// Push to output
	p->outputPipe->Insert(&res);
	
	// End thread
	p->outputPipe->ShutDown();
	delete p;
	pthread_exit(NULL);
}

void Sum :: Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	operationParams *params = new operationParams(&inPipe, &outPipe, &computeMe);
	pthread_create(&this->thread, NULL, sumThread, (void *) params);
}

void* groupThread(void* params) {
	operationParams *p = (operationParams *)params;

	Pipe *sortedIn = new Pipe(PIPE_BUFFER_SIZE);
	Pipe *out = p->outputPipe;
	OrderMaker *order = p->order;
	Function *function  = p->function;

	BigQ q(*(p->inputPipe), *sortedIn, *order, p->runLen);

	ComparisonEngine ce;
	Record prev, r, mergedRecord, res;
	Type type;

	int intRes, intSum;
	double doubleRes, doubleSum;

	if(sortedIn->Remove(&prev)){		
		type = function->Apply(prev, intRes, doubleRes);
		intSum = intRes;
		doubleSum = doubleRes;
		while(sortedIn->Remove(&r)) {
			if(ce.Compare(&prev, &r, order)) {
				// Create the new record with all elemnts in the OrderMaker and the sum.
				res.ComposeRecord(type, intSum, doubleSum);
				mergedRecord.ComposeGroupedRecord(&res, &prev, order);
				out->Insert(&mergedRecord);
				prev.Consume(&r);

				// Reset the sum
				type = function->Apply(prev, intRes, doubleRes);
				intSum = intRes;
				doubleSum = doubleRes;
			} else {
				// Add to the current ongoing sum
				type = function->Apply(r, intRes, doubleRes);
				intSum += intRes;
				doubleSum += doubleRes;
			}
		}
		// Determine type and push final record.
		res.ComposeRecord(type, intSum, doubleSum);
		mergedRecord.ComposeGroupedRecord(&res, &prev, order);
		out->Insert(&mergedRecord);

		// End thread
		out->ShutDown();
		delete sortedIn, p;
		pthread_exit(NULL);
	}
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	operationParams *params = new operationParams(&inPipe, &outPipe, &groupAtts, &computeMe, maxPages);
	pthread_create(&this->thread, NULL, groupThread, (void *) params);
}
