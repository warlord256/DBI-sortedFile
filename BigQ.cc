#include "BigQ.h"

void generateRandomString(int size, string &s) {
    srand(time(NULL)); // seed with time
	static const string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	
    size = (rand() % size) + 1; // this randomises the size

    while(--size > -1)
        s += str[(rand() % str.length())]; // generate a string ranging from the space character to ~ (tilde)

}

// Writes the sorted run into the file.
void WriteRunToFile(vector<Record *> &records, File *file) {
	Page *buffer = new Page();
	for(int i=0;i<records.size();i++) {
		if(buffer->Append(records[i])==0) {
			// Page full write the page and empty it.
			WritePageToFile(file, buffer);
			buffer->Append(records[i]);
		}
	}
	WritePageToFile(file, buffer);
	records.clear();
}

// Splits the incoming data into runs and stores it into the file.
int SplitToRuns(Pipe &in, int runLen, OrderMaker *order, File *file) {
	// Keep a fourth of a page as a buffer so that it dosent overflow.
	int maxRunSize = (PAGE_SIZE)* runLen - (PAGE_SIZE/4);
	int curRunSize = 0;
	int runCount = 0;
	vector<Record *> *records = new vector<Record *>();
	// Open temporary file with random name.
	string s;
	generateRandomString(8, s);
	s.append(".tmp");
	file->Open(0, (char*)s.c_str());
	Record *temp = new Record();
	ComparisonEngine ce;
	auto compareLambda = [&ce, &order] (Record *a, Record *b) {return ce.Compare(a, b, order)<0;};
	while(in.Remove(temp)) {
		int size = temp->Size();
		if(maxRunSize < curRunSize+size) {
			// Write run to file.
			sort(records->begin(), records->end(), compareLambda);
			WriteRunToFile(*records, file);
			curRunSize = 0;
			runCount++;
		}
		records->push_back(temp);
		curRunSize += size;
		temp = new Record();
	}
	// Write final run to file.
	sort(records->begin(),records->end(), compareLambda);
	WriteRunToFile(*records, file);
	curRunSize = 0;
	runCount++;
	return runCount;
}

void MergeRuns(File *file, int runLen, int runCount, OrderMaker *order, Pipe &out) {
	ComparisonEngine ce;
	auto compare = [&ce, &order] (pair<Record *, int> a, pair<Record *, int> b) {
		return ce.Compare(b.first, a.first, order)<0;
	};
	priority_queue<pair<Record *, int>, vector<pair<Record *, int>>, decltype(compare)> pq(compare);
	vector<Page*>*pages = new vector<Page*>(runCount, NULL);
	vector<int>relativeIndex(runCount,0);
	for(int i=0, j=0;i<runCount && j<file->GetLength();i++, j+=runLen) {
		Page *temp = new Page();
		file->GetPage(temp, j);
		(*pages)[i] = temp;
	}

	for(int i=0;i<runCount;i++) {
		Record *temp = new Record();
		 (*pages)[i]->GetFirst(temp);
		pq.push({temp, i});
	}

	while(!pq.empty()) {
		pair<Record*, int> p = pq.top();
		pq.pop();
		out.Insert(p.first);
		int index = p.second;
		Record *r = new Record();
		 if((*pages)[index]->GetFirst(r)==0) {
			relativeIndex[index]++;
			int pageNo = (index * runLen) + relativeIndex[index];
			if(relativeIndex[index] >= runLen || pageNo >= file->GetLength()-1) {
				// There are no more pages for the run
				continue;
			}
			// Get the next page in the run.
			 file->GetPage((*pages)[index], pageNo);
			 (*pages)[index]->GetFirst(r);
		}
		// Push new record into pq.
		pq.push({r, index});
	}
}

void MergeRunsWithoutQueue(File *file, int runLen, int runCount, OrderMaker *order, Pipe &out) {
	ComparisonEngine ce;
	vector <Record *> *records = new vector<Record *>(runCount);
	vector<Page*>*pages = new vector<Page*>(runCount, NULL);
	vector<int>relativeIndex(runCount,0);
	for(int i=0, j=0;i<runCount && j<file->GetLength();i++, j+=runLen) {
		Page *temp = new Page();
		file->GetPage(temp, j);
		(*pages)[i] = temp;
	}

	
	for(int i=0;i<runCount;i++) {
		Record *temp = new Record();
		(*pages)[i]->GetFirst(temp);
		(*records)[i]=temp;
	}
	while(!records->empty()) {
		Record *min = (*records)[0];
		int index = 0;
		for(int i=1;i<records->size();i++){
			if((*records)[i]==NULL) {
				continue;
			}
			if(min==NULL || ce.Compare((*records)[i], min, order)<0) {
				min = (*records)[i];
				index = i;
			}
		}
		if(min==NULL) {
			return;
		}
		out.Insert(min);
		Record *r = new Record();
		 if((*pages)[index]->GetFirst(r)==0) {
			relativeIndex[index]++;
			int pageNo = (index * runLen) + relativeIndex[index];
			if(relativeIndex[index] >= runLen || pageNo >= file->GetLength()-1) {
				// There are no more pages for the run
				(*records)[index]=NULL;
				continue;
			}
			// Get the next page in the run.
			 file->GetPage((*pages)[index], pageNo);
			 (*pages)[index]->GetFirst(r);
		}
		// Push new record into pq.
		(*records)[index]=r;
	}
}

void *TPMMS (void *params) {
	threadParams *p = (threadParams *)params;
	File *file = new File();
	// read data from in pipe sort them into runlen pages
	int runCount = SplitToRuns(*p->inputPipe, p->runLen, p->order, file);
    // construct priority queue over sorted runs and dump sorted data into the out pipe
	MergeRuns(file, p->runLen, runCount, p->order, *p->outputPipe);
    // finally shut down the out pipe
	p->outputPipe->ShutDown();
	file->Close();
	return NULL;
}

BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	threadParams *params = new threadParams(&in, &out, &sortorder, runlen);
	pthread_create(&this->sortingThread, NULL, TPMMS, (void *) params);
}

BigQ::~BigQ () {
}