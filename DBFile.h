#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

class DBFile {

private:
	// A write buffer page
	Page* bufferPage;
	// The binary file.
	File* heapFile;
	// A read buffer page
	Page* readBuffer;
	// The page number in the read buffer
	off_t curReadPage=0;
	// Indicates any new writes.
	bool dirtyBit = false;
	// Method to write a page to the file.
	void WriteToFile(Page *page);

public:
	DBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
