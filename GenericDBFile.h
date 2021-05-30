#ifndef GENERIC_DBFILE_H
#define GENERIC_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;
static const string fTypeStrings[] = { "heap", "sorted", "tree" };

class GenericDBFile {
public:
	GenericDBFile() {};
	virtual ~GenericDBFile() {};
	virtual int Create (const char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (const char *fpath) = 0;
	virtual int Close () = 0;

	// Loads all the contents/records of a .tbl file onto the binary file.
	void Load (Schema &myschema, const char *loadpath) {
		FILE *loadFile = fopen (loadpath, "r");
		Record r;
		while(r.SuckNextRecord(&myschema, loadFile)) {
			Add(r);
		}
		fclose(loadFile);
	};

	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};
#endif
