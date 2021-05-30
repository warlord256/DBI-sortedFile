#ifndef DBFILE_H
#define DBFILE_H

#include "GenericDBFile.h"

typedef struct {OrderMaker *o; int l;} sortParams;

class DBFile {

private:
	GenericDBFile *dbFile;

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
