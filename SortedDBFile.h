#include "GenericDBFile.h"
#include "BigQ.h"

class SortedDBFile : public GenericDBFile {

private:
    // Indicates the mode (read or write).
	bool dirtyBit = false;
	bool inOrder = false;
    // For sorting
    BigQ *bigQ;
    int runLen;
    OrderMaker *order;
	Pipe *in;
	Pipe *out;
	// The binary file.
	File* file;
	// The read buffer
	Page* readBuffer;
	int curReadPage;

	// Store the file name.
	char * openedFileName;

	// query OrderMaker.
	OrderMaker *queryOrderMaker;
	OrderMaker *cnfOrderMaker;


	bool BinarySearch(Record &fetchMe, CNF &cnf, Record &literal, OrderMaker *cnfOrder); 
	void WriteToFile();

public:
	SortedDBFile(OrderMaker *order, int runLen);
	~SortedDBFile() {};
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
