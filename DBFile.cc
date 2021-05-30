#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile::DBFile () {
    // Initialize the write buffer
    bufferPage = new Page();
    // initialize the read buffer
    readBuffer = new Page();
    // initialize binary db file
    heapFile = new File();
}

// Creates and opens the file specified.   
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type == heap) {
        cout << "Creating new file at " << f_path << endl; 
        heapFile->Open(0, (char*)f_path);
        return 1;
    }
    return 0;
}

// Loads all the contents/records of a .tbl file onto the binary file. 
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *loadFile = fopen (loadpath, "r");
    Record r;
    while(r.SuckNextRecord(&f_schema, loadFile)) {
        Add(r);
    }
}

// Opens an existing binary file to perform operations.(Need not be used if create was called before for the same file)
int DBFile::Open (const char *f_path) {
    heapFile->Open(1, (char *) f_path);
    return 1;
}

// Loads up the first page from the file onto the read buffer.
void DBFile::MoveFirst () {
    if(dirtyBit)
        WriteToFile(bufferPage);
    curReadPage = 0;
    heapFile->GetPage(readBuffer, curReadPage);
}

// Closes the binary file and writes everything in the write buffer. 
int DBFile::Close () {
    if(dirtyBit)
        WriteToFile(bufferPage);
    heapFile->Close();
    return 1;
}

// Adds a record/entry into the write buffer. 
void DBFile::Add (Record &rec) {
    // If the buffer page is full write the page into the file.
    if(bufferPage->Append(&rec) == 0) {
        WriteToFile(bufferPage);
        // Append the record again since the first one failed.
        bufferPage->Append(&rec);
    }
    dirtyBit = true;
}

// Writes everything on the write buffer into the binary file.
void DBFile::WriteToFile(Page *bufferPage) {
    heapFile->AddPage(bufferPage, heapFile->GetLength());
    bufferPage->EmptyItOut();
    WritePageToFile(heapFile, bufferPage);
    dirtyBit = false;
}

// Get the next record from the read buffer.
int DBFile::GetNext (Record &fetchme) {
    // Assume the buffer is dirty and write.
    if(dirtyBit)
        WriteToFile(bufferPage);
    if(readBuffer==nullptr) {
        MoveFirst();
    }
    
    while(readBuffer->GetFirst(&fetchme)==0) {
        // Read failed
        if(heapFile->GetLength()-1 <= curReadPage+1) {
            return 0;
        }

        // Read next page
        heapFile->GetPage(readBuffer, ++curReadPage);
    }
    return 1;

}

// Get the next record from the read buffer that matches the given CNF.
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // Assume the buffer is dirty and write.
    if(dirtyBit)
        WriteToFile(bufferPage);
    if(readBuffer==nullptr) {
        MoveFirst();
    }
    ComparisonEngine comp;
    do {
        while(readBuffer->GetFirst(&fetchme)==0) {
            // Read failed
            if(heapFile->GetLength()-1 <= curReadPage+1) {
                return 0;
            }

            // Read next page
            heapFile->GetPage(readBuffer,++curReadPage);
        }
    } while(!comp.Compare(&fetchme, &literal, &cnf));
    
    return 1;
}
