#include "HeapDBFile.h"

HeapDBFile :: HeapDBFile() {
    // Initialize the write buffer
    bufferPage = new Page();
    // initialize the read buffer
    readBuffer = new Page();
    // initialize binary db file
    heapFile = new File();
}

// Creates and opens the file specified.   
int HeapDBFile::Create (const char *f_path, fType f_type, void *startup) {
    heapFile->Open(0, (char*)f_path);
    return 1;
}

// Loads all the contents/records of a .tbl file onto the binary file. 
// void HeapDBFile::Load (Schema &f_schema, const char *loadpath) {
//     FILE *loadFile = fopen (loadpath, "r");
//     Record r;
//     while(r.SuckNextRecord(&f_schema, loadFile)) {
//         Add(r);
//     }
// }

// Opens an existing binary file to perform operations.(Need not be used if create was called before for the same file)
int HeapDBFile::Open (const char *f_path) {
    heapFile->Open(1, (char *) f_path);
    return 1;
}

// Loads up the first page from the file onto the read buffer.
void HeapDBFile::MoveFirst () {
    if(dirtyBit)
        WriteToFile(bufferPage);
    curReadPage = 0;
    heapFile->GetPage(readBuffer, curReadPage);
}

// Closes the binary file and writes everything in the write buffer. 
int HeapDBFile::Close () {
    if(dirtyBit)
        WriteToFile(bufferPage);
    heapFile->Close();
    return 1;
}

// Adds a record/entry into the write buffer. 
void HeapDBFile::Add (Record &rec) {
    // If the buffer page is full write the page into the file.
    if(bufferPage->Append(&rec) == 0) {
        WriteToFile(bufferPage);
        // Append the record again since the first one failed.
        bufferPage->Append(&rec);
    }
    dirtyBit = true;
}

// Writes everything on the write buffer into the binary file.
void HeapDBFile::WriteToFile(Page *bufferPage) {
    WritePageToFile(heapFile, bufferPage);
    dirtyBit = false;
}

// Get the next record from the read buffer.
int HeapDBFile::GetNext (Record &fetchme) {
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
int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
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