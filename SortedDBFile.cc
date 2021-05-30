#include <stdio.h>
#include "SortedDBFile.h"

SortedDBFile :: SortedDBFile(OrderMaker *order, int runLen) {
    this->order = order;
    this->runLen = runLen;
    this->bigQ = NULL;
    this->dirtyBit = false;
    this->in = NULL;
    this->out = NULL;
    this->file = new File();
    this->readBuffer = new Page();
    this->curReadPage = 0;
    this->inOrder = false;
}

int SortedDBFile :: Create (const char *fpath, fType file_type, void *startup) {
    this->openedFileName = (char*)fpath;
    file->Open(0, openedFileName);
    return 1;
}

int SortedDBFile :: Open (const char *fpath) {
    this->openedFileName = (char*)fpath;
    file->Open(1, openedFileName);
    return 1;
}

void SortedDBFile :: MoveFirst() {
    inOrder = false;
    if(dirtyBit) {
        WriteToFile();
    }
    curReadPage = 0;
    if(file->GetLength()>0) {
        file->GetPage(readBuffer, 0);
    }
}

int SortedDBFile :: Close() {
    if (dirtyBit) {
        WriteToFile();
    }
    file->Close();
    return 1;
}

void SortedDBFile :: Add(Record &r) {
    if(dirtyBit) {
        in->Insert(&r);
        return;
    }
    in = new Pipe(PIPE_BUFFER_SIZE);
    out = new Pipe(PIPE_BUFFER_SIZE);
    dirtyBit = true;
    bigQ = new BigQ(*in, *out, *order, runLen); 
    in->Insert(&r);
}

void SortedDBFile :: WriteToFile () {
    inOrder = false;
    in->ShutDown();
    // Very important!!!! if not false will cause an infinite loop!!
    dirtyBit = false;
    
    // To sore records from pipe and file in parallel.
    Record *r1 = new Record();
    Record *r2 = new Record();

    // New file to contain the new sorted records.
    File *tempFile = new File();
    string *fPath = new string(this->openedFileName);
    fPath->append(".tmp");
    tempFile->Open(0, (char *)fPath->c_str());

    // Write buffer.
    Page *bufferPage = new Page();

    // Begin from the start of the existing file.
    MoveFirst();

    ComparisonEngine ce;
    bool eofReached = GetNext(*r2) == 0;
    while(out->Remove(r1)) {
        // Until the pipe is exhausted compare and merge.
        while(!eofReached && ce.Compare(r2, r1, order)<0) {
            if(bufferPage->Append(r2)==0) {
                WritePageToFile(tempFile, bufferPage);
                bufferPage->Append(r2);
            }
            eofReached = GetNext(*r2) == 0;
        }
        if(bufferPage->Append(r1)==0) {
            WritePageToFile(tempFile, bufferPage);
            bufferPage->Append(r1);
        }
    }
    // Pipe exhausted dump all records remaining in file to new sorted file.
    while(!eofReached) {
        if(bufferPage->Append(r2)==0) {
            WritePageToFile(tempFile, bufferPage);
            bufferPage->Append(r2);
        }
        eofReached = GetNext(*r2) == 0;
    }
    WritePageToFile(tempFile, bufferPage);

    // Close the files as it will be deleted or renamed.
    file->Close();
    tempFile->Close();
    
    // Rename the temp file to be the original file.
    rename(fPath->c_str(), openedFileName);

    // Reopen the file so that further functions are not affected.
    Open(openedFileName);

    MoveFirst();
}

// This is a Write to file implementation using pipe might have very high complexity for large data.
// void SortedDBFile :: WriteToFile() {
//     Record *r = new Record();
//     // Very important!!!! if not false will cause an infinite loop!!
//     dirtyBit = false;
//     MoveFirst();
//     // Push all records from file into the pipe to sort completely.
//     while(GetNext(*r)) {
//         in->Insert(r);
//     }

//     // Close the pipe to sort the files
//     in->ShutDown();

//     // Overwrite the file to store the sorted files.
//     file->Close();
//     file->Open(0, openedFileName);
//     Page *writeBuffer = new Page();
//     while(out->Remove(r)) {
//         if(writeBuffer->Append(r)==0) {
//             WritePageToFile(file, writeBuffer);
//             writeBuffer->Append(r);
//         }
//     }
//     WritePageToFile(file, writeBuffer);
    
//     // Might be required to move to the first page.
//     MoveFirst();
// }

int SortedDBFile :: GetNext (Record &fetchme) {
    if(dirtyBit) {
        MoveFirst();
    }
    
    while(readBuffer->GetFirst(&fetchme)==0) {
        // Read failed
        if(file->GetLength()-1 <= curReadPage+1) {
            return 0;
        }
        // Read next page
        file->GetPage(readBuffer, ++curReadPage);
    }
    return 1;
}

int SortedDBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine ce;
    if(!inOrder) {
        // Not in order, generate the query order maker and binary search the file.
        inOrder = true;
        // Generate Ordermaker 
        OrderMaker *queryOrder = new OrderMaker();
        OrderMaker *cnfOrder = new OrderMaker();
        order->GenerateQueryOrderMaker(cnf, *cnfOrder, *queryOrder);
        this->queryOrderMaker = queryOrder;
        this->cnfOrderMaker = cnfOrder;
        if(!BinarySearch(fetchme, cnf, literal, cnfOrder)) {
            return 0;
        }
    } else if(GetNext(fetchme)==0) {
        // Get the next record if in order. 
        return 0;
    }
    
    while(!ce.Compare(&fetchme, &literal, &cnf)) {
        if(GetNext(fetchme)==0) {
            return 0;
        }
    }
    return 1;
}

bool SortedDBFile :: BinarySearch(Record &fetchMe, CNF &cnf, Record &literal, OrderMaker *cnfOrder) {
    ComparisonEngine ce;
    // Edge case: check if the current record satisfies the constraint and return without changing page.
    GetNext(fetchMe);
    if(ce.Compare(&fetchMe, queryOrderMaker, &literal, cnfOrder)>=0) {
        return true;
    }
    
    int low = 0;
    int high = file->GetLength()-2;
    int mid = 0;
    while(low < high) {
        // Binary search the possible page.
        mid = (high+low+1)/2;
        file->GetPage(readBuffer, mid);
        curReadPage = mid;
        if(readBuffer->GetFirst(&fetchMe)==0) {
            // Failed
            return false;
        }

        if(ce.Compare(&fetchMe, queryOrderMaker, &literal, cnfOrder)<0) {
            low = mid;
        } else {
            high = mid-1;
        }

    }

    // Linear search the page for record
    file->GetPage(readBuffer, low);
    curReadPage = low;
    while(readBuffer->GetFirst(&fetchMe)) {
        int res = ce.Compare(&fetchMe, queryOrderMaker, &literal, cnfOrder);
        if(res>=0) {
            return true;
        }
    }
    return false;
} 