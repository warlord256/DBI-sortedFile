#include <iostream>
#include <fstream>
#include <string.h>
#include "DBFile.h"
#include "Defs.h"
#include "HeapDBFile.h"
#include "SortedDBFile.h"
#include "Errors.h"

DBFile :: DBFile () {
    dbFile = NULL;
}

string getMetaFilePath(const char *fpath) {
    string metafilePath(fpath);
    metafilePath.append(".meta");
    return metafilePath;
}

int DBFile :: Create (const char *fpath, fType file_type, void *startup){
    
    sortParams *s = (sortParams*)startup;

    switch (file_type) {
        case heap:
            dbFile = new HeapDBFile();
            break;
        case sorted:
            FATALIF(startup==NULL, "Ordermaker and Run length required to created sorted file");
            dbFile = new SortedDBFile(s->o, s->l);
            break;
        default:
            cout<<"Unknown file type!\n";
            return 0;
    }

    // Create meta file.
    ofstream fileStream;
    fileStream.open(getMetaFilePath(fpath));
    
    // Write to file after the instantiation.
    fileStream<<fTypeStrings[file_type]<<endl;
    if(startup != NULL) {
        fileStream<<*s->o<<endl;
        fileStream<<s->l<<endl;
    }
    fileStream.close();
    return dbFile->Create(fpath, file_type, startup);
}

int DBFile :: Open (const char *fpath){
    // This check is to make sure the previous file is closed if called after create.
    // Else results in random length of the file.
    if(dbFile!=NULL) {
        dbFile->Close();
    }
    // Read metadata file and figure out the instance.
    ifstream fileStream(getMetaFilePath(fpath));
    string s;
    FATALIF(!fileStream.is_open(), "No meta file found! Please make sure the file is created.");
    getline(fileStream,s);
    if(s.compare(fTypeStrings[heap])==0) {
       dbFile = new HeapDBFile(); 
    } else if(s.compare(fTypeStrings[sorted])==0) {
        OrderMaker *o = new OrderMaker();
        int runLen;
        fileStream>>*o;
        fileStream>>runLen;
        dbFile = new SortedDBFile(o, runLen);
    } else {
        cout << "Unkown file type "<< s <<". Unable to open file.\n";
        fileStream.close();
        return 0;
    }
    fileStream.close();

    return dbFile->Open(fpath);
}

int DBFile :: Close (){
    if(dbFile==NULL) {
        return 0;
    }
    return dbFile->Close();
}

void DBFile :: Load (Schema &myschema, const char *loadpath){
    dbFile->Load(myschema, loadpath);
}

void DBFile :: MoveFirst (){
    dbFile->MoveFirst();
}

void DBFile :: Add (Record &addme){
    dbFile->Add(addme);
}

int DBFile :: GetNext (Record &fetchme){
    return dbFile->GetNext(fetchme);
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal){
    return dbFile->GetNext(fetchme, cnf, literal);
}

