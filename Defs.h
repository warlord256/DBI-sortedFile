#ifndef DEFS_H
#define DEFS_H

#include <string>

#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
#define DEFAULT_NO_OF_PAGES 128
#define PIPE_BUFFER_SIZE 100

#define WritePageToFile(file, page) 								\
	int pageNum = file->GetLength()==0 ? 0 : file->GetLength()-1;	\
	file->AddPage(page, pageNum); 									\
	page->EmptyItOut();

#define DeletePointersInVector(vectorPointer)           \
    for(int i=0;i<vectorPointer->size();i++) {          \
        delete (*vectorPointer)[i];                     \
    }

#define DeleteVectorOfPointers(vectorPointer)           \
    DeletePointersInVector(vectorPointer)               \
    delete vectorPointer;

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};

unsigned int Random_Generate();

static void generateRandomString(int size, std::string &s) {
    // !! Do not seed the rand as it is called by threads, it'll genrate the same strings.
    // srand(time(NULL)); // seed with time
    static const std::string validChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    
    size = (rand() % size) + 1; // this randomises the size

    while(--size > -1)
        s += validChars[(rand() % validChars.length())]; // generate a string ranging from the space character to ~ (tilde)

}


#endif

