#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072

#define WritePageToFile(file, page) 								\
	int pageNum = file->GetLength()==0 ? 0 : file->GetLength()-1;	\
	file->AddPage(page, pageNum); 									\
	page->EmptyItOut(); 											\
    

enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};


unsigned int Random_Generate();


#endif

