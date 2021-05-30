
#include <iostream>
#include "test.h"
#include "Execute.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {
	setup();
	cout<< "Enter the query to execute: \n";
	while(!yyparse()) {
		ExecuteQuery();
		cout<<"Query successful!\n";
		cout<< "Enter the query to execute: \n";
	}
	return -1;
}


