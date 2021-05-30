
#include <iostream>
#include "ParseTree.h"
#include "QueryOptimizer.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {

	if(yyparse()) {
		return -1;
	}
	QueryOptimizer opt;
	QueryPlan plan;
	opt.Optimize(plan);	
	plan.AssignPipes(plan.root);
	plan.Print();
	return 0;
}


