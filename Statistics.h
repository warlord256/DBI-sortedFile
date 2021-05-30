#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <string>
using namespace std;

struct RelationStats{
	int noOfTuples;
	unordered_map<string, int> attrMap;
};

class Statistics
{
	int relCount = 0;
	unordered_map<string, int> relToSet;
	unordered_map<int, RelationStats> setToStats;
    unordered_map<string, int> attrToSet;

	bool IsValidJoin(char *relNames[], int numToJoin);

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
