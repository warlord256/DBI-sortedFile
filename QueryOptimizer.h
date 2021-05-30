#ifndef QUERY_OPTIMIZER_H
#define QUERY_OPTIMIZER_H

#include "Statistics.h"
#include "QueryPlan.h"
#include <vector>

class QueryOptimizer {

    unordered_map<string, Node*> *selectOperations_t;
    unordered_map<string, int> *joinCountMap;
    void PerformAggregateFunction(QueryPlan *plan);
    void PerformDuplicateRemoval(QueryPlan *plan);
    void PerformProjection(QueryPlan *plan);
    void GetOptimalJoinOrder(Statistics &stats, vector<AndList*> &joins, unordered_map<string, string> &tables);        
    void PerformSelectOperations(Statistics &stats, vector<AndList*> &selects, unordered_map<string, string> &aliasToRel);
    void PerformOptimalOperation(Statistics &stats, vector<AndList*> &selects, vector<AndList*> &joins);

public:
    QueryOptimizer();
    virtual ~QueryOptimizer();
    void Optimize(QueryPlan &plan); 
};

#endif