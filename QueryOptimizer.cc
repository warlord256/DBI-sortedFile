#include "QueryOptimizer.h"
#include "Errors.h"
#include "Defs.h"
#include <iostream>
#include <set>
#include "Function.h"

char *catalog_path = "catalog";

#define GetInvlolvedTables(involvedTables, andList)             \
    OrList *temp = andList->left;                               \
    while(temp!=NULL) {                                         \
        ComparisonOp *comp = temp->left;                        \
        temp = temp->rightOr;                                   \
        if(comp->left->code==NAME) {                            \
            string s(comp->left->value);                        \
            involvedTables.insert(s.substr(0, s.find('.')));    \
        }                                                       \
    }

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

void QueryOptimizer :: PerformAggregateFunction(QueryPlan *plan) {
    // Check if there is some aggrgate function.
    if(finalFunction == NULL) {
        return;
    }

    Node *root = plan->root;

    //Check if distinct has to be called first.
    if(distinctFunc) {
        // I Have no clue what to do.
    }

    // Count the number of grouping attrs.
    int attrCount = 0;
    NameList *cur = groupingAtts;
    while(cur!=NULL) {
        attrCount++;
        cur = cur->next;
    }

    // Reset cur.
    cur = groupingAtts;

    // Fill the atts except for the first one
    Attribute atts[attrCount+1];
    for(int i=1;i<=attrCount;i++) {
        atts[i] = {cur->name, root->outputSchema->FindType(cur->name)};
        cur = cur->next;
    } 
    
    // Create the function and check what is returned.
    Function *func = new Function();
    func->GrowFromParseTree(finalFunction, *root->outputSchema);
    if(func->returnsInt) {
        atts[0] = {"sum", Int};
    } else {
        atts[0] = {"sum", Double};
    }

    Node *n;
    // Check if there are grouping attrs
    if(groupingAtts!=NULL) {
        n = new Node(GROUPBY);
        n->order = new OrderMaker(root->outputSchema, attrCount, &atts[1]);
    } else {
        // Perform SUM:
        n = new Node(SUM);
    }

    n->leftChild = root;
    // Perepare out schema.
    n->outputSchema = new Schema(attrCount+1, atts);      
    // Assign stuff required for group by or sum.
    n->function = new Function();
    n->function->GrowFromParseTree(finalFunction, *root->outputSchema);

    // Update root.
    root = n;
    plan->root = n;
    return;
}

QueryOptimizer::QueryOptimizer() {
    selectOperations_t = new unordered_map<string, Node*>();
}

QueryOptimizer::~QueryOptimizer() {
    // delete your maps
    delete selectOperations_t;
    delete joinCountMap;
}


void QueryOptimizer :: PerformDuplicateRemoval(QueryPlan *plan) {
    Node *root = plan->root;
    if(distinctAtts) {
        Node *n = new Node(DISTINCT);
        n->leftChild = root;
        n->outputSchema = root->outputSchema;
        
        // Update root
        plan->root = n;
    }
}

void QueryOptimizer :: PerformProjection(QueryPlan *plan) {
    Node *root = plan->root;
    NameList *cur = attsToSelect;
    set<string> toProject;

    int count = 0;
    // If there is function it should be in the projection.
    if(finalFunction!=NULL) {
        count++;
        toProject.insert("sum");
    }

    while (cur != NULL) {
        count++;
        toProject.insert(string(cur->name));
        cur = cur->next;
    }

    cur = attsToSelect;
    Attribute attrs[count];
    int *keepMe = new int[count];
    int index = 0;

    Attribute *opAttrs = root->outputSchema->GetAtts();
    for(int i=0;i<root->outputSchema->GetNumAtts(); i++) {
        string curAttr = string(opAttrs[i].name);
        if(toProject.count(curAttr)>0) {
            attrs[index] = opAttrs[i];
            keepMe[index] = i;
            index++;
        }
    }

    FATALIF(count!=index, "A projected attribute does not seem to exist in the schema!"); 
    
    Node *n = new Node(PROJECT);
    n->outputSchema = new Schema(count, attrs);
    n->leftChild = root;
    // Data for project
    n->numAttsInput = root->outputSchema->GetNumAtts();
    n->numAttsOutput = count;
    n->keepMe = keepMe;

    plan->root = n;
    return;
}

void QueryOptimizer::Optimize(QueryPlan &plan) {
    cout<<"Optimizing\n";
    unordered_map<string, string> aliasToRel;
    
    Statistics stat;
    stat.Read("Statistics.txt");

    FATALIF(NULL==tables, "There seems to no query read for optimizing.");
    
    while (tables!=NULL) {
        aliasToRel[string(tables->aliasAs)] = string(tables->tableName);

        stat.CopyRel(tables->tableName, tables->aliasAs);

        tables = tables->next;
    }
    
    for(auto i : aliasToRel) {
        cout << i.first << " : "<<i.second<<endl;
    }

    cout<<"Join + cross : " << aliasToRel.size()-1<<endl;
    
    // Split up the ands
    vector<AndList *> splitAnds;
    while(boolean!=NULL) {
        AndList *temp = boolean;
        splitAnds.push_back(temp);
        boolean = boolean->rightAnd;
        temp->rightAnd = NULL;
    }

    vector<AndList *> selects;
    vector<AndList *> joins;
    
    for(auto i : splitAnds) {
        // Read all ors and determine if it is a join.
        OrList *temp = i->left;
        
        FATALIF (temp==NULL, "Seems to be an invalid andList");

        if(temp->rightOr==NULL) {
            ComparisonOp *comp = temp->left;
            // Check if it is a join like (A.b = B.c)
            if(comp->left->code==NAME && comp->right->code==NAME) {
                // Push the respective andList as a join
                joins.push_back(i);
            } else {
                // It is a select like (A.b = 5)
                selects.push_back(i);
            }
        } else {
            // Assume it is a select like (A.b > 5 or A.c < 10)
            selects.push_back(i);
        }
    }
    cout << "Number of joins = "<<joins.size()<<endl;
    cout << "Number of Selects = "<<selects.size()<<endl;

    joinCountMap = new unordered_map<string, int>();
    for(auto i : aliasToRel) {
        (*joinCountMap)[i.first] = 1;
    }

    // Start statistics calaculations using a recursive function.
    PerformSelectOperations(stat, selects, aliasToRel);
    PerformOptimalOperation(stat, selects, joins);
    // TODO: check for any cross products have to be performed.
    // TODO: Check that the map only has a single node as values for all relations.
    // Assign the node as the root of the plan.
    plan.root = (*selectOperations_t->begin()).second;
    // Perform Aggregate functions.
    PerformAggregateFunction(&plan);
    // Perform Project.
    PerformProjection(&plan);
    // Perform distinct selection.
    PerformDuplicateRemoval(&plan);

    cout<<"Done optimizing...\n";
    return;
}

void QueryOptimizer :: PerformOptimalOperation(Statistics &stats, vector<AndList*> &selects, vector<AndList*> &joins) {
    if(joins.size()==0 && selects.size()==0) {
        return;
    }

    char* relNames[joinCountMap->size()];
    set<string> involvedTables;
    // vector<AndList *>::iterator itrToDelete = selects.end();
    for(int i = 0; i<selects.size(); i++) {
        involvedTables.clear();
        AndList *cond = selects[i];
        GetInvlolvedTables(involvedTables, cond);
        if(involvedTables.size()==1 || stats.AreTablesJoined(involvedTables)) {
            string relation = *involvedTables.begin();
            FATALIF(selectOperations_t->count(relation)==0, "All selects were not performed before joining.");
            
            Node *n = new Node(SELECT_PIPE);
            // Add required stuff like CNF into Node.
            // n->defn = "Peforming select from Pipe "+relation;
            n->leftChild = (*selectOperations_t)[relation];
            n->outputSchema = n->leftChild->outputSchema;
            n->andList = selects[i];
            n->cnf = new CNF();
            n->literal = new Record();
            n->cnf->GrowFromParseTree(selects[i], n->outputSchema, *n->literal);
              
            // char* relNames[involvedTables.size()];
            int index=0;
            for(auto j : involvedTables) {
                relNames[index++] = (char *)j.c_str();
            }
            
            stats.Apply(cond, relNames, involvedTables.size());

            selects.erase(selects.begin()+i);
            
            PerformOptimalOperation(stats, selects, joins);
            return;
        }
        
    }

    //Perform the best join.
    string relNames_s[joinCountMap->size()];
    unordered_map<string, int> &countMap = *joinCountMap;

    int nextJoinIndex = -1;
    double minTuples = INT64_MAX; 
    string leftRel, rightRel, tempLeft, tempRight;
    for (int i=0;i<joins.size();i++) {
        ComparisonOp *comp = joins[i]->left->left;
        tempLeft = string(comp->left->value);
        tempLeft = tempLeft.substr(0, tempLeft.find('.'));
        tempRight = string(comp->right->value);
        tempRight = tempRight.substr(0, tempRight.find('.'));
        relNames_s[0] = tempLeft;
        relNames_s[1] = tempRight;
        stats.GetRelNamesForJoin(relNames, relNames_s);
        double count = stats.Estimate(joins[i], relNames, countMap[tempLeft]+countMap[tempRight]);
        if(count < minTuples) {
            nextJoinIndex = i;
            minTuples = count;
            leftRel = tempLeft;
            rightRel = tempRight;
        }
    }
    // cout<<"Selected index = "<<nextJoinIndex<<endl;
    countMap[leftRel] = countMap[leftRel]+countMap[rightRel];
    countMap[rightRel] = countMap[leftRel];
    relNames_s[0] = leftRel;
    relNames_s[1] = rightRel;
    // Swap left and right if the number of rows is more on the right.
    // The bigger relation should be on the left for implemented join.
    if(stats.ShouldSwap(leftRel, rightRel)) {
        string tmp = leftRel;
        leftRel = rightRel;
        rightRel = tmp;
    }
    stats.GetRelNamesForJoin(relNames, relNames_s);
    stats.Apply(joins[nextJoinIndex], relNames, countMap[leftRel]);

    // Make sure left and right relations are slected.
    FATALIF(selectOperations_t->count(leftRel)==0, "Make sure left relation is slected.");
    FATALIF(selectOperations_t->count(rightRel)==0, "Make sure right relation is slected.");

    // Add the join node.
    Node *n = new Node(JOIN);
    // n->defn = "Performing Join of relations " + leftRel + " and "+rightRel;
    n->leftChild = (*selectOperations_t)[leftRel];
    n->rightChild = (*selectOperations_t)[rightRel];
    // Get the joined schema.
    n->outputSchema = new Schema(n->leftChild->outputSchema, n->rightChild->outputSchema);

    // Add required data for performing join.
    n->andList = joins[nextJoinIndex];
    n->cnf = new CNF();
    n->literal = new Record();
    n->cnf->GrowFromParseTree(n->andList, n->leftChild->outputSchema, n->rightChild->outputSchema, *n->literal);
    
    // Update the node for all involved relations.
    for(int i=0;i<countMap[leftRel];i++) {
        (*selectOperations_t)[relNames_s[i]] = n;
    }

    // Erase the performed join.
    joins.erase(joins.begin()+nextJoinIndex);
    // Call recursive func.
    PerformOptimalOperation(stats, selects, joins);

    return;
}

void QueryOptimizer::PerformSelectOperations(Statistics &stats, vector<AndList*> &selects, unordered_map<string, string> &aliasToRel) {
    set<string> invTables;
    set<int> indexToDelete;
    for(int i=0;i<selects.size();i++) {
        invTables.clear();
        GetInvlolvedTables(invTables, selects[i]);
        
        if(invTables.size()>1) {
            continue;
        }

        string j = *invTables.begin();
        if(selectOperations_t->count(j)==0) {
            // Missing! first perform selection.
            Node *n = new Node(SELECT_FILE);
            // Get the schema from catalog and fix the schema with the alias.
            Schema sch(catalog_path, (char *)aliasToRel[j].c_str());
            n->outputSchema = new Schema(&sch, j.c_str());
            
            // Add required stuff like CNF into Node.
            n->cnf = new CNF();
            n->literal = new Record();
            n->cnf->GrowFromParseTree(selects[i], n->outputSchema, *n->literal);
            n->andList = selects[i];
            // n->defn = "Peforming select of relation "+j;
            (*selectOperations_t)[j] = n;
            
        } else {
            Node *n = (*selectOperations_t)[j];
            // Modify the and list to include this condition as well.
            AndList *toJoin = n->andList;
            while(toJoin->rightAnd!=NULL) {
                toJoin = toJoin->rightAnd;
            }
            toJoin->rightAnd = selects[i];
            // Update required stuff like CNF into Node.
            n->cnf->GrowFromParseTree(n->andList, n->outputSchema, *n->literal);
        }
        indexToDelete.insert(i);

        char* relName[] = {(char*) j.c_str()};
        stats.Apply(selects[i], relName, 1);
    }

    // Remove the performed selects.
    for(int i : indexToDelete) {
        selects.erase(selects.begin()+i);
    }

    for(auto i : aliasToRel) {
        // Make sure left and right relations are slected.
        if(selectOperations_t->count(i.first)==0) {
            // Missing! first perform selection.
            Node *n = new Node(SELECT_FILE);
            // Add required stuff like CNF into Node.
            n->cnf = new CNF();
            n->literal = new Record();
            // Get the schema from catalog and fix the schema with the alias.
            Schema sch(catalog_path, (char *)aliasToRel[i.first].c_str());
            n->outputSchema = new Schema(&sch, i.first.c_str());

            // n->defn = "Peforming select of relation "+i.first;            
            (*selectOperations_t)[i.first] = n;
        }
    }

}
