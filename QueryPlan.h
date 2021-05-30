#include "Schema.h"
#include "Function.h"

// Node types
enum Operation {SELECT_FILE, SELECT_PIPE, PROJECT, DISTINCT, GROUPBY, SUM, JOIN};


class Node {
public:
    Operation type;
    Node *leftChild;
    Node *rightChild;

    // Output
    Schema *outputSchema;

    // Inputs
    CNF *cnf; 
    OrderMaker *order; 
    Function *function; 
    Schema *schema;
    Record *literal;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
    AndList *andList; // Not necessary but required for a tiny functionality.

    // Pipes involved.
    int lPipe = -1, rPipe = -1, outPipe = -1;

    // string defn;
    Node(Operation type);
    virtual ~Node();
    void Print();
};

class QueryPlan {
public:
    Node *root;
    int pipeCount=0;

    QueryPlan();
    virtual ~QueryPlan();
    // Assign pipes for a given node.
    void AssignPipes(Node *node);
    // Print the query plan.
    void Print();
};