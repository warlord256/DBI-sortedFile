#include "Schema.h"
#include "Function.h"
#include "RelOp.h"

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
    string relName;
    AndList *andList; // Not necessary but required for a tiny functionality.

    // Pipes involved.
    int lPipe = -1, rPipe = -1, outPipe = -1;

    // The relation operation for the node.
    RelationalOp *relOp;

    // DBFile for select oprations.
    DBFile *file;

    Node(Operation type);
    virtual ~Node();
    void Print();
    // Function which starts the thread of the node.
    void Execute(Pipe **pipes);
    // Cleans up
    void CleanUp();
};

class QueryPlan {
private:
    // Operations for each node.
    int nodeCount = 0;
    RelationalOp **operations;

    // Recursive function to start threads for operation.
    void Execute(Node *n);

public:
    Node *root;
    // Pipes to be used for execution.
    int pipeCount=0;
    Pipe **pipes;

    QueryPlan();
    virtual ~QueryPlan();
    // Assign pipes for a given node.
    void AssignPipes(Node *node);
    // Print the query plan.
    void Print();
    // Executes the query plan.
    void Execute();
    // Waits until all of the nodes complete execution.
    void Cleanup(Node *n);
    // Gets the final output pipe.
    void GetOutputPipe(Pipe *out);
};