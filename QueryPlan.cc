#include "QueryPlan.h"
#include "Errors.h"
#include <iostream>

using namespace std;

#define PRINT_SCHEMA(schema, attrs) \
    cout << endl << "Output Schema:\n" ;\
    attrs = schema->GetAtts();\
    for(int i=0;i<schema->GetNumAtts();i++) {\
        cout << "\tAtt: "<< string(attrs[i].name) << " : "<< type_s[attrs[i].myType]<<endl;\
    }

extern char* dbfile_dir;

QueryPlan :: QueryPlan() {
    root = NULL;
    pipes = NULL;
}

QueryPlan :: ~QueryPlan () {
    // Destroy
    delete root;
    if(pipes!=NULL)
        for(int i=0;i<pipeCount;i++) {
            delete pipes[i];
        }
    delete pipes;
}

Node :: Node (Operation type) {
    this->type = type;
    this->leftChild = NULL;
    this->rightChild = NULL;
    this->outputSchema = NULL;
    this->cnf = NULL;
    this->order = NULL;
    this->function = NULL;
    this->schema = NULL;
    this->literal = NULL;
    this->keepMe = NULL;
    this->andList = NULL;
    this->file = NULL;
    this->relOp = NULL;
};

Node :: ~Node() {
    delete leftChild;
    delete rightChild;
    delete outputSchema;
    delete cnf;
    delete order;
    delete function;
    delete schema;
    delete literal;
    delete[] keepMe;
    delete file;
    delete relOp;
}

void QueryPlan :: Print() {
    root->Print();
    cout << "**************************************************\n";
    return;
}

void PrintOrList(OrList *orList) {
    OrList *cur = orList;
    while(cur!=NULL) {
        cout << string(cur->left->left->value);
        if(cur->left->code == LESS_THAN) {
            cout << " < ";
        } else if (cur->left->code == GREATER_THAN) {
            cout << " > ";
        } else {
            cout << " = ";
        }
        cout << string(cur->left->right->value);
        if(cur->rightOr!=NULL) {
            cout << " OR ";
        }
        cur = cur->rightOr;
    }
}

void PrintAndList(AndList* andList) {
    AndList* cur = andList; 
    while(cur!=NULL) {
        cout<<"( ";
        PrintOrList(cur->left);
        cout << " )";
        if(cur->rightAnd!=NULL) {
            cout << " AND ";
        }
        cur = cur->rightAnd;
    }
}

string type_s[3] = {"Int", "Double", "String"};

void Node :: Print() {
    // Inorder printing.
    // Print left.
    if(leftChild != NULL) {
        leftChild->Print();
    }
    
    // Print node.
    Attribute *attrs = NULL;
    cout << "**************************************************\n";
    switch (type) {
    case SELECT_FILE:
        cout << "SELECT FILE Operation\n";
        cout << "CNF: ";
        PrintAndList(andList);
        cout << endl << "Output pipe: " << outPipe << endl;
        break;
    case SELECT_PIPE:
        cout<< "SELECT PIPE Operation\n";
        cout << "CNF: ";
        PrintAndList(andList);
        cout << endl << "Input pipe : " << lPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    case JOIN:
        cout << "JOIN Operation\n";
        cout << "CNF: ";
        PrintAndList(andList);
        cout << endl << "Left input pipe : " << lPipe << endl;
        cout << "Right input pipe : " << rPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    case DISTINCT:
        cout << "DUPLICATE REMOVAL Operation\n";
        cout << endl << "Input pipe : " << lPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    case GROUPBY:
        cout << "GROUP BY Operation\n";
        cout << "OrderMaker: \n";
        order->Print();
        cout<< "Function:\n";
        function->Print();
        cout << endl << "Input pipe : " << lPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    case SUM:
        cout << "SUM Operation\n";
        cout<< "Function:\n";
        function->Print();
        cout << endl << "Input pipe : " << lPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    case PROJECT:
        cout << "PROJECT Operation\n";
        cout << endl << "Input pipe : " << lPipe << endl;
        cout << "Output pipe : " << outPipe << endl;
        break;
    default:
        cout << "Unkown type of Node found!\n";
        break;
    }

    // Print schema.
    PRINT_SCHEMA(outputSchema, attrs);


    // Print right.
    if(rightChild != NULL) {
        rightChild->Print();
    }
    return;
}

void QueryPlan :: AssignPipes(Node* node) {
    FATALIF(node == NULL, "Cannot assign pipes to non existant node or NULL");
    // Assign pipes to the left subtree. 
    if(node->leftChild != NULL) {
        AssignPipes(node->leftChild);
        node->lPipe = node->leftChild->outPipe;
    }

    // Assign pipes to the right subtree.
    if(node->rightChild != NULL) {
        AssignPipes(node->rightChild);
        node->rPipe = node->rightChild->outPipe;
    }

    // Assign output pipe.
    node->outPipe = pipeCount++;
}

void QueryPlan::Execute() {
    pipes = new Pipe*[pipeCount];
    for(int i=0;i<pipeCount;i++) {
        pipes[i] = new Pipe(PIPE_BUFFER_SIZE);
    }
    Execute(root);
}

void QueryPlan::Execute(Node *node) {
    if(node == NULL) {
        return;
    }
    node->Execute(pipes);
    Execute(node->leftChild);
    Execute(node->rightChild);
}

string GetRelationFilePath(string relation) {
    return string(dbfile_dir)+relation+".bin";
}

void Node::Execute(Pipe **pipes) {
    switch (type) {
    case SELECT_FILE:
        file = new DBFile();
        file->Open(GetRelationFilePath(relName).c_str());
        relOp = new SelectFile();
        ((SelectFile*)relOp)->Run(*file, *pipes[outPipe], *cnf, *literal );
        break;
    case SELECT_PIPE:
        relOp = new SelectPipe();
        ((SelectPipe*)relOp)->Run(*pipes[lPipe], *pipes[outPipe], *cnf, *literal);
        break;
    case JOIN:
        relOp = new Join();
        ((Join*)relOp)->Run(*pipes[lPipe], *pipes[rPipe], *pipes[outPipe], *cnf, *literal);
        break;
    case DISTINCT:
        relOp = new DuplicateRemoval();
        ((DuplicateRemoval*)relOp)->Run(*pipes[lPipe], *pipes[outPipe], *schema);
        break;
    case GROUPBY:
        relOp = new GroupBy();
        ((GroupBy*)relOp)->Run(*pipes[lPipe], *pipes[outPipe], *order, *function);
        break;
    case SUM:
        relOp = new Sum();
        ((Sum*)relOp)->Run(*pipes[lPipe], *pipes[outPipe], *function);
        break;
    case PROJECT:
        relOp = new Project();
        ((Project*)relOp)->Run(*pipes[lPipe], *pipes[outPipe], keepMe, numAttsInput, numAttsOutput);
        break;
    default:
        cout << "Unkown type of Node found!\n";
        break;
    }
}

void QueryPlan::Cleanup(Node *n) {
    if(n == NULL) {
        return;
    }
    n->relOp->WaitUntilDone();
    n->CleanUp();
    Cleanup(n->leftChild);
    Cleanup(n->rightChild);
}

void Node::CleanUp() {
    // Take care of any cleanups.
    if(file != NULL) {
        file->Close();
    }
}

void QueryPlan :: GetOutputPipe(Pipe *out) {
    out = pipes[root->outPipe];
}