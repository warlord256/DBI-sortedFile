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

QueryPlan :: QueryPlan() {
    root = NULL;
}

QueryPlan :: ~QueryPlan () {
    // Destroy
    delete root;
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
    delete keepMe;
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