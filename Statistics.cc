#include "Statistics.h"
#include <fstream>
#include <sstream>
#include <set>
#include "Errors.h"

#define FindNumberOfTuples(attr, attrName)\
    setToStats[attrToSet[attr]].attrMap[attrName]

#define UpdateAttrCount(attr, attrName, val)\
    setToStats[attrToSet[attr]].attrMap[attrName] = val

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
    relCount = copyMe.relCount;
    relToSet = copyMe.relToSet;
    setToStats = copyMe.setToStats;
    attrToSet = copyMe.attrToSet;
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    string rel(relName);
    if(relToSet.count(rel)) {
        setToStats[relToSet[rel]].noOfTuples = numTuples;
    } else {
        RelationStats stat;
        stat.noOfTuples = numTuples;
        relToSet[rel] = relCount;
        setToStats[relCount] = stat;
        relCount++;       
    }
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    string rel(relName);
    string attr(attName);
    // Att to map.
    attrToSet[attr] = relToSet[rel];

    if(numDistincts == -1) {
        numDistincts = setToStats[relToSet[rel]].noOfTuples;
    }
    setToStats[relToSet[rel]].attrMap.insert({attName, numDistincts});
}

void Statistics::CopyRel(char *oldName, char *newName)
{
    string oldName_s(oldName);
    string newName_s(newName);
    AddRel(newName, setToStats[relToSet[oldName_s]].noOfTuples);
    setToStats[relToSet[newName_s]].attrMap = setToStats[relToSet[oldName_s]].attrMap;
    for(auto i : setToStats[relToSet[newName_s]].attrMap) {
        string attrName = newName_s + '.' +i.first;
        attrToSet[attrName] = relToSet[newName_s];
    }
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream in(fromWhere);
    string line;
    // Very simple read for singleton relations.
    // while(getline(in, line)) {
    //     istringstream stream(line);
    //     string str;
    //     int count;
    //     stream>>str;
    //     stream>>count;
    //     char * relName = (char*)str.c_str();
    //     AddRel(relName, count);
    //     while(getline(in, line)) {
    //         if(line.empty()) {
    //             break;
    //         }
    //         stream = istringstream(line);
    //         stream>>str>>count;
    //         AddAtt(relName, (char*)str.c_str(), count);
    //     }
    // }

    // The very detailed read supporting non singleton sets.
    if(getline(in, line)) {
        in>>relCount;
    }

    // Fill rel to set
    while(getline(in, line)) {
        if(line.empty()) {
            break;
        }

        istringstream stream(line);
        string str;
        int num;
        stream>>str;
        stream>>num;
        relToSet[str] = num;
    }

    // Fill Set to stats
    while(getline(in, line)) {
        if(line.empty()) {
            break;
        }
        istringstream stream(line);
        string str;
        int set;
        stream>>set;
        int num;
        stream>>num;
        setToStats[set].noOfTuples = num;
        bool done = false;
        while(getline(in, line)) {
            if(line.empty()) {
                break;
            }
            istringstream attrStream(line);
            attrStream>>str>>num;
            setToStats[set].attrMap[str] = num;
        }
    }

    // Fill attr to set.
    while(getline(in, line)) {
        istringstream stream(line);
        string str;
        int num;
        stream>>str>>num;
        attrToSet[str] = num;
    }

}

void Statistics::Write(char *fromWhere)
{
    ofstream out(fromWhere);
    // A very simple write, enough for the case of singelton relations.
    // for(auto i : relToSet) {
    //     out<<i.first<<" "<<setToStats[i.second].noOfTuples<<endl;
    //     for(auto j : setToStats[i.second].attrMap) {
    //         out<<j.first<<" "<<j.second<<endl;
    //     }
    //     out<<endl;
    // }
    
    // A very detailed write... this supports writes after calling apply, i.e with joined tables.
    // Write the relations count
    out<<relCount<<endl;
    // Write the relation to set
    // Prepare relation map.
    unordered_map<int, set<string>> setToRel;
    for(auto i : relToSet) {
        setToRel[i.second].insert(i.first);
    }

    // Write out all sets.
    for(auto i : setToRel) {
        // Write the set id.
        out<<i.first<<" ";
        // write all relations.
        for(auto j : i.second) {
            out<<j<<" ";
        }
        out<<endl;
    }
    out<<endl;
    
    // Write out the set to stats.
    for(auto i : setToStats) {
        out<<i.first<<" "<<i.second.noOfTuples<<endl;
        for(auto j : i.second.attrMap) {
            out<<j.first<<" "<<j.second<<endl;
        }
        out<<endl;
    }
    out<<endl;

    // Write attr to set.
    for(auto i : attrToSet) {
        out<<i.first<<" "<<i.second<<endl;
    }

}

bool Statistics::IsValidJoin(char *relNames[], int numToJoin) {
    set<int> includedSets;
    // Check the sets included in the join.
    for(int i = 0;i<numToJoin;i++) {
        includedSets.insert(relToSet[string(relNames[i])]);
    }

    // Check if all relations are included in the join.
    int count = 0;
    for(auto i : relToSet) {
        if(includedSets.count(i.second)) {
            count++;
        }
    }

    return (count==numToJoin);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    if(numToJoin <= 1) {
        return;
    }

    FATALIF(!IsValidJoin(relNames, numToJoin), "The join requested is not a valid join.");

    double res = 1;
    double factor = 1.0;
    set<string> processedOp;

    while(parseTree != NULL) {
        OrList *orList = parseTree->left;
        parseTree = parseTree->rightAnd;
        double orFactor = 0;
        double orFactorCorrection = 1;
        processedOp.clear();
        while(orList!=NULL) {
            ComparisonOp *cOp = orList->left;
            orList = orList->rightOr;
            
            string opLeft(cOp->left->value);
            string attrLeft = opLeft;
            int idxToSplit = opLeft.find('.');
            if(idxToSplit!=string::npos) {
                attrLeft = attrLeft.substr(idxToSplit+1);
            }
            string opRight(cOp->right->value);
            string attrRight(opRight);
            idxToSplit = opRight.find('.');
            if(idxToSplit!=string::npos) {
                attrRight = attrRight.substr(idxToSplit+1);
            }

            bool newOperandProcessed = false;

            if(cOp->code < EQUALS) {
                // Insert name into processed attributes.
                newOperandProcessed = cOp->left->code == NAME ? processedOp.insert(opLeft).second : processedOp.insert(opLeft).second;
                
                // Assume 1/3 of the cross product is selected.
                orFactor += 1.0/3;
                if(newOperandProcessed) {
                    orFactorCorrection *= 1.0/3;
                }
                string msg = "The attribute "+ opLeft+ " does not exist.";
                FATALIF(attrToSet.count(opLeft)==0, msg.c_str());
                UpdateAttrCount(opLeft, attrLeft, FindNumberOfTuples(opLeft, attrLeft)/3);
                continue;
            }

            int maxTuples = 0;
            int minTuples = INT32_MAX;

            if(cOp->left->code==NAME) {
                FATALIF(attrToSet.count(opLeft)==0, "The attribute does not exist");
                maxTuples = max(FindNumberOfTuples(opLeft, attrLeft), maxTuples);
                minTuples = min(minTuples, FindNumberOfTuples(opLeft, attrLeft));
                newOperandProcessed = processedOp.insert(opLeft).second;
            }
            if(cOp->right->code==NAME) {
                FATALIF(attrToSet.count(opRight)==0, "The attribute does not exist");
                maxTuples = max(FindNumberOfTuples(opRight, attrRight), maxTuples);
                minTuples = min(minTuples, FindNumberOfTuples(opRight, attrRight));
                newOperandProcessed = processedOp.insert(opRight).second;
            }

            // Add it to factor.
            double reductionFactor = 1.0/maxTuples;
            orFactor += reductionFactor;
            if(newOperandProcessed) {
                orFactorCorrection *= reductionFactor;
            }
            
            //Update the distincts of attrs
            if(cOp->left->code==NAME) {
                UpdateAttrCount(opLeft, attrLeft, minTuples);
            }
            if(cOp->right->code==NAME) {
                UpdateAttrCount(opRight, attrRight, minTuples);
            }
        }

        // Correct the estimation of orFactor a little.
        if(processedOp.size()>1 && orFactor > orFactorCorrection) {
            orFactor -= orFactorCorrection;
        }

        // Multiply the probablity for ands
        factor *= orFactor;
    }

    // Calculate the final count of tuples
    res = factor;

    set<int> visited;
    for(int i=0;i<numToJoin;i++) {
        string rel(relNames[i]);
        int relSet = relToSet[rel];
        if(visited.count(relSet) == 0) {
            // visit set.
            res *= setToStats[relSet].noOfTuples;
            visited.insert(relSet);
        }
    }
    
    // Update the relation tuples
    if(visited.empty()) {
        return;
    }
    
    int selectedSet = *visited.begin();
    visited.erase(selectedSet);
    for(int i=0;i<numToJoin;i++) {
        // Update the set to which the relation belongs.
        string rel(relNames[i]);
        relToSet[rel] = selectedSet;
    }

    // Update the attribute to set map to point to the new set.
    for(auto i : attrToSet) {
        if(visited.count(i.second)) {
            attrToSet[i.first] = selectedSet;
        }
    }

    // Copy attributes and Delete the entries of other sets.
    for(auto i : visited) {
        // Copy the attributes to the new set.
        for(auto entry : setToStats[i].attrMap) {
            setToStats[selectedSet].attrMap.insert(entry);
        }
        // Delete the set.
        setToStats[i].attrMap.clear();
        setToStats.erase(i);
    }
    setToStats[selectedSet].noOfTuples = (int)(res+0.5);
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {

    FATALIF(!IsValidJoin(relNames, numToJoin), "The join requested is not a valid join.");
    
    double res = 1;
    double factor = 1.0;
    set<string> processedOp;
    while(parseTree != NULL) {
        OrList *orList = parseTree->left;
        parseTree = parseTree->rightAnd;
        double orFactor = 0;
        double orFactorCorrection = 1;
        processedOp.clear();
        while(orList!=NULL) {
            ComparisonOp *cOp = orList->left;
            orList = orList->rightOr;
            string opLeft(cOp->left->value);
            string attrLeft = opLeft;
            int idxToSplit = opLeft.find('.');
            if(idxToSplit!=string::npos) {
                attrLeft = attrLeft.substr(idxToSplit+1);
            }
            string opRight(cOp->right->value);
            string attrRight(opRight);
            idxToSplit = opRight.find('.');
            if(idxToSplit!=string::npos) {
                attrRight = attrRight.substr(idxToSplit+1);
            }

            bool newOperandProcessed = false;

            if(cOp->code < EQUALS) {
                // Insert name into processed attributes.
                newOperandProcessed = cOp->left->code == NAME ? processedOp.insert(opLeft).second : processedOp.insert(opLeft).second;
                
                // Assume 1/3 of the table is selected.
                orFactor += 1.0/3;
                if(newOperandProcessed) {
                    orFactorCorrection *= 1.0/3;
                }
                continue;
            }
            
            int tuples = 0;
            if(cOp->left->code==NAME) {
                FATALIF(attrToSet.count(opLeft)==0, "The attribute does not exist");
                tuples = max(FindNumberOfTuples(opLeft, attrLeft), tuples);
                newOperandProcessed = processedOp.insert(opLeft).second;
            }

            if(cOp->right->code==NAME) {
                FATALIF(attrToSet.count(opRight)==0, "The attribute does not exist.");
                tuples = max(FindNumberOfTuples(opRight, attrRight), tuples);
                newOperandProcessed = processedOp.insert(opRight).second;
            }
            // Add it to factor.
            orFactor += 1.0/tuples;
            if(newOperandProcessed) {
                orFactorCorrection *= 1.0/tuples;
            }
        }

        // Correct the estimation of orFactor a little.
        if(processedOp.size()>1  && orFactor > orFactorCorrection) {
            orFactor -= orFactorCorrection;
        }

        factor *= orFactor;
    }

    res = factor;

    set<int> visited;
    for(int i=0;i<numToJoin;i++) {
        string rel(relNames[i]);
        int relSet = relToSet[rel];
        if(visited.count(relSet) == 0) {
            // visit set.
            res *= setToStats[relSet].noOfTuples;
            visited.insert(relSet);
        }
    }
    return res;
}

