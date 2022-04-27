#include "Statistics.h"

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    for(auto &groupTupleCountMapItem : copyMe.groupTupleCountMap) {
        groupTupleCountMap[groupTupleCountMapItem.first] = groupTupleCountMapItem.second;
    }

    for(auto &attDistinctCountMapItem : copyMe.attDistinctCountMap) {
        attDistinctCountMap[attDistinctCountMapItem.first] = attDistinctCountMapItem.second;
    }

    for(auto &groupToRelationsMapItem : copyMe.groupToRelationsMap) {
        unordered_set<string> relations;
        for (auto &relation : groupToRelationsMapItem.second) {
            relations.insert(relation);
        }
        groupToRelationsMap[groupToRelationsMapItem.first] = relations;
    }

    for(auto &relationToGroupMapItem : copyMe.relationToGroupMap) {
        relationToGroupMap[relationToGroupMapItem.first] = relationToGroupMapItem.second;
    }
}

Statistics::~Statistics() {
}

void Statistics::AddRel(char *relName, int numTuples) {
    if(groupToRelationsMap.find(relName) == groupToRelationsMap.end()) {
        unordered_set<string> relations;
        relations.insert(relName);
        relationToGroupMap[relName] = relName;
        groupToRelationsMap[relName] = relations;
        groupTupleCountMap[relName] = numTuples;
    } else if(relationToGroupMap[relName] == relName) {
        groupTupleCountMap[relName] = numTuples;
    } else {
        cerr << "Relation is joined already" << endl;
        exit(1);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string attIdentifier = string(relName) + "." + attName;

    if(attDistinctCountMap.find(attIdentifier) != attDistinctCountMap.end() && relationToGroupMap[relName] != relName) {
        cerr << "Relation is joined already so cant upadte attribute."  << endl;
        exit(1);
    }
    if(numDistincts == -1) {
        numDistincts = groupTupleCountMap[relationToGroupMap[relName]];
    }
    attDistinctCountMap[attIdentifier] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName) {

    AddRel(newName, groupTupleCountMap[relationToGroupMap[oldName]]);

    for(auto attDistinctCountMapItem : attDistinctCountMap) {
        string attIdentifier = attDistinctCountMapItem.first;
        string relation = attIdentifier.substr(0, attIdentifier.find('.'));

        if(relation == string(oldName)) {
            string attName = attIdentifier.substr(attIdentifier.find('.') + 1);
            attIdentifier = string(newName) + "." + attName;
            attDistinctCountMap[attIdentifier] = attDistinctCountMapItem.second;
        }
    }
}

void Statistics::Read(char *fromWhere) {
    ifstream fIn;
    fIn.open(fromWhere);

    if (!fIn.is_open()) return;

    string readLine;

    getline(fIn, readLine);
    getline(fIn, readLine);
    int size = stoi(readLine);
    groupTupleCountMap.clear();
    for (int i = 0; i < size; i++) {
        getline(fIn, readLine);
        string groupName = readLine;
        getline(fIn, readLine);
        int numOfTuples = stoi(readLine);
        groupTupleCountMap[groupName] = numOfTuples;
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    size = stoi(readLine);
    attDistinctCountMap.clear();
    for (int i = 0; i < size; i++) {
        getline(fIn, readLine);
        string attIdentifier = readLine;
        getline(fIn, readLine);
        int numOfDistinct = stoi(readLine);
        attDistinctCountMap[attIdentifier] = numOfDistinct;
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    size = stoi(readLine);
    groupToRelationsMap.clear();
    for (int i = 0; i < size; i++) {
        getline(fIn, readLine);
        string groupName = readLine;

        unordered_set<string> relations;
        getline(fIn, readLine);
        stringstream s_stream(readLine);
        while (s_stream.good()) {
            getline(s_stream, readLine, '|');
            relations.insert(readLine);
        }
        
        groupToRelationsMap[groupName] = relations;
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    size = stoi(readLine);
    relationToGroupMap.clear();
    for (int i = 0; i < size; i++) {
        getline(fIn, readLine);
        string relation = readLine;
        getline(fIn, readLine);
        string groupName = readLine;
        relationToGroupMap[relation] = groupName;
    }

}

void Statistics::Write(char *fromWhere) {
    ofstream fOut;
    fOut.open(fromWhere);

    fOut << "Group Tuple Count -->" << endl;
    fOut << groupTupleCountMap.size() << endl;
    for (auto &i: groupTupleCountMap) {
        fOut << i.first << endl;
        fOut << i.second << endl;
    }

    fOut << "Attributes -->" << endl;
    fOut << attDistinctCountMap.size() << endl;
    for (auto &i: attDistinctCountMap) {
        fOut << i.first << endl;
        fOut << i.second << endl;
    }

    fOut << "GroupName to Relations -->" << endl;
    fOut << groupToRelationsMap.size() << endl;
    for (auto &i: groupToRelationsMap) {
        auto j = i.second.begin();
        fOut << i.first << endl;
        fOut << *(j);
        while (++j != i.second.end()) {
            fOut << "|" << *(j);
        }
        fOut << endl;
    }

    fOut << "Relation Name to Group Name -->" << endl;
    fOut << relationToGroupMap.size() << endl;
    for (auto &i: relationToGroupMap) {
        fOut << i.first << endl;
        fOut << i.second << endl;
    }
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    unordered_set<string> relNamesSet;
    unordered_set<string> relationsInResult;

    
    //validation of realtions in Parse Tree
    for (int i = 0; i < numToJoin; i++) {
        relNamesSet.insert(relNames[i]);
        if (relationToGroupMap.find(relNames[i]) == relationToGroupMap.end()) {
            cerr << "Relation " << relNames[i] << " is not present in statistics" << endl;
            exit(1);
        }
        for (const string &relName : groupToRelationsMap[relationToGroupMap[relNames[i]]]) {
            relationsInResult.insert(relName);
        }
    }

    for (const string &relName : relNamesSet) {
        relationsInResult.erase(relName);
    }

    if (!relationsInResult.empty()) {
        cerr << "Associated Relations seem wrong"  << endl;
        exit(1);
    }

    string finalGroupName;
    unordered_map<string, double> attProbabilityMap;
    while (parseTree) {
        attProbabilityMap.clear();

        OrList *orList = parseTree->left;
        while (orList) {
            if(orList->left->left->code == NAME && orList->left->right->code == NAME) {

                ProcessAttributeName(orList->left->left, &relNamesSet);
                ProcessAttributeName(orList->left->right, &relNamesSet);

                if (orList->left->code != EQUALS) {
                    cerr << "Join should be implemented only for EQUAL." << endl;
                    exit(1);
                }

                string leftAttNameWithRelName = string(orList->left->left->value);
                int numOfDistinctInLeftAtt = attDistinctCountMap[leftAttNameWithRelName];
                string leftRelName = leftAttNameWithRelName.substr(0, leftAttNameWithRelName.find('.'));
                string leftGroupName = relationToGroupMap[leftRelName];
                double numOfTuplesInLeftGroup = groupTupleCountMap[leftGroupName];

                string rightAttNameWithRelName = string(orList->left->right->value);
                int numOfDistinctInRightAtt = attDistinctCountMap[rightAttNameWithRelName];
                string rightRelName = rightAttNameWithRelName.substr(0, rightAttNameWithRelName.find('.'));
                string rightGroupName = relationToGroupMap[rightRelName];
                double numOfTuplesInRightGroup = groupTupleCountMap[rightGroupName];

                double numOfTuplesPerAttValueInLeft = (numOfTuplesInLeftGroup / numOfDistinctInLeftAtt);
                double numOfTuplesPerAttValueInRight = (numOfTuplesInRightGroup / numOfDistinctInRightAtt);

                double numOfTuplesAfterJoin = numOfTuplesPerAttValueInLeft * numOfTuplesPerAttValueInRight * min(numOfDistinctInLeftAtt, numOfDistinctInRightAtt);
                string newGroupName;
                newGroupName.append(leftGroupName).append("&").append(rightGroupName);

                groupTupleCountMap.erase(leftGroupName);
                groupTupleCountMap.erase(rightGroupName);

                groupTupleCountMap[newGroupName] = numOfTuplesAfterJoin;
                unordered_set<string> newRelationSet;

                for (const string &relName : groupToRelationsMap[leftGroupName]) {
                    relationToGroupMap[relName] = newGroupName;
                    newRelationSet.insert(relName);
                }
                groupToRelationsMap.erase(leftGroupName);

                for (const string &relName : groupToRelationsMap[rightGroupName]) {
                    relationToGroupMap[relName] = newGroupName;
                    newRelationSet.insert(relName);
                }
                groupToRelationsMap.erase(rightGroupName);

                groupToRelationsMap[newGroupName] = newRelationSet;
                finalGroupName = newGroupName;
            } else if (orList->left->left->code == NAME ^ orList->left->right->code == NAME) {
                Operand *nameOperand = orList->left->left->code == NAME ? orList->left->left : orList->left->right;
                ProcessAttributeName(nameOperand, &relNamesSet);

                string attIdentifier = string(nameOperand->value);
                string relName = attIdentifier.substr(0, attIdentifier.find('.'));
                if (orList->left->code == EQUALS) {
                    double attProbability = 1.0 / attDistinctCountMap[attIdentifier];
                    if (attProbabilityMap.find(attIdentifier) == attProbabilityMap.end()) {
                        attProbabilityMap[attIdentifier] = attProbability;
                    } else {
                        attProbabilityMap[attIdentifier] += attProbability;
                    }
                } else {
                    if (attProbabilityMap.find(attIdentifier) == attProbabilityMap.end()) {
                        attProbabilityMap[attIdentifier] = (1.0 / 3.0);
                    }
                }
                finalGroupName = relationToGroupMap[relName];
            } else {
                cerr << "Invalid Operands" << endl;
                exit(1);
            }
            orList = orList->rightOr;
        }

        if (!attProbabilityMap.empty()) {
            double numOfTuples = groupTupleCountMap[finalGroupName];
            double unionProbablity = 0.0;

            if (attProbabilityMap.size() == 1) {
                unionProbablity = (*attProbabilityMap.begin()).second;
            } else {
                double sum = 0.0;
                double mul = 1.0;

                for (const auto &attProbabilityMapItem : attProbabilityMap) {
                    sum += attProbabilityMapItem.second;
                    mul *= attProbabilityMapItem.second;
                }
                unionProbablity = sum - mul;
            }
            numOfTuples *= unionProbablity;
            groupTupleCountMap[finalGroupName] = numOfTuples;
        }
        parseTree = parseTree->rightAnd;
    }

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    Statistics temp(*this);

    temp.Apply(parseTree, relNames, numToJoin);
    unordered_set<string> groups;
    for (int i = 0; i < numToJoin; i++) {
        groups.insert(temp.relationToGroupMap[relNames[i]]);
    }

    if (groups.size() != 1) {
        cerr << "Error" << endl;
        exit(1);
    }

    return temp.groupTupleCountMap[*groups.begin()];
}

void Statistics::ProcessAttributeName(Operand *operand, unordered_set<string> *relNames) {
    string operandValue = operand->value;
    if (operandValue.find('.') == string::npos) {
        bool found = false;
        for (const string &relName : *relNames) {
            string attIdentifier = relName + "." + string(operandValue);
            if (attDistinctCountMap.find(attIdentifier) != attDistinctCountMap.end()) {
                found = true;
                char *newOperandValue = new char[attIdentifier.size() + 1];
                strcpy(newOperandValue, attIdentifier.c_str());
                operand->value = newOperandValue;
                break;

            }
        }
        if (!found) {
            cerr << "No relation contains given attribute " << operandValue << endl;
            exit(1);
        }

    }
}