#include "Statistics.h"

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
    for (auto &x : copyMe.groupTupleCountMap) {
        groupTupleCountMap[x.first] = x.second;
    }

    for (auto &attDistinctCountMapItem : copyMe.attDistinctCountMap) {
        attDistinctCountMap[attDistinctCountMapItem.first] = attDistinctCountMapItem.second;
    }

    for (auto &groupToRelationsMapItem : copyMe.groupToRelationsMap) {
        unordered_set<string> newRelationSet;
        for (auto &relName : groupToRelationsMapItem.second) {
            newRelationSet.insert(relName);
        }
        groupToRelationsMap[groupToRelationsMapItem.first] = newRelationSet;
    }

    for (auto &relationToGroupMapItem : copyMe.relationToGroupMap) {
        relationToGroupMap[relationToGroupMapItem.first] = relationToGroupMapItem.second;
    }
}

Statistics::~Statistics() {
}

void Statistics::AddRel(char *relName, int numTuples) {

    // If the relation is not present. Add new relation and corresponding entries in map.
    if (groupToRelationsMap.find(relName) == groupToRelationsMap.end()) {
        unordered_set<string> newRelationSet;
        newRelationSet.insert(relName);

        relationToGroupMap[relName] = relName;
        groupToRelationsMap[relName] = newRelationSet;
        groupTupleCountMap[relName] = numTuples;

        // If the relation is not yet joined, update the number of tuples.
    } else if (relationToGroupMap[relName] == relName) {
        groupTupleCountMap[relName] = numTuples;

        // Otherwise throw an error, as table is already joined.
    } else {
        cerr << "Relation is already joined with some table.\n";
        exit(1);
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string attNameWithRelName = string(relName) + "." + attName;

    if (attDistinctCountMap.find(attNameWithRelName) != attDistinctCountMap.end()
        && relationToGroupMap[relName] != relName) {
        cerr << "Relation is already joined with some table. Hence attribute can't be updated.\n";
        exit(1);
    }

    if (numDistincts == -1) {
        numDistincts = groupTupleCountMap[relationToGroupMap[relName]];
    }
    attDistinctCountMap[attNameWithRelName] = numDistincts;
}

void Statistics::CopyRel(char *oldName, char *newName) {

    // Add new relation.
    AddRel(newName, groupTupleCountMap[relationToGroupMap[oldName]]);

    // Add attributes in the new relation.
    for (auto attDistinctCountMapItem : attDistinctCountMap) {
        string attNameWithRelName = attDistinctCountMapItem.first;
        string relName = attNameWithRelName.substr(0, attNameWithRelName.find('.'));

        if (relName == string(oldName)) {
            string attName = attNameWithRelName.substr(attNameWithRelName.find('.') + 1);
            attNameWithRelName = string(newName) + "." + attName;
            attDistinctCountMap[attNameWithRelName] = attDistinctCountMapItem.second;
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
    int setNameToRelationMapSize = stoi(readLine);
    groupTupleCountMap.clear();
    for (int i = 0; i < setNameToRelationMapSize; i++) {
        getline(fIn, readLine, '=');
        string groupName = readLine;
        getline(fIn, readLine);
        int numOfTuples = stoi(readLine);
        groupTupleCountMap[groupName] = numOfTuples;
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int attDistinctCountMapSize = stoi(readLine);
    attDistinctCountMap.clear();
    for (int i = 0; i < attDistinctCountMapSize; i++) {
        getline(fIn, readLine, '=');
        string attName = readLine;
        getline(fIn, readLine);
        int numOfDistinct = stoi(readLine);
        attDistinctCountMap[attName] = numOfDistinct;
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int setNameToSetOfRelationsMapSize = stoi(readLine);
    groupToRelationsMap.clear();
    for (int i = 0; i < setNameToSetOfRelationsMapSize; i++) {
        getline(fIn, readLine, '=');
        string groupName = readLine;

        unordered_set<string> newRelationSet;
        groupToRelationsMap[groupName] = newRelationSet;

        getline(fIn, readLine);
        stringstream s_stream(readLine);

        while (s_stream.good()) {
            getline(s_stream, readLine, ',');
            groupToRelationsMap[groupName].insert(readLine);
        }
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int relNameToSetNameMapSize = stoi(readLine);
    relationToGroupMap.clear();
    for (int i = 0; i < attDistinctCountMapSize; i++) {
        getline(fIn, readLine, '=');
        string relName = readLine;
        getline(fIn, readLine);
        string groupName = readLine;
        relationToGroupMap[relName] = groupName;
    }

}

void Statistics::Write(char *fromWhere) {
    ofstream fOut;
    fOut.open(fromWhere);

    fOut << "**************** Group Relations *****************\n";
    fOut << groupTupleCountMap.size() << "\n";
    for (auto &x: groupTupleCountMap) {
        fOut << x.first << "=" << x.second << "\n";
    }

    fOut << "**************** Attributes ******************\n";
    fOut << attDistinctCountMap.size() << "\n";
    for (auto &x: attDistinctCountMap) {
        fOut << x.first << "=" << x.second << "\n";
    }

    fOut << "****************** GroupName to Relations ****************\n";
    fOut << groupToRelationsMap.size() << "\n";
    for (auto &x: groupToRelationsMap) {
        auto secondIterator = x.second.begin();
        fOut << x.first << "=" << *(secondIterator);
        while (++secondIterator != x.second.end()) {
            fOut << "," << *(secondIterator);
        }
        fOut << "\n";
    }

    fOut << "******************** Relation Name to Group Name *****************\n";
    fOut << relationToGroupMap.size() << "\n";
    for (auto &x: relationToGroupMap) {
        fOut << x.first << "=" << x.second << "\n";
    }
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    unordered_set<string> relNamesSet;
    for (int i = 0; i < numToJoin; i++) {
        relNamesSet.insert(relNames[i]);
    }

    //validation
    ValidateApplyOnRelations(&relNamesSet);
    PreProcessApplyOnAttributes(parseTree, &relNamesSet);

    string resultantGroupName;
    unordered_map<string, double> attNameToProbabilitiesMap;
    while (parseTree) {
        attNameToProbabilitiesMap.clear();

        OrList *orList = parseTree->left;
        while (orList) {
            ComparisonOp *currentComparisonOp = orList->left;
            Operand *leftOperand = currentComparisonOp->left;
            Operand *rightOperand = currentComparisonOp->right;
            int comparisonOperator = currentComparisonOp->code;

            // if both side of a operator, there is a name, then Join the two tables.
            if (leftOperand->code == NAME && rightOperand->code == NAME) {

                if (comparisonOperator != EQUALS) {
                    cerr << "Join is not implemented for other than Equals operator\n";
                    exit(1);
                }

                string leftAttNameWithRelName = string(leftOperand->value);
                int numOfDistinctInLeftAtt = attDistinctCountMap[leftAttNameWithRelName];
                string leftRelName = leftAttNameWithRelName.substr(0, leftAttNameWithRelName.find('.'));
                string leftGroupName = relationToGroupMap[leftRelName];
                double numOfTuplesInLeftGroup = groupTupleCountMap[leftGroupName];

                string rightAttNameWithRelName = string(rightOperand->value);
                int numOfDistinctInRightAtt = attDistinctCountMap[rightAttNameWithRelName];
                string rightRelName = rightAttNameWithRelName.substr(0, rightAttNameWithRelName.find('.'));
                string rightGroupName = relationToGroupMap[rightRelName];
                double numOfTuplesInRightGroup = groupTupleCountMap[rightGroupName];

                if (leftGroupName == rightGroupName) {
                    cerr << "Table " << leftRelName << " is already joined with " << rightGroupName << ".\n";
                    exit(1);
                }

                double numOfTuplesPerAttValueInLeft = (numOfTuplesInLeftGroup / numOfDistinctInLeftAtt);
                double numOfTuplesPerAttValueInRight = (numOfTuplesInRightGroup / numOfDistinctInRightAtt);

                double numOfTuplesAfterJoin = numOfTuplesPerAttValueInLeft
                                              * numOfTuplesPerAttValueInRight
                                              * min(numOfDistinctInLeftAtt, numOfDistinctInRightAtt);

                string newGroupName;
                newGroupName.append(leftGroupName).append("&").append(rightGroupName);

                // Delete leftGroups and rightGroups for Different map.
                groupTupleCountMap.erase(leftGroupName);
                groupTupleCountMap.erase(rightGroupName);

                // Create new group relation.
                groupTupleCountMap[newGroupName] = numOfTuplesAfterJoin;
                unordered_set<string> newRelationSet;


                // Change groups of leftGroups and rightGroups relations.
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
                resultantGroupName = newGroupName;
            }
                // Otherwise it is a select operation.
            else if (leftOperand->code == NAME ^ rightOperand->code == NAME) {
                Operand *nameOperand = leftOperand->code == NAME ? leftOperand : rightOperand;
                string attNameWithRelName = string(nameOperand->value);
                string relName = attNameWithRelName.substr(0, attNameWithRelName.find('.'));
                if (currentComparisonOp->code == EQUALS) {
                    double probabilityFraction = 1.0 / attDistinctCountMap[attNameWithRelName];
                    if (attNameToProbabilitiesMap.find(attNameWithRelName) == attNameToProbabilitiesMap.end()) {
                        attNameToProbabilitiesMap[attNameWithRelName] = probabilityFraction;
                    } else {
                        attNameToProbabilitiesMap[attNameWithRelName] += probabilityFraction;
                    }
                } else {
                    if (attNameToProbabilitiesMap.find(attNameWithRelName) == attNameToProbabilitiesMap.end()) {
                        attNameToProbabilitiesMap[attNameWithRelName] = (1.0 / 3.0);
                    } else {

                    }
                }
                resultantGroupName = relationToGroupMap[relName];
            } else {
                cerr << "left operand " << string(leftOperand->value) << " and right operand "
                     << string(rightOperand->value) << " are not valid.\n";
                exit(1);
            }
            orList = orList->rightOr;
        }

        if (!attNameToProbabilitiesMap.empty()) {
            double numOfTuples = groupTupleCountMap[resultantGroupName];
            double multiplicationFactor = 0.0;

            if (attNameToProbabilitiesMap.size() == 1) {
                multiplicationFactor = (*attNameToProbabilitiesMap.begin()).second;
            } else {
                double additionFactor = 0.0;
                double subtractionFactor = 1.0;

                for (const auto &attNameToProbabilitiesMapItem : attNameToProbabilitiesMap) {
                    additionFactor += attNameToProbabilitiesMapItem.second;
                    subtractionFactor *= attNameToProbabilitiesMapItem.second;
                }
                multiplicationFactor = additionFactor - subtractionFactor;

            }

            numOfTuples *= multiplicationFactor;


            groupTupleCountMap[resultantGroupName] = numOfTuples;
        }
        parseTree = parseTree->rightAnd;
    }

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
    Statistics dummy(*this);

    dummy.Apply(parseTree, relNames, numToJoin);
    unordered_set<string> groupNames;
    for (int i = 0; i < numToJoin; i++) {
        groupNames.insert(dummy.relationToGroupMap[relNames[i]]);
    }

    if (groupNames.size() != 1) {
        cerr << "Error while estimating.\n";
        exit(1);
    }

    return dummy.groupTupleCountMap[*groupNames.begin()];
}

void Statistics::ValidateApplyOnRelations(unordered_set<string> *relNames) {
    unordered_set<string> setNamesToJoin;
    for (const string &relName : *relNames) {
        if (relationToGroupMap.find(relName) == relationToGroupMap.end()) {
            cerr << "Relation " << relName << " is not present in statistics.\n";
            exit(1);
        }
        setNamesToJoin.insert(relationToGroupMap[relName]);
    }

    unordered_set<string> relationsInResult;
    for (const string &setName : setNamesToJoin) {
        for (const string &relName : groupToRelationsMap[setName]) {
            relationsInResult.insert(relName);
        }
    }

    for (const string &relName : *relNames) {
        relationsInResult.erase(relName);
    }

    if (!relationsInResult.empty()) {
        cerr << "Relation association doesn't make sense\n";
        exit(1);
    }
}

void Statistics::PreProcessApplyOnAttributes(struct AndList *parseTree, unordered_set<string> *relNames) {
    while (parseTree) {
        OrList *orList = parseTree->left;
        while (orList) {
            if (orList->left->left->code == NAME) {
                PreProcessNameOperand(orList->left->left, relNames);
            }
            if (orList->left->right->code == NAME) {
                PreProcessNameOperand(orList->left->right, relNames);
            }
            orList = orList->rightOr;
        }
        parseTree = parseTree->rightAnd;
    }
}

void Statistics::PreProcessNameOperand(Operand *operand, unordered_set<string> *relNames) {
    string operandValue = operand->value;

    // If operandValue contains relation name i.e name is of the form "relName.attName".
    if (operandValue.find('.') != string::npos) {
        string relationName = operandValue.substr(0, operandValue.find('.'));
        if (attDistinctCountMap.find(operandValue) == attDistinctCountMap.end()) {
            cerr << "Attribute " << string(operandValue) << " is not present in Statistics.\n";
        }
        if (relNames->find(relationName) == relNames->end()) {
            cerr << "Attribute is not linked with any rel names given.\n";
        }
    } else {
        bool relFound = false;
        for (const string &relName : *relNames) {
            string attributeNameWithRelName = relName + "." + string(operandValue);
            if (attDistinctCountMap.find(attributeNameWithRelName) != attDistinctCountMap.end()) {
                relFound = true;
                char *newOperandValue = new char[attributeNameWithRelName.size() + 1];
                strcpy(newOperandValue, attributeNameWithRelName.c_str());
                operand->value = newOperandValue;
                break;
            }
        }
        if (!relFound) {
            cerr << "No relation contains attribute " << operandValue << ".\n";
            exit(1);
        }
    }
}