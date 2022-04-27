#include "QueryPlan.h"

QueryPlan::QueryPlan(Statistics *statistics, Query *query) {
    this->statistics = statistics;
    this->query = query;

    unordered_map<string, AndList *> tableSelectionAndList;
    vector<AndList *> join;
    vector<AndList *> minTupleCountJoin;

    ProcessRelationFiles();

    FindSelectionAndJoins(&tableSelectionAndList, &join);

    ProcessSelect(&tableSelectionAndList);

    FindMinTupleJoin(&join, &minTupleCountJoin);

    ProcessJoins(&minTupleCountJoin);

    ProcessGroupBy();

    ProcessSum();

    ProcessProject();

    ProcessDuplicateRemoval();
}

QueryPlan::~QueryPlan() {
}

void QueryPlan::ProcessRelationFiles() {
    TableList *table = query->tables;

    while (table) {
        SelectFileQueryPlanNode *selectFileNode = new SelectFileQueryPlanNode(NULL, NULL);
        Schema *schema = new Schema("catalog", table->tableName);
        schema->AliasAttributes(table->aliasAs);
        selectFileNode->outputSchema = schema;
        selectFileNode->outputPipeId = nextAvailablePipeId++;
        relationToGroupMap[table->aliasAs] = table->aliasAs;
        groupToQueryPlanNodeMap[table->aliasAs] = selectFileNode;
        table = table->next;
    }
}

void QueryPlan::ProcessSelect(unordered_map<string, AndList *> *tableSelectionAndList) {
    for (auto const &item : *tableSelectionAndList) {
        string relation = item.first;
        string group = relationToGroupMap[relation];
        SelectFileQueryPlanNode *node = dynamic_cast<SelectFileQueryPlanNode *>(groupToQueryPlanNodeMap[group]);
        AndList *andList = item.second;
        CNF *cnf = new CNF();
        Record *literal = new Record();
        cnf->GrowFromParseTree(andList, node->outputSchema, *literal); 

        if (node->selOp) {
            SelectPipeQueryPlanNode *selectPipeQueryPlanNode = new SelectPipeQueryPlanNode(node, NULL);
            selectPipeQueryPlanNode->outputSchema = node->outputSchema;
            selectPipeQueryPlanNode->outputPipeId = nextAvailablePipeId++;
            selectPipeQueryPlanNode->selOp = cnf;
            selectPipeQueryPlanNode->literal = literal;
            groupToQueryPlanNodeMap[group] = selectPipeQueryPlanNode;
            
        } else {
            node->selOp = cnf;
            node->literal = literal;
        }

        char *relations[] = {const_cast<char *>(relation.c_str())};
        statistics->Apply(andList, relations, 1);
    }
}

void QueryPlan::ProcessJoins(vector<AndList *> *joins) {
    for (AndList *andList : *joins) {

        string leftOperand = string(andList->left->left->left->value);
        string leftRelation = leftOperand.substr(0, leftOperand.find('.'));
        string leftGroup = relationToGroupMap[leftRelation];
        QueryPlanNode *leftNode = groupToQueryPlanNodeMap[leftGroup];

        string rightOperand = string(andList->left->left->right->value);
        string rightRelation = rightOperand.substr(0, rightOperand.find('.'));
        string rightGroup = relationToGroupMap[rightRelation];
        QueryPlanNode *rightNode = groupToQueryPlanNodeMap[rightGroup];

        CNF *cnf = new CNF();
        Record *literal = new Record();
        cnf->GrowFromParseTree(andList, leftNode->outputSchema, rightNode->outputSchema, *literal);

        JoinQueryPlanNode *joinQueryPlanNode = new JoinQueryPlanNode(leftNode, rightNode);
        Schema *outputSchema = new Schema(leftNode->outputSchema, rightNode->outputSchema);
        joinQueryPlanNode->outputSchema = outputSchema;
        joinQueryPlanNode->outputPipeId = nextAvailablePipeId++;
        joinQueryPlanNode->selOp = cnf;
        joinQueryPlanNode->literal = literal;
        string newGroupName;
        newGroupName.append(leftGroup).append("&").append(rightGroup);
        relationToGroupMap[leftRelation] = newGroupName;
        relationToGroupMap[rightRelation] = newGroupName;
        groupToQueryPlanNodeMap.erase(leftGroup);
        groupToQueryPlanNodeMap.erase(rightGroup);
        groupToQueryPlanNodeMap[newGroupName] = joinQueryPlanNode;
    }
}

void QueryPlan::ProcessGroupBy() {
    NameList *nameList = query->groupingAtts;
    if (!nameList)
        return;

    string group = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *node = groupToQueryPlanNodeMap[group];
    Schema *inputSchema = node->outputSchema;
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *inputSchema);
    OrderMaker *orderMaker = new OrderMaker(inputSchema, nameList);
    vector<int> keepMe;
    Schema *outputSchema = new Schema(&sumSchema, new Schema(inputSchema, nameList, &keepMe));

    GroupByQueryPlanNode *groupByQueryPlanNode = new GroupByQueryPlanNode(node, NULL);
    groupByQueryPlanNode->outputSchema = outputSchema;
    groupByQueryPlanNode->outputPipeId = nextAvailablePipeId++;
    groupByQueryPlanNode->groupAtts = orderMaker;
    groupByQueryPlanNode->computeMe = function;
    groupByQueryPlanNode->distinctFunc = query->distinctFunc;
    groupToQueryPlanNodeMap[group] = groupByQueryPlanNode;
}

void QueryPlan::ProcessSum() {
    if (query->groupingAtts || !query->finalFunction) {
        return;
    }
    
    string group = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *node = groupToQueryPlanNodeMap[group];
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *node->outputSchema);

    SumQueryPlanNode *sumQueryPlanNode = new SumQueryPlanNode(node, NULL);
    sumQueryPlanNode->outputSchema = &sumSchema;
    sumQueryPlanNode->outputPipeId = nextAvailablePipeId++;
    sumQueryPlanNode->computeMe = function;
    sumQueryPlanNode->distinctFunc = query->distinctFunc;
    groupToQueryPlanNodeMap[group] = sumQueryPlanNode;
}

void QueryPlan::ProcessProject() {
    NameList *attsToSelect = query->attsToSelect;
    if (query->finalFunction) {
        NameList *temp = new NameList();
        temp->name = SUM_ATT_NAME;
        temp->next = attsToSelect;
        attsToSelect = temp;
    }
    if (!attsToSelect)
        return;

    string group = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *node = groupToQueryPlanNodeMap[group];
    Schema *inputSchema = node->outputSchema;
    vector<int> *keepMeVector = new vector<int>;
    Schema *outputSchema = new Schema(inputSchema, attsToSelect, keepMeVector);
    int *keepMe = new int();
    keepMe = &keepMeVector->at(0);

    if (inputSchema->GetNumAtts() == outputSchema->GetNumAtts()) {
        return;
    }

    ProjectQueryPlanNode *projectQueryPlanNode = new ProjectQueryPlanNode(node, NULL);
    projectQueryPlanNode->outputSchema = outputSchema;
    projectQueryPlanNode->outputPipeId = nextAvailablePipeId++;
    projectQueryPlanNode->keepMe = keepMe;
    projectQueryPlanNode->numAttsInput = inputSchema->GetNumAtts();
    projectQueryPlanNode->numAttsOutput = outputSchema->GetNumAtts();
    groupToQueryPlanNodeMap[group] = projectQueryPlanNode;
}

void QueryPlan::ProcessDuplicateRemoval() {
    if (!query->distinctAtts)
        return;

    string group = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *node = groupToQueryPlanNodeMap[group];
    
    DuplicateRemovalQueryPlanNode *duplicateRemovalQueryPlanNode = new DuplicateRemovalQueryPlanNode(node, NULL);
    duplicateRemovalQueryPlanNode->outputPipeId = nextAvailablePipeId++;
    duplicateRemovalQueryPlanNode->outputSchema = node->outputSchema;
    groupToQueryPlanNodeMap[group] = duplicateRemovalQueryPlanNode;
}

void QueryPlan::FindSelectionAndJoins(unordered_map<string, AndList *> *tableSelections, vector<AndList *> *joins) {
    AndList *andList = query->andList;
    while(andList) {
        unordered_map<string, AndList *> currentSelection;
        OrList *orList = andList->left;
        
        while (orList) {
            Operand *leftOperand = orList->left->left;
            Operand *rightOperand = orList->left->right;
            OrList *newOrList = new OrList();
            newOrList->left = orList->left;
            if (leftOperand->code == NAME && rightOperand->code == NAME) {
                AndList *newAndList = new AndList();
                newAndList->left = newOrList;
                joins->push_back(newAndList);
            } else if (leftOperand->code == NAME || rightOperand->code == NAME) {
                Operand *operand = leftOperand->code == NAME ? leftOperand : rightOperand;
                string name = string(operand->value);
                string relation = name.substr(0, name.find('.'));
                if (currentSelection.find(relation) == currentSelection.end()) {
                    AndList *newAndList = new AndList();
                    newAndList->left = newOrList;
                    currentSelection[relation] = newAndList;
                } else {
                    OrList *currentOrList = currentSelection[relation]->left;
                    while (currentOrList->rightOr) {
                        currentOrList = currentOrList->rightOr;
                    }
                    currentOrList->rightOr = newOrList;
                }
            }
            orList = orList->rightOr;
        }

        for (auto const &item : currentSelection) {
            if (tableSelections->find(item.first) == tableSelections->end()) {
                (*tableSelections)[item.first] = item.second;
            } else {
                AndList *currentAndList = tableSelections->at(item.first);
                while (currentAndList->rightAnd) {
                    currentAndList = currentAndList->rightAnd;
                }
                currentAndList->rightAnd = item.second;
            }
        }
        andList = andList->rightAnd;
    }
}

void QueryPlan::FindMinTupleJoin(vector<AndList *> *join, vector<AndList *> *minTupleCountJoin) {
    if (join->size() < 1) {
        return;
    }
    int initialPermutation[join->size()];
    for (int i = 0; i < join->size(); i++) {
        initialPermutation[i] = i;
    }

    vector<int *> permutations;
    BuildJoinPermutation(initialPermutation, join->size(), join->size(), &permutations);
    int minIndex = -1;
    double minTupleCount = DBL_MAX;
    for (int i = 0; i < permutations.size(); i++) {
        double permutationTupleCount = 0.0;
        Statistics temp(*statistics);

        int relNamesIndex = 0;
        char **relNames = new char *[2 * join->size()];
        unordered_set<string> relNamesSet;
        for (int j = 0; j < join->size(); j++) {
            AndList *currentAndList = join->at(permutations[i][j]);
            string leftAttIdentifier = string(currentAndList->left->left->left->value);
            string rightAttIdentifier = string(currentAndList->left->left->right->value);
            string leftRelation = leftAttIdentifier.substr(0, leftAttIdentifier.find('.'));
            string rightRelation = rightAttIdentifier.substr(0, rightAttIdentifier.find('.'));

            if (relNamesSet.find(leftRelation) == relNamesSet.end()) {
                relNamesSet.insert(string(leftRelation));
                char *newRel = new char[leftRelation.length() + 1];
                strcpy(newRel, leftRelation.c_str());
                relNames[relNamesIndex++] = newRel;
            }
            if (relNamesSet.find(rightRelation) == relNamesSet.end()) {
                relNamesSet.insert(rightRelation);
                char *newRel = new char[rightRelation.length() + 1];
                strcpy(newRel, rightRelation.c_str());
                relNames[relNamesIndex++] = newRel;
            }
            permutationTupleCount += temp.Estimate(currentAndList, relNames, relNamesIndex);
            temp.Apply(currentAndList, relNames, relNamesIndex);
        }
        if (permutationTupleCount < minTupleCount) {
            minTupleCount = permutationTupleCount;
            minIndex = i;
        }
    }
    for (int i = 0; i < join->size(); i++) {
        minTupleCountJoin->push_back(join->at(permutations[minIndex][i]));
    }
}

void BuildJoinPermutation(int *initialPermutation, int p, int size, vector<int *> *permutations) {
    if (p == 1) {
        int *newPermutation = new int[size];
        for (int i = 0; i < size; i++) {
            newPermutation[i] = initialPermutation[i];
        }
        permutations->push_back(newPermutation);
        return;
    }
    for (int i = 0; i < p; i++) {
        BuildJoinPermutation(initialPermutation, p - 1, size, permutations);
        if (p % 2 == 0)
            swap(initialPermutation[i], initialPermutation[p - 1]);     
        else
            swap(initialPermutation[0], initialPermutation[p - 1]);
    }
}

void QueryPlan::Print() {
    cout << "INORDER TRAVERSAL: " << "\n";
    PrintQueryPlanInOrder(groupToQueryPlanNodeMap[groupToQueryPlanNodeMap.begin()->first]);
}

void QueryPlan::PrintQueryPlanInOrder(QueryPlanNode *node) {
    if (node == nullptr)
        return;
    PrintQueryPlanInOrder(node->left);
   
    PrintQueryPlanInOrder(node->right); 
     node->Print(); 
}