#include "QueryPlan.h"

QueryPlan::QueryPlan(Statistics *statistics, Query *query) {
    this->statistics = statistics;
    this->query = query;

    unordered_map<string, AndList *> tableSelectionAndList;
    vector<AndList *> joins;
    vector<AndList *> joins_arranged;

    // Load all the tables using SelectFile RelOp.
    ProcessRelationFiles();

    // Split the AndList into selection and joins.
    FindSelectionAndJoins(&tableSelectionAndList, &joins);

    // Apply selection on tables using SelectPipe RelOp.
    ProcessSelect(&tableSelectionAndList);

    // Rearrange joins, so that number of intermediate tuples generated will be minimum.
    FindMinTupleJoin(&joins, &joins_arranged);
    
    // Apply joins on tables using Join RelOp.
    ProcessJoins(&joins_arranged);

    // Apply group by if it is in the query using GroupBy RelOp.
    ProcessGroupBy();

    // Apply Function if it is in the query using Sum RelOp.
    ProcessSum();

    // Apply Project using Project RelOp.
    ProcessProject();

    // Apply Duplicate removal using DuplicateRemoval RelOp if distinct is present.
    ProcessDuplicateRemoval();
}

QueryPlan::~QueryPlan() {
}

void QueryPlan::ProcessRelationFiles() {
    TableList *table = query->tables;

    while (table) {
        // Create SelectFileNode RelOp.
        SelectFileQueryPlanNode *selectFileNode = new SelectFileQueryPlanNode(NULL, NULL);

        // Create Schema with aliased attributes.
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
        OrList *orList = andList->left;

        string leftOperandName = string(orList->left->left->value);
        string tableName1 = leftOperandName.substr(0, leftOperandName.find('.'));
        string groupName1 = relationToGroupMap[tableName1];
        QueryPlanNode *inputRelOp1 = groupToQueryPlanNodeMap[groupName1];

        string rightOperandName = string(orList->left->right->value);
        string tableName2 = rightOperandName.substr(0, rightOperandName.find('.'));
        string groupName2 = relationToGroupMap[tableName2];
        QueryPlanNode *inputRelOp2 = groupToQueryPlanNodeMap[groupName2];

        // constructs CNF predicate
        CNF *cnf = new CNF();
        Record *literal = new Record();
        cnf->GrowFromParseTree(andList, inputRelOp1->outputSchema, inputRelOp2->outputSchema, *literal);

        JoinQueryPlanNode *joinNode = new JoinQueryPlanNode(inputRelOp1, inputRelOp2);

        Schema *outputSchema = new Schema(inputRelOp1->outputSchema, inputRelOp2->outputSchema);
        joinNode->outputSchema = outputSchema;
        joinNode->outputPipeId = nextAvailablePipeId++;

        joinNode->selOp = cnf;
        joinNode->literal = literal;

        string newGroupName;
        newGroupName.append(groupName1).append("&").append(groupName2);
        relationToGroupMap[tableName1] = newGroupName;
        relationToGroupMap[tableName2] = newGroupName;

        groupToQueryPlanNodeMap.erase(groupName1);
        groupToQueryPlanNodeMap.erase(groupName2);
        groupToQueryPlanNodeMap[newGroupName] = joinNode;
    }
}

void QueryPlan::ProcessGroupBy() {
    NameList *nameList = query->groupingAtts;
    if (!nameList)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *inputRelOpNode = groupToQueryPlanNodeMap[finalGroupName];

    Schema *groupByInputSchema = inputRelOpNode->outputSchema;

    // Build Compute function.
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *groupByInputSchema);

    // Build OrderMaker
    OrderMaker *orderMaker = new OrderMaker(groupByInputSchema, nameList);

    // Build Schema and keepMe From nameList
    vector<int> keepMeVector;
    Schema *groupedAttsSchema = new Schema(groupByInputSchema, nameList, &keepMeVector);

    Schema *outputSchema = new Schema(&sumSchema, groupedAttsSchema);

    // Create Group by Node.
    GroupByQueryPlanNode *groupByNode = new GroupByQueryPlanNode(inputRelOpNode, NULL);

    groupByNode->outputSchema = outputSchema;
    groupByNode->outputPipeId = nextAvailablePipeId++;

    groupByNode->groupAtts = orderMaker;
    groupByNode->computeMe = function;
    groupByNode->distinctFunc = query->distinctFunc;

    groupToQueryPlanNodeMap[finalGroupName] = groupByNode;
}

void QueryPlan::ProcessSum() {
    if (query->groupingAtts || !query->finalFunction) {
        return;
    }

    // Get Resultant RelOp Node.
    string finalGroupName = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *inputRelOpNode = groupToQueryPlanNodeMap[finalGroupName];

    // Build Compute function.
    Function *function = new Function();
    function->GrowFromParseTree(query->finalFunction, *inputRelOpNode->outputSchema);

    SumQueryPlanNode *sumNode = new SumQueryPlanNode(inputRelOpNode, NULL);
    sumNode->outputSchema = &sumSchema;
    sumNode->outputPipeId = nextAvailablePipeId++;

    sumNode->computeMe = function;
    sumNode->distinctFunc = query->distinctFunc;

    groupToQueryPlanNodeMap[finalGroupName] = sumNode;
}

void QueryPlan::ProcessProject() {
    NameList *attsToSelect = query->attsToSelect;

    if (query->finalFunction) {
        NameList *sumAtt = new NameList();
        sumAtt->name = SUM_ATT_NAME;
        sumAtt->next = attsToSelect;
        attsToSelect = sumAtt;
    }

    if (!attsToSelect)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *inputRelOpNode = groupToQueryPlanNodeMap[finalGroupName];

    Schema *inputSchema = inputRelOpNode->outputSchema;

    vector<int> *keepMeVector = new vector<int>;
    Schema *outputSchema = new Schema(inputSchema, attsToSelect, keepMeVector);

    int *keepMe = new int();
    keepMe = &keepMeVector->at(0);

    if (inputSchema->GetNumAtts() == outputSchema->GetNumAtts()) {
        return;
    }

    // Create Project RelOp Node
    ProjectQueryPlanNode *projectNode = new ProjectQueryPlanNode(inputRelOpNode, NULL);

    projectNode->outputSchema = outputSchema;
    projectNode->outputPipeId = nextAvailablePipeId++;

    projectNode->keepMe = keepMe;
    projectNode->numAttsInput = inputSchema->GetNumAtts();
    projectNode->numAttsOutput = outputSchema->GetNumAtts();

    groupToQueryPlanNodeMap[finalGroupName] = projectNode;
}

void QueryPlan::ProcessDuplicateRemoval() {
    if (!query->distinctAtts)
        return;

    // Get Resultant RelOp Node.
    string finalGroupName = groupToQueryPlanNodeMap.begin()->first;
    QueryPlanNode *inputRelOpNode = groupToQueryPlanNodeMap[finalGroupName];

    // Create Distinct RelOp Node.
    DuplicateRemovalQueryPlanNode *duplicateRemovalNode = new DuplicateRemovalQueryPlanNode(inputRelOpNode, NULL);

    duplicateRemovalNode->outputPipeId = nextAvailablePipeId++;
    duplicateRemovalNode->outputSchema = inputRelOpNode->outputSchema;

    //duplicateRemovalNode->inputSchema = inputRelOpNode->outputSchema;

    groupToQueryPlanNodeMap[finalGroupName] = duplicateRemovalNode;
}

void QueryPlan::FindSelectionAndJoins(unordered_map<string, AndList *> *tableSelectionAndList, vector<AndList *> *joins) {

    AndList *andList = query->andList;
    while(andList) {
        unordered_map<string, AndList *> currentTableSelectionAndList;

        OrList *orList = andList->left;

        while (orList) {

            Operand *leftOperand = orList->left->left;
            Operand *rightOperand = orList->left->right;

            // Duplicate OrList
            OrList *newOrList = new OrList();
            newOrList->left = orList->left;
            if (leftOperand->code == NAME && rightOperand->code == NAME) {
                AndList *newAndList = new AndList();

                // Add to new or list to and list.
                newAndList->left = newOrList;

                // Push newly created and list to joins vector.
                joins->push_back(newAndList);
            } else if (leftOperand->code == NAME || rightOperand->code == NAME) {
                Operand *nameOperand = leftOperand->code == NAME ? leftOperand : rightOperand;
                string name = string(nameOperand->value);
                string relationName = name.substr(0, name.find('.'));

                if (currentTableSelectionAndList.find(relationName) == currentTableSelectionAndList.end()) {
                    AndList *newAndList = new AndList();
                    newAndList->left = newOrList;
                    currentTableSelectionAndList[relationName] = newAndList;
                } else {
                    OrList *currentOrList = currentTableSelectionAndList[relationName]->left;

                    while (currentOrList->rightOr) {
                        currentOrList = currentOrList->rightOr;
                    }
                    currentOrList->rightOr = newOrList;
                }
            }
            orList = orList->rightOr;
        }

        // Iterate and merge and lists
        for (auto const &item : currentTableSelectionAndList) {
            if (tableSelectionAndList->find(item.first) == tableSelectionAndList->end()) {
                (*tableSelectionAndList)[item.first] = item.second;
            } else {
                AndList *currentAndList = tableSelectionAndList->at(item.first);

                while (currentAndList->rightAnd) {
                    currentAndList = currentAndList->rightAnd;
                }

                currentAndList->rightAnd = item.second;
            }
        }

        andList = andList->rightAnd;
    }
}

void QueryPlan::FindMinTupleJoin(vector<AndList *> *joins, vector<AndList *> *joins_arranged) {
    int n = joins->size();
    if (n < 1) {
        return;
    }
    int initialPermutation[n];
    for (int i = 0; i < n; i++) {
        initialPermutation[i] = i;
    }

    vector<int *> permutations;

    BuildJoinPermutation(initialPermutation, joins->size(), joins->size(), &permutations);
    int minI = -1;
    double minIntermediateTuples = DBL_MAX;
    for (int i = 0; i < permutations.size(); i++) {
        double permutationIntermediateTuples = 0.0;
        Statistics dummy(*statistics);

        int relNamesIndex = 0;
        char **relNames = new char *[2 * n];
        unordered_set<string> relNamesSet;
        for (int j = 0; j < n; j++) {
            AndList *currentAndList = joins->at(permutations[i][j]);
            string attNameWithRelName1 = string(currentAndList->left->left->left->value);
            string attNameWithRelName2 = string(currentAndList->left->left->right->value);
            cout << attNameWithRelName1 << " " << attNameWithRelName2 << endl;
            string relName1 = attNameWithRelName1.substr(0, attNameWithRelName1.find('.'));
            string relName2 = attNameWithRelName2.substr(0, attNameWithRelName2.find('.'));

            if (relNamesSet.find(relName1) == relNamesSet.end()) {
                relNamesSet.insert(string(relName1));
                char *newRel = new char[relName1.length() + 1];
                strcpy(newRel, relName1.c_str());
                relNames[relNamesIndex++] = newRel;
            }

            if (relNamesSet.find(relName2) == relNamesSet.end()) {
                relNamesSet.insert(relName2);
                char *newRel = new char[relName2.length() + 1];
                strcpy(newRel, relName2.c_str());
                relNames[relNamesIndex++] = newRel;
            }

            double intermediate = dummy.Estimate(currentAndList, relNames, relNamesIndex);
            permutationIntermediateTuples += intermediate;
            dummy.Apply(currentAndList, relNames, relNamesIndex);

        }
        if (permutationIntermediateTuples < minIntermediateTuples) {
            minIntermediateTuples = permutationIntermediateTuples;
            minI = i;
        }
    }
    for (int i = 0; i < n; i++) {
        joins_arranged->push_back(joins->at(permutations[minI][i]));
    }
}

void BuildJoinPermutation(int *initialPermutation, int p, int size, vector<int *> *permutations) {
    // if size becomes 1 then prints the obtained
    // permutation
    if (p == 1) {
        // Add new Permutation in the permutations vector.
        int *newPermutation = new int[size];
        for (int i = 0; i < size; i++) {
            newPermutation[i] = initialPermutation[i];
        }
        permutations->push_back(newPermutation);
        return;
    }

    for (int i = 0; i < p; i++) {
        BuildJoinPermutation(initialPermutation, p - 1, size, permutations);

        // if size is odd, swap first and last
        // element
        if (p % 2 == 1)
            swap(initialPermutation[0], initialPermutation[p - 1]);

            // If size is even, swap ith and last
            // element
        else
            swap(initialPermutation[i], initialPermutation[p - 1]);
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
    node->Print();
    PrintQueryPlanInOrder(node->right);
    
}