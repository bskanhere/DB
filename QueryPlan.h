#ifndef QUERY_PLAN_H
#define QUERY_PLAN_H

#include <vector>
#include <float.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "Function.h"
#include "QueryPlanNodes.h"
#include "RelOp.h"
#include "Comparison.h"

void HeapPermutation(int *a, int size, int n, vector<int *> *permutations);

struct Query {
    FuncOperator *finalFunction;
    TableList *tables;
    AndList *andList;
    NameList *groupingAtts;
    NameList *attsToSelect;
    int distinctAtts;
    int distinctFunc;
};


class QueryPlan {
private:
    Query *query;
    Statistics *statistics;
    unordered_map<string, QueryPlanNode *> groupNameToRelOpNode;
    unordered_map<string, string> relNameToGroupNameMap;

    int nextAvailablePipeId = 1;

    void ProcessRelationFiles();

    void SplitAndList(unordered_map<string, AndList *> *tableSelectionAndList, vector<AndList *> *joins);

    void ProcessSelect(unordered_map<string, AndList *> *tableSelectionAndList);

    void RearrangeJoins(vector<AndList *> *joins, vector<AndList *> *joins_arranged);

    void ApplyJoins(vector<AndList *> *joins);

    void ApplyGroupBy();

    void ApplySum();

    void ApplyDuplicateRemoval();

    void ApplyProject();

    static void PrintQueryPlanInOrder(QueryPlanNode *node);

    string GetResultantGroupName();

public:
    QueryPlan(Statistics *statistics, Query *query);

    ~QueryPlan();

    void Print();
};

#endif