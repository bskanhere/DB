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

void BuildJoinPermutation(int *initialPermutation, int p, int size, vector<int *> *permutations);

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
    unordered_map<string, QueryPlanNode *> groupToQueryPlanNodeMap;
    unordered_map<string, string> relationToGroupMap;

    int nextAvailablePipeId = 1;

    void ProcessRelationFiles();

    void FindSelectionAndJoins(unordered_map<string, AndList *> *tableSelectionAndList, vector<AndList *> *joins);

    void ProcessSelect(unordered_map<string, AndList *> *tableSelectionAndList);

    void FindMinTupleJoin(vector<AndList *> *joins, vector<AndList *> *joins_arranged);

    void ProcessJoins(vector<AndList *> *joins);

    void ProcessGroupBy();

    void ProcessSum();

    void ProcessDuplicateRemoval();

    void ProcessProject();

    static void PrintQueryPlanInOrder(QueryPlanNode *node);

public:
    QueryPlan(Statistics *statistics, Query *query);

    ~QueryPlan();

    void Print();
};

#endif