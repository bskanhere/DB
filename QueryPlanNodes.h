#ifndef QUERY_PLAN_NODES_H
#define QUERY_PLAN_NODES_H

#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "DBFile.h"
#include <iostream>

class QueryPlanNode {
public:
    QueryPlanNode *left = nullptr;
    int leftPipeId = -1;

    QueryPlanNode *right = nullptr;
    int rightPipeId = -1;

    Schema *outputSchema = nullptr;
    int outputPipeId = -1;

    QueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    virtual void Print();
};

class SelectPipeQueryPlanNode : public QueryPlanNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    SelectPipeQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class SelectFileQueryPlanNode : public QueryPlanNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    SelectFileQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class ProjectQueryPlanNode : public QueryPlanNode {
public:
    int *keepMe = nullptr;
    int numAttsInput;
    int numAttsOutput;

    ProjectQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class SumQueryPlanNode : public QueryPlanNode {
public:
    Function *computeMe = nullptr;
    int distinctFunc = 0;

    SumQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class JoinQueryPlanNode : public QueryPlanNode {
public:
    CNF *selOp = nullptr;
    Record *literal = nullptr;

    JoinQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class DuplicateRemovalQueryPlanNode : public QueryPlanNode {
public:
    DuplicateRemovalQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

class GroupByQueryPlanNode : public QueryPlanNode {
public:
    OrderMaker *groupAtts = nullptr;
    Function *computeMe = nullptr;
    int distinctFunc = 0; 

    GroupByQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right);
    void Print();
};

#endif