#include "QueryPlanNodes.h"

QueryPlanNode::QueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) {
    this->left = left;
    this->right = right;

    if (this->left) {
        this->leftPipeId = this->left->outputPipeId;
    }

    if (this->right) {
        this->rightPipeId = this->right->outputPipeId;
    }
}

void QueryPlanNode::Print() {
    if (leftPipeId != -1) {
        cout << "Input Pipe " << leftPipeId << "\n";
    }

    if (rightPipeId != -1) {
        cout << "Input Pipe " << rightPipeId << "\n";
    }

    if (outputPipeId != -1) {
        cout << "Output Pipe " << outputPipeId << "\n";
    }

    if (outputSchema) {
        cout << "Output Schema:" << "\n";
        outputSchema->Print();
    }
}

SelectPipeQueryPlanNode::SelectPipeQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void SelectPipeQueryPlanNode::Print() {
    cout << "******SELECT PIPE******" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF: ";
        selOp->Print(left->outputSchema, NULL, literal);
        cout << "\n";
    }
}

SelectFileQueryPlanNode::SelectFileQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void SelectFileQueryPlanNode::Print() {
    cout << "******SELECT FILE******" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF: ";
        selOp->Print(outputSchema, NULL, literal);
        cout << "\n";
    }
}

ProjectQueryPlanNode::ProjectQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void ProjectQueryPlanNode::Print() {
    cout << "******PROJECT******" << "\n";
    QueryPlanNode::Print();
    cout << "Number of attributes Input: " << numAttsInput << "\n";
    cout << "Number of attributes Output: " << numAttsOutput << "\n";
    cout << "\n";
}

SumQueryPlanNode::SumQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void SumQueryPlanNode::Print() {
    cout << "******SUM******" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (computeMe) {
        cout << "FUNCTION: ";
        computeMe->Print(left->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}

JoinQueryPlanNode::JoinQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void JoinQueryPlanNode::Print() {
    cout << "******JOIN******" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    cout << "Join CNF: ";
    if (selOp) {
        selOp->Print(left->outputSchema, right->outputSchema, literal);
        cout << "\n";
    }

}

DuplicateRemovalQueryPlanNode::DuplicateRemovalQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void DuplicateRemovalQueryPlanNode::Print() {
    cout << "******DISTINCT******" << "\n";
    QueryPlanNode::Print();
}

GroupByQueryPlanNode::GroupByQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void GroupByQueryPlanNode::Print() {
    cout << "******GROUP BY******" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (groupAtts) {
        cout << "GROUPING ON: ";
        groupAtts->Print(left->outputSchema);
        cout << "\n";
    }

    if (computeMe) {
        cout << "FUNCTION: ";
        computeMe->Print(left->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}



