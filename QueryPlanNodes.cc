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
    cout << " *********** " << "\n";
    cout << "SELECT PIPE operation" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF :" << "\n";
        selOp->Print(left->outputSchema, NULL, literal);
        cout << "\n";
    }
}

SelectFileQueryPlanNode::SelectFileQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void SelectFileQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "SELECT FILE operation" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (selOp) {
        cout << "SELECTION CNF :" << "\n";
        selOp->Print(outputSchema, NULL, literal);
        cout << "\n";
    }
}

ProjectQueryPlanNode::ProjectQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void ProjectQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "PROJECT operation" << "\n";
    QueryPlanNode::Print();
    cout << "Number of attributes Input: " << numAttsInput << "\n";
    cout << "Number of attributes Output: " << numAttsOutput << "\n";
    cout << "Keep Me: ";
    for (int i = 0; i < numAttsOutput; i++) {
        cout << keepMe[i] << " ";
    }
    cout << "\n";
}

SumQueryPlanNode::SumQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void SumQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "SUM operation" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (computeMe) {
        cout << "FUNCTION" << "\n";
        computeMe->Print(left->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}

JoinQueryPlanNode::JoinQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void JoinQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "JOIN operation" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    cout << "CNF: " << "\n";
    if (selOp) {
        selOp->Print(left->outputSchema, right->outputSchema, literal);
        cout << "\n";
    }

}

DuplicateRemovalQueryPlanNode::DuplicateRemovalQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void DuplicateRemovalQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "DISTINCT operation" << "\n";
    QueryPlanNode::Print();
}

GroupByQueryPlanNode::GroupByQueryPlanNode(QueryPlanNode *left, QueryPlanNode *right) : QueryPlanNode(left, right) {}

void GroupByQueryPlanNode::Print() {
    cout << " *********** " << "\n";
    cout << "GROUP BY operation" << "\n";
    QueryPlanNode::Print();
    cout << "\n";

    if (groupAtts) {
        cout << "GROUPING ON" << "\n";
        groupAtts->Print(left->outputSchema);
        cout << "\n";
    }

    if (computeMe) {
        cout << "FUNCTION" << "\n";
        computeMe->Print(left->outputSchema);
        cout << "\n";
        cout << "Distinct Function: " << distinctFunc << "\n";
        cout << "\n";
    }
}



