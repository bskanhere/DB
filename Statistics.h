#ifndef STATISTICS_
#define STATISTICS_

#include "ParseTree.h"
#include <gtest/gtest_prod.h>
#include <cstring>
#include <string>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include<sstream>

using namespace std;

class Statistics {



private:
    FRIEND_TEST(Statistics, SuccessfulAddRelationTest);
    FRIEND_TEST(Statistics, SuccessfulAddAttributeTest);
    FRIEND_TEST(Statistics, SuccessfulGroupRelationMapTest);
    FRIEND_TEST(Statistics, SuccessfulUpdateRelationTest);
    unordered_map<string, double> groupTupleCountMap;
    unordered_map<string, int> attDistinctCountMap;
    unordered_map<string, unordered_set<string> > groupToRelationsMap;
    unordered_map<string, string> relationToGroupMap;

    void ValidateApplyOnRelations(unordered_set<string> *relNames);

    void PreProcessApplyOnAttributes(struct AndList *parseTree, unordered_set<string> *relNames);

    void PreProcessNameOperand(Operand *operand, unordered_set<string> *relNames);

public:
    Statistics();

    Statistics(Statistics &copyMe);     // Performs deep copy

    ~Statistics();

    void AddRel(char *relName, int numTuples);

    void AddAtt(char *relName, char *attName, int numDistincts);

    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);

    void Write(char *fromWhere);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);

    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
