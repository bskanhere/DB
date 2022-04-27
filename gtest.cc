#include "gtest/gtest.h"
#include "QueryPlan.h"
#include "QueryPlanNodes.h"
#include <vector>

TEST(QueryPlan, SuccessfulBuildJoinPermutation) {
    int initialPermutation[4] = {1, 2, 3, 4};
    vector<int *> permutations;
    BuildJoinPermutation(initialPermutation, 4, 4, &permutations);
    ASSERT_EQ(permutations.size(), 24);
}

TEST(QueryPlan, SuccessfulQueryPlanNodes) {
    QueryPlanNode left(NULL, NULL);
    QueryPlanNode right(NULL, NULL);
    QueryPlanNode parent(&left, &right);
    ASSERT_TRUE(parent.left == &left);
    ASSERT_TRUE(parent.right == &right);
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}