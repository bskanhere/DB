#include "gtest/gtest.h"
#include "../../Statistics.h"

TEST(Statistics, SuccessfulAddRelationTest){
	Statistics s;
	s.AddRel("nation", 25);
	ASSERT_EQ(s.groupTupleCountMap["nation"], 25);
}

TEST(Statistics, SuccessfulUpdateRelationTest){
	Statistics s;
	s.AddRel("nation", 25);
	ASSERT_EQ(s.groupTupleCountMap["nation"], 25);
    
	s.AddRel("nation", 40);
	ASSERT_NE(s.groupTupleCountMap["nation"], 25);
	ASSERT_EQ(s.groupTupleCountMap["nation"], 40);
}

TEST(Statistics, SuccessfulAddAttributeTest){
	Statistics s;
	s.AddRel("nation", 25);
	s.AddAtt("nation", "n_nationkey", -1);
	ASSERT_EQ(s.attDistinctCountMap["nation.n_nationkey"], 25); 
}

TEST(Statistics, SuccessfulGroupRelationMapTest){
	Statistics s;
	s.AddRel("nation", 25);
	ASSERT_EQ(s.relationToGroupMap["nation"], "nation");
}


int main(int argc, char **argv){
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}