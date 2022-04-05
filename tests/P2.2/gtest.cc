#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
#include "../../DBFile.h"
#include "../../Record.h"
#include "../../DBFile.h"
#include "../../Defs.h"
using namespace std;

TEST(SortedDBFileTest, SuccessfulCreate) {
    Schema* testSchema = new Schema("catalog","nation");
    OrderMaker* orderMaker = new OrderMaker(testSchema);
    DBFile dbfile;
    struct {OrderMaker *o; int l;} startup = {orderMaker, 16};
    int fileCreatedStatus = dbfile.Create("testData/gtest.bin", sorted, &startup);
    ASSERT_EQ(fileCreatedStatus, 1);
    dbfile.Close();    
}

TEST(SortedDBFileTest, SuccessfulOpen) {
    DBFile dbfile;
    EXPECT_EQ(dbfile.Open("testData/gtest.bin"), 1);
    dbfile.Close();
}

TEST(SortedDBFileTest, SuccessfulClose) {
    DBFile dbfile;
    dbfile.Open("testData/gtest.bin");
    EXPECT_EQ(dbfile.Close(), 1);
}

TEST(SortedDBFileTest, SuccessfulCreateAndClose) {
    Schema* testSchema = new Schema("catalog","nation");
    OrderMaker* orderMaker = new OrderMaker(testSchema);
    DBFile dbfile;
    struct {OrderMaker *o; int l;} startup = {orderMaker, 16};
    dbfile.Create("testData/gtest.bin", sorted, &startup);
    EXPECT_EQ(dbfile.Close(), 1);
}

TEST(SortedDBFileTest, SuccessfulGetNext) {
    Schema* testSchema = new Schema("catalog","nation");
    OrderMaker* orderMaker = new OrderMaker(testSchema);
    DBFile dbfile;
    struct {OrderMaker *o; int l;} startup = {orderMaker, 16};
    dbfile.Open("testData/gtest.bin");
    Record rec;
    EXPECT_EQ(dbfile.GetNext(rec), 0);
    dbfile.Close();
}

void TearDown() {
    remove("testdata/gtest.bin");
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
