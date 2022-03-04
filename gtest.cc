#include "gtest/gtest.h"
#include <iostream>
#include "BigQ.cc"
#include "DBFile.h"

TEST(BigQTest1, RecordComparatorTest) {
    DBFile dbfile;
	dbfile.Open ("nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2;
    dbfile.GetNext(record1);
    dbfile.GetNext(record2);
    RecordComparator recordComparator (order);
    EXPECT_EQ(recordComparator.operator()(&record1, &record2), false);
    dbfile.Close();
}

TEST(BigQTest2, RunComparatorTest) {
    DBFile dbfile;
	dbfile.Open ("nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2;
    File file;
    file.Open(0, "temp");
    dbfile.GetNext(record1);
    dbfile.GetNext(record2);
    bufferPage.Append(&record1);
    file.AddPage(&bufferPage, 0);
    bufferPage.EmptyItOut();
    bufferPage.Append(&record2);
    file.AddPage(&bufferPage, 1);  
    class Run* first = new class Run(&file, 0, 1);
    class Run* second = new class Run(&file, 1, 1);
    RunComparator runComparator (order);
    EXPECT_EQ(runComparator.operator()(first, second), false);
    file.Close();
    dbfile.Close();
}

TEST(BigQTest3, CleanUpTest) {
    DBFile dbfile;
	dbfile.Open ("nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker order (scheme);
    Pipe in (25);
    Pipe out (25);
    Record temp;
    while(dbfile.GetNext(temp)==1){
        in.Insert(&temp);
    }
    BigQ bigq (in, out, order, 5);
    EXPECT_FALSE(out.isShutdown());
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
