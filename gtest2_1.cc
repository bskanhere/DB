#include "gtest/gtest.h"
#include <iostream>
#include "BigQ.cc"
#include "HeapDBFile.h"

TEST(BigQTest1, testRecordComparator) {
    HeapDBFile heapdbfile;
	heapdbfile.Open ("testData/nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2;
    heapdbfile.GetNext(record1);
    heapdbfile.GetNext(record2);
    RecordComparator recordComparator (order);
    EXPECT_EQ(recordComparator.operator()(&record1, &record2), false);
    heapdbfile.Close();
}

TEST(BigQTest2, testRunComparator) {
    HeapDBFile heapdbfile;
	heapdbfile.Open ("testData/nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2;
    File file;
    file.Open(0, "temp");
    heapdbfile.GetNext(record1);
    heapdbfile.GetNext(record2);
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
    heapdbfile.Close();
}

TEST(BigQTest4, testGetNextRecordOnDifferentPage) {
    HeapDBFile heapdbfile;
	heapdbfile.Open ("testData/nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2, temp;
    File file;
    file.Open(0, "temp");
    heapdbfile.GetNext(record1);
    heapdbfile.GetNext(record2);

    temp.Copy(&record1);
    bufferPage.Append(&temp);
    file.AddPage(&bufferPage, 0);
    bufferPage.EmptyItOut();
    temp.Copy(&record2);
    bufferPage.Append(&temp);
    file.AddPage(&bufferPage, 1);  
    class Run* first = new class Run(&file, 0, 2);
    RunComparator runComparator (order);

    ComparisonEngine c;
    EXPECT_EQ(c.Compare( first->currRecord, &record1, order), 0);
    first->getNextRecord();
    EXPECT_EQ(c.Compare( first->currRecord, &record2, order), 0);

    file.Close();
    heapdbfile.Close();
}

TEST(BigQTest4, testGetNextRecordOnSamePage) {
    HeapDBFile heapdbfile;
	heapdbfile.Open ("testData/nation.bin");
    Schema* scheme = new Schema("catalog", "nation");
    OrderMaker* order = new OrderMaker(scheme);
    Page bufferPage;
    Record record1, record2, temp;
    File file;
    file.Open(0, "temp");
    heapdbfile.GetNext(record1);
    heapdbfile.GetNext(record2);

    temp.Copy(&record1);
    bufferPage.Append(&temp);
    temp.Copy(&record2);
    bufferPage.Append(&temp);
    file.AddPage(&bufferPage, 0);  
    class Run* first = new class Run(&file, 0, 1);

    ComparisonEngine c;
    EXPECT_EQ(c.Compare( first->currRecord, &record1, order), 0);
    first->getNextRecord();
    EXPECT_EQ(c.Compare( first->currRecord, &record2, order), 0);

    file.Close();
    heapdbfile.Close();
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
