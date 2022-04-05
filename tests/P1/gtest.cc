#include "gtest/gtest.h"
#include "../../HeapDBFile.h"
#include <iostream>


HeapDBFile heapdbFile;

TEST(DBFile, SuccessfulCreate) {
    int fileCreatedStatus = heapdbFile.Create("gtest.bin", heap, NULL);
    ASSERT_EQ(fileCreatedStatus, 1);
}

TEST(DBFile, SuccessfulOpen) {
    heapdbFile.Create("gtest.bin", heap, NULL);
    heapdbFile.Close();
    int readStatus = heapdbFile.Open("gtest.bin");
    ASSERT_EQ(readStatus, 1);
}

TEST(DBFile, SuccessfulClose) {
    heapdbFile.Create("gtest.bin", heap, NULL);
    int closeStatus = heapdbFile.Close();
    ASSERT_EQ(closeStatus, 2);
}

void TearDown() {
    heapdbFile.Close();
    remove("gtest.bin");
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}