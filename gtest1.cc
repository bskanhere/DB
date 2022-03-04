#include "gtest/gtest.h"
#include "DBFile.h"
#include <iostream>


DBFile dbFile;

TEST(DBFile, SuccessfulCreate) {
    int fileCreatedStatus = dbFile.Create("gtest.bin", heap, NULL);
    ASSERT_EQ(fileCreatedStatus, 1);
}

TEST(DBFile, SuccessfulOpen) {
    dbFile.Create("gtest.bin", heap, NULL);
    dbFile.Close();
    int readStatus = dbFile.Open("gtest.bin");
    ASSERT_EQ(readStatus, 1);
}

TEST(DBFile, SuccessfulClose) {
    dbFile.Create("gtest.bin", heap, NULL);
    int closeStatus = dbFile.Close();
    ASSERT_EQ(closeStatus, 2);
}

void TearDown() {
    dbFile.Close();
    remove("gtest.bin");
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}