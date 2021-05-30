#include "gtest/gtest.h"
#include "DBFile.h"
#include "BigQ.h"
#include "test.h"

TEST(DBFILE_TEST_SUIT, DBFILE_CREATE_TEST){

    DBFile db;

    // Unimplemented, should return 0.
    EXPECT_EQ(0, db.Create("abc.bin", sorted, NULL));
    // Regular call
    EXPECT_EQ(1, db.Create("test.bin", heap, NULL));
    // Should be able to overwrite existing file.
    EXPECT_EQ(1, db.Create("test.bin", heap, NULL));
}

TEST(DBFILE_TEST_SUIT, DBFILE_OPEN_TEST){
    DBFile db;

    // Regular call.
    EXPECT_EQ(1, db.Open("test.bin"));
    // Open non existant file.
    EXPECT_EXIT(db.Open("abc.bin"), ::testing::ExitedWithCode(1), "BAD!  Open did not work for abc.bin");
}

TEST(DBFILE_TEST_SUIT, DBFILE_CLOSE_TEST){
    DBFile db;
    // Should always be successfull.
    EXPECT_EQ(1, db.Close());
}

TEST(DBFILE_TEST_SUIT, DBFILE_INTEGRATED_TEST){
    DBFile db;
    //Integration tests may look like a combination of above tests, but this may change in the future.
    EXPECT_EQ(1, db.Create("test.bin", heap, NULL));
    EXPECT_EQ(1, db.Open("test.bin"));
    EXPECT_EXIT(db.Open("abc.bin"), ::testing::ExitedWithCode(1), "BAD!  Open did not work for abc.bin");
    EXPECT_EXIT(db.Open(NULL), ::testing::ExitedWithCode(1), "BAD!  Open did not work for ");
    EXPECT_EQ(1, db.Close());
    EXPECT_EQ(1, db.Close());
    
    // !!!!!! Will be used for further tests. !!!!!!!!!
    // Working test, under normal usage. 
    setup();
    ASSERT_EQ(1,db.Open("test.bin"));
    char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;
    db.Load(*(n->schema()), tbl_path);
    db.Close();

}

TEST(BIGQ_TEST_SUIT, BIGQ_UNIT_TESTS) {
    vector<Record *> r;
    EXPECT_EXIT(WriteRunToFile(r, NULL), ::testing::KilledBySignal(11), "");
}

TEST(BIGQ_TEST_SUIT, BIGQ_INTEGRATION_TEST) {
    // Fill the input pipe.
    Pipe input (100);
    Pipe output(100);
    DBFile dbfile;
	ASSERT_EQ(1, dbfile.Open ("test.bin"));
	dbfile.MoveFirst ();
    Record temp;
    int count = 0;
	while (dbfile.GetNext (temp) == 1) {
		input.Insert (&temp);
        count++;
	}

	dbfile.Close ();
	input.ShutDown ();

    // Create order maker.
    OrderMaker *order = new OrderMaker(n->schema());

    File *tempFile = new File();
    
    // Call split to runs.
    EXPECT_EQ(1,SplitToRuns(input, 1, order, tempFile));
    
    MergeRuns(tempFile, 1, 1, order, output);
    output.ShutDown();
    vector<Record *> rec;
    Record * temp1 = new Record();
    while(output.Remove(temp1)==1) {
        rec.push_back(temp1);
        temp1 = new Record();
    }
    ASSERT_EQ(rec.size(), count);
    tempFile->Close();
}