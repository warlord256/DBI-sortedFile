#include "gtest/gtest.h"
#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "test.h"

TEST(DBFILE_TEST_SUIT, DBFILE_CREATE_TEST){

    DBFile db;

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
    EXPECT_EXIT(db.Open("abc.bin"), ::testing::KilledBySignal(6), "Assertion `1==2' failed");
}

TEST(DBFILE_TEST_SUIT, DBFILE_CLOSE_TEST){
    DBFile db;
    // Fails if Open is not called.
    EXPECT_EQ(0, db.Close());
}

TEST(DBFILE_TEST_SUIT, DBFILE_INTEGRATED_TEST){
    DBFile db;
    //Integration tests may look like a combination of above tests, but this may change in the future.
    EXPECT_EQ(1, db.Create("test.bin", heap, NULL));
    EXPECT_EQ(1, db.Open("test.bin"));
    EXPECT_EQ(1, db.Close());
    
    // !!!!!! Will be used for further tests. !!!!!!!!!
    // Working test, under normal usage. 
    setup();
    ASSERT_EQ(1,db.Open("test.bin"));
    char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;
    db.Load(*(n->schema()), tbl_path);
    ASSERT_EQ(1, db.Close());

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

TEST(SORTED_DBFILE_TEST_SUIT, DBFILE_CREATE_TEST){

    DBFile db;
    OrderMaker *o = new OrderMaker(n->schema());

    struct {OrderMaker *o; int l;} startup = {o, 3};

    // Regular call
    EXPECT_EQ(1, db.Create("test.bin", sorted, &startup));
    // Without startup struct
    EXPECT_EXIT(db.Create("abc.bin", sorted, NULL), ::testing::KilledBySignal(6), "Assertion `1==2' failed");
}

TEST(SORTED_DBFILE_TEST_SUIT, DBFILE_OPEN_TEST){

    DBFile db;
    OrderMaker *o = new OrderMaker(n->schema());

    struct {OrderMaker *o; int l;} startup = {o, 3};

    // Regular call
    EXPECT_EQ(1, db.Open("test.bin"));
    // Non existant file
    EXPECT_EXIT(db.Open("abc.bin"), ::testing::KilledBySignal(6), "Assertion `1==2' failed");
}

TEST(SORTED_DBFILE_TEST_SUIT, DBFILE_INTEGRATION_TEST) {
    DBFile db;
    ASSERT_EQ(1, db.Open("test.bin"));

    char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, n->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;
    db.Load(*(n->schema()), tbl_path);
    ASSERT_EQ(1, db.Close());

    ASSERT_EQ(1, db.Open("test.bin"));
    db.MoveFirst();
    ComparisonEngine ce;
    OrderMaker *o = new OrderMaker(n->schema());
    int i = 0;
    int err = 0;
    Record rec[2];
	Record *last = NULL, *prev = NULL;

	while (db.GetNext(rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ce.Compare (prev, last, o) == 1) {
				err++;
			}
		}
		i++;
	}

    ASSERT_EQ(0, err);
    db.Close();

}

TEST(RELOP_TEST_SUIT, SF_TEST) {
    DBFile db;
    ASSERT_EQ(1, db.Open("test.bin"));
    Pipe p(1);
    SelectFile sf;
    CNF cnf;
    Record lit, r;
    ComparisonEngine ce;
    sf.Use_n_Pages(10);
    get_cnf("(n_nationkey > 20)", n->schema(), cnf, lit);
    sf.Run(db, p, cnf, lit);
    while(p.Remove(&r)) {
        ASSERT_EQ(1, ce.Compare(&r, &lit, &cnf)); 
    }
    sf.WaitUntilDone();
}

TEST(RELOP_TEST_SUIT, SUM_TEST) {
    DBFile db;
    ASSERT_EQ(1, db.Open("test.bin"));
    Pipe in(10), out(1);
    Sum S;
    Function func;
    get_cnf("(n_nationkey)", n->schema(), func);
    Record r;
    S.Run(in, out, func);
    db.MoveFirst();
    int sum = 0;
    int count = 0;
    while(db.GetNext(r)) {
        sum += count++;
        in.Insert(&r);
    }
    in.ShutDown();
    ASSERT_EQ(1, out.Remove(&r));
    int res = ((int *)(r.bits))[2];
    ASSERT_EQ(sum, res);
    S.WaitUntilDone();
}

TEST(RELOP_TEST_SUIT, DUPLICATE_TEST) {
    DBFile db;
    ASSERT_EQ(1, db.Open("test.bin"));
    Pipe in(10), out(1);
    DuplicateRemoval dp;
    Record r;
    dp.Run(in, out, *n->schema());
    db.MoveFirst();
    int count = 0;
    while(db.GetNext(r)) {
        count++;
        in.Insert(&r);
    }
    in.ShutDown();
    while(out.Remove(&r)) {
        count--;
    }
    ASSERT_EQ(0, count);
    dp.WaitUntilDone();
}

TEST(RELOP_TEST_SUIT, GROUPBY_TEST) {
    DBFile db;
    ASSERT_EQ(1, db.Open("test.bin"));
    
    GroupBy G;
    Pipe in(10), out (1);
    Function func;
    Attribute *n_regionkey = new Attribute{"n_regionkey", Int};
    // Attribute atts[] = {s_regionkey};
    char *str_sum = "(1)";
    get_cnf (str_sum, n->schema(), func);
    OrderMaker grp_order (n->schema(), 1, n_regionkey);
	G.Use_n_Pages (1);
    G.Run(in, out, grp_order, func);
    Record r;
    db.MoveFirst();
    while(db.GetNext(r)) {
        in.Insert(&r);
    }
    in.ShutDown();
    int count = 0;
    while(out.Remove(&r)) {
        count++;
    }
    ASSERT_EQ(5, count);
    G.WaitUntilDone();
}