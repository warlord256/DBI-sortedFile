#include "gtest/gtest.h"
// #include "DBFile.h"
// #include "BigQ.h"
// #include "RelOp.h"
// #include "test.h"
// #include "Statistics.h"
// #include "QueryPlan.h"
#include "QueryOptimizer.h"

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);

TEST(OPTIMIZER_TEST, OPTIMIZE){
    QueryOptimizer qo;
    QueryPlan qp;
    EXPECT_DEATH(qo.Optimize(qp), "");
    yy_scan_string("SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10) GROUP BY r.r_regionkey");
    yyparse();
    EXPECT_NO_FATAL_FAILURE(qo.Optimize(qp));
}

TEST(QUERY_PLAN_TEST, ASSIGN_PIPES){
    // QueryOptimizer qo;
    QueryPlan qp;
    // EXPECT_NO_FATAL_FAILURE(qo.Optimize(qp));
    EXPECT_EQ(NULL, qp.root);
    EXPECT_DEATH(qp.AssignPipes(NULL),"");
    Node *n1 = new Node(SELECT_FILE), *n2 = new Node(SELECT_FILE), *j = new Node(JOIN);
    j->leftChild = n1;
    j->rightChild = n2;
    qp.root = j;
    EXPECT_NO_FATAL_FAILURE(qp.AssignPipes(j));
    EXPECT_EQ(0, n1->outPipe);
    EXPECT_EQ(1, n2->outPipe);
    EXPECT_EQ(0, j->lPipe);
    EXPECT_EQ(1, j->rPipe);
    EXPECT_EQ(2, j->outPipe);
}

TEST(OPTIMIZER_TEST, COMPLETE){
    QueryOptimizer qo;
    QueryPlan qp;
    EXPECT_DEATH(qo.Optimize(qp), "");
    yy_scan_string("SELECT SUM DISTINCT (n.n_nationkey + r.r_regionkey) FROM nation AS n, region AS r, customer AS c WHERE (n.n_regionkey = r.r_regionkey) AND (n.n_nationkey = c.c_nationkey) AND (n.n_nationkey > 10) GROUP BY r.r_regionkey");
    yyparse();
    EXPECT_NO_FATAL_FAILURE(qo.Optimize(qp));
    EXPECT_NO_FATAL_FAILURE(qp.AssignPipes(qp.root));
    EXPECT_EQ(7, qp.pipeCount);
}

/**
 * The test cases for statistics class requires another lexer to parse the cnf directly without user input.
 * IF required make sure the lparsers are also included to run these.
 * 
 **/
/*
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

TEST(STATISTICS_TEST_SUIT, STATISTICS_ESTIMATE){
    Statistics s;
    char *relName[] = {"supplier","partsupp"};
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);	

	char *cnf = "(s_suppkey = ps_unknownattr)";

	yy_scan_string(cnf);
	yyparse();
    EXPECT_EXIT(s.Estimate(final, relName, 2), ::testing::KilledBySignal(6), "Assertion `1==2' failed");

    char *valid_cnf = "(s_suppkey = ps_suppkey)";
	yy_scan_string(valid_cnf);
	yyparse();
    EXPECT_EQ(800000, s.Estimate(final, relName, 2));    
}

TEST(STATISTICS_TEST_SUIT, STATISTICS_APPLY){
    Statistics s;
    char *relName[] = {"supplier","partsupp"};
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);	

	char *cnf = "(s_suppkey = ps_unknownattr)";

	yy_scan_string(cnf);
	yyparse();
    EXPECT_EXIT(s.Apply(final, relName, 2), ::testing::KilledBySignal(6), "Assertion `1==2' failed");

    char *valid_cnf = "(s_suppkey = ps_suppkey)";
	yy_scan_string(valid_cnf);
	yyparse();
    EXPECT_NO_FATAL_FAILURE(s.Apply(final, relName, 2));   
}

TEST(STATISTICS_TEST_SUIT, STATISTICS_VALID_JOIN){
    Statistics s;
    char *relName[] = {"a","b","c"};
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "a_attr",10000);

	s.AddRel(relName[1],10000);
	s.AddAtt(relName[1], "b_attr", 10000);	

	s.AddRel(relName[2],10000);
	s.AddAtt(relName[2], "c_attr", 10000);	

	char *cnf = "(a_attr = b_attr)";

	yy_scan_string(cnf);
	yyparse();
    s.Apply(final, relName, 2);

    char *cnf_2 = "(b_attr = c_attr)";

	yy_scan_string(cnf_2);
	yyparse();

    EXPECT_EXIT(s.Estimate(final, &relName[1], 2), ::testing::KilledBySignal(6), "Assertion `1==2' failed");
    EXPECT_EQ(10000, s.Estimate(final, relName, 3));
}

TEST(STATISTICS_TEST_SUIT, STATISTICS_NORMAL_FLOW){
    Statistics s;
        char *relName[] = {"supplier","partsupp"};

	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);	

	char *cnf = "(s_suppkey = ps_suppkey)";

	yy_scan_string(cnf);
	yyparse();
	EXPECT_EQ(800000, s.Estimate(final, relName, 2));
    EXPECT_NO_FATAL_FAILURE(s.Apply(final, relName, 2));
	
    cnf = "(s_suppkey>1000)";	
	yy_scan_string(cnf);
	yyparse();
    EXPECT_NEAR(266666, s.Estimate(final, relName, 2), 1);
}
*/


/* 
********
Uncomment the tests if it is required to run, 
commented because for the current project it is not required to create bin files, 
for the test cases to run a valid tpch directory needs to be mentioned.
But Project 4.1 does not require it hence commented the code.
*********


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
*/
