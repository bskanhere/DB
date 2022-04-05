#include "gtest/gtest.h"
#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "test.h"
#include <fstream>
#include <pthread.h>
#include <iostream>


int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe (Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		int ival = 0; double dval = 0;
		func.Apply (rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}


TEST(RelOp, testSelectFile){
	setup();

	SelectFile SF_r;
	Pipe _r(100);
	CNF cnf_r;
	Record lit_r;
	DBFile dbf_r;
	char *pred_r = "(r_regionkey < 4)";
	dbf_r.Open ("testData/region.bin");
	get_cnf (pred_r, r->schema (), cnf_r, lit_r);
	SF_r.Use_n_Pages (100);
	SF_r.Run (dbf_r, _r, cnf_r, lit_r);
	SF_r.WaitUntilDone ();
	int count = clear_pipe (_r, r->schema (), false);
	dbf_r.Close ();
	cleanup();
	ASSERT_EQ(count, 4);
}

TEST(RelOp, testSum){
	setup();
	SelectFile SF_r;
	Pipe _r(100);
	CNF cnf_r;
	Record lit_r;
	DBFile dbf_r;
	char *pred_r = "(r_regionkey = r_regionkey)";
	
	dbf_r.Open ("testData/region.bin");
	get_cnf (pred_r, r->schema (), cnf_r, lit_r);
	SF_r.Use_n_Pages (100);

	Sum T;
	Pipe _out (1);
	Function func;
	char *str_sum = "(r_regionkey)";
	get_cnf (str_sum, r->schema (), func);
	func.Print ();
	T.Use_n_Pages (1);
	SF_r.Run (dbf_r, _r, cnf_r, lit_r);
	T.Run (_r, _out, func);

	SF_r.WaitUntilDone ();
	T.WaitUntilDone ();


	Record rec;
	_out.Remove (&rec);
	int pointer = ((int *) rec.bits)[1];
	double *outputSum = (double *) &(rec.bits[pointer]);


	dbf_r.Close ();

	ASSERT_EQ(*outputSum, 10); // check if outputsum is equal to the expected sum of 10
}

TEST(RelOp, testProject){
	setup();
	SelectFile SF_r;
	Pipe _r(100);
	CNF cnf_r;
	Record lit_r;
	DBFile dbf_r;
	char *pred_r = "(r_regionkey = 0)";
	dbf_r.Open (r->path());
	get_cnf (pred_r, r->schema (), cnf_r, lit_r);
	SF_r.Use_n_Pages (100);
	Project P_r;
	Pipe _out (100);
	int keepMe[] = {0,2};
	P_r.Use_n_Pages (100);
	SF_r.Run (dbf_r, _r, cnf_r, lit_r);
	P_r.Run (_r, _out, keepMe, 3, 2);
	SF_r.WaitUntilDone ();
	P_r.WaitUntilDone ();
	Attribute att2[] = {{"int", Int}, {"string", String}};
	Record rec;	
	_out.Remove(&rec);
	dbf_r.Close ();
	cleanup();
	ASSERT_EQ(2, rec.GetNumAtts()); 
}


TEST(RelOp, testJoin){
	setup();

	SelectFile SF_n;
	Pipe _n(100);
	CNF cnf_n;
	Record lit_n;
	DBFile dbf_n;
	char *pred_n = "(n_regionkey = n_regionkey)";

	dbf_n.Open (n->path());
	get_cnf (pred_n, n->schema (), cnf_n, lit_n);
	SF_n.Use_n_Pages (100);
	SF_n.Run (dbf_n, _n, cnf_n, lit_n); 


	SelectFile SF_r;
	Pipe _r(100);
	CNF cnf_r;
	Record lit_r;
	DBFile dbf_r;
	char *pred_r = "(r_regionkey = r_regionkey)";
	dbf_r.Open (r->path());
	get_cnf (pred_r, r->schema (), cnf_r, lit_r);
	SF_r.Use_n_Pages (100);
	Join J;
	Pipe _n_r (100);
	CNF cnf_n_r;
	Record lit_n_r;
	get_cnf ("(n_regionkey = r_regionkey)", n->schema(), r->schema(), cnf_n_r, lit_n_r);
	SF_r.Run (dbf_r, _r, cnf_r, lit_r); 
	J.Run (_n, _r, _n_r, cnf_n_r, lit_n_r);
	SF_r.WaitUntilDone ();
	Record rec;
	_n_r.Remove(&rec);
	ASSERT_EQ(7, rec.GetNumAtts());
}



int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}