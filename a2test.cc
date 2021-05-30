#include "test.h"

int add_data (FILE *src, int &res) {
	DBFile dbfile;
	dbfile.Open (rel->path ());
	Record temp;

	int proc = 0;
	int xx = 20000;
	while ((res = temp.SuckNextRecord (rel->schema (), src))) {
        proc++;
		dbfile.Add (temp);
		if (proc == xx) cerr << "\t ";
		if (proc % xx == 0) cerr << ".";
	}

	dbfile.Close ();
	return proc;
}

//For generating sorted DBFile.
void test1()
{
    OrderMaker o;
	rel->get_sort_order (o);

	cin.clear();
	clearerr(stdin);

	int runlen = 0;
	while (runlen < 1) {
		cout << "\t\n specify runlength:\n\t ";
		cin >> runlen;
	}
	struct {OrderMaker *o; int l;} startup = {&o, runlen};

	DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path () << endl;
	dbfile.Create (rel->path(), sorted, &startup);	
	dbfile.Close ();	
	char tbl_path[100];
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout << " input from file : " << tbl_path << endl;
	int proc = 1, res = 1;
	//dbfile.Load (*(rel->schema ()), tbl_path);
	FILE *tblfile = fopen (tbl_path, "r");
	proc = add_data (tblfile, res);
    
	fclose (tblfile);
}

void test2()
{
    DBFile dbfile;
	cout << "\n output to dbfile : " << rel->path () << endl;
	dbfile.Create (rel->path(), heap, NULL);

    char tbl_path[100]; // construct path of the tpch flat text file
	sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name()); 
	cout << " tpch file will be loaded from " << tbl_path << endl;
	
	dbfile.Load (*(rel->schema ()), tbl_path);
	dbfile.Close ();
}


int main()
{
    setup ();

	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	void (*test_ptr[]) () = {&test1, &test2};  
	void (*test) ();

	int tindx = 0;
	while (tindx < 1 || tindx > 2) {
		cout << " select test option: \n";
		cout << " \t 1. create sorted dbfile\n";
		cout << " \t 2. create heap dbfilen\n \t";
		//cout << " \t 3. run some query \n \t ";
		cin >> tindx;
	}

	int findx = 0;
	while (findx < 1 || findx > 8) {
		cout << "\n select table: \n";
		cout << "\t 1. nation \n";
		cout << "\t 2. region \n";
		cout << "\t 3. customer \n";
		cout << "\t 4. part \n";
		cout << "\t 5. partsupp \n";
		cout << "\t 6. supplier \n";
		cout << "\t 7. orders \n";
		cout << "\t 8. lineitem \n \t ";
		cin >> findx;
	}
	rel = rel_ptr [findx - 1];

	test = test_ptr [tindx-1];
	test ();
	cleanup ();
	cout << "\n\n";
}