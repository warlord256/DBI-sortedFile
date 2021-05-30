#include <fstream>
#include "ParseTree.h"
#include "Errors.h"
#include "RelOp.h"
#include "QueryOptimizer.h"

extern char* dbfile_dir;
extern char* catalog_path;

extern int queryType; // Type of query like SELECT, INSERT etc.
extern char* tableToInsert; // Table name to insert.
extern AttributeList *attsToCreate; // Attributes to create.
extern int fileType; // File type.
extern char* filename; //File name if needed.
extern bool shouldExecute; // Specifies if the query plan should be executed.
extern char* outputFilename; // File to which the output is to be written.


void ExecuteCreateQuery() {
	
	FATALIF(tableToInsert==NULL, "No table name provided to create.");

	DBFile file;
	string path = string(dbfile_dir)+string(tableToInsert)+".bin";
	file.Create(path.c_str(), heap, NULL);
	file.Close();
	ofstream catalog_file;
	catalog_file.open(string(catalog_path), catalog_file.app);
	catalog_file<<"BEGIN\n";
	catalog_file<<string(tableToInsert)<<endl;
	
	//Try to remove the below line.
	catalog_file<<string(tableToInsert)<<".tbl\n";

	while(attsToCreate != NULL) {
		catalog_file<<string(attsToCreate->name)<<" ";
		switch(attsToCreate->type){
		case INT:
			catalog_file<<"Int\n";
			break;
		case DOUBLE:
			catalog_file<<"Double\n";
			break;
		default:
			catalog_file<<"String\n";
		}
		attsToCreate = attsToCreate->next;
	}
	catalog_file<<"END\n\n";
	catalog_file.close();
}

void ExecuteSelectQuery() {
	QueryOptimizer opt;
	QueryPlan plan;
	opt.Optimize(plan);	
	plan.AssignPipes(plan.root);
	if(!shouldExecute) {
		plan.Print();
	} else {
		
		Schema *outSchema = plan.root->outputSchema;
		// Execute the plan.
		plan.Execute();
		Pipe *in = plan.pipes[plan.root->outPipe];
		// Pipe *in;
		// plan.GetOutputPipe(in);
		
		// Check if there is a filename to write the output to.
		if(outputFilename == NULL) {
			// Print the query output to STDOUT.
			Record r;
			while(in->Remove(&r)) {
				r.Print(outSchema);
			}
		} else {
			// Print the query output to the file.
			FILE *outFile = fopen(outputFilename, "w");
			
			WriteOut *wo = new WriteOut();
			
			FATALIF(in == NULL, "No output pipe for the plan!!");

			wo->Run(*in, outFile, *outSchema);
			wo->WaitUntilDone();
			fclose(outFile);
		}
		plan.Cleanup(plan.root);
	}

	return;
}

void ExecuteInsertQuery() {

	FATALIF(tableToInsert==NULL, "No table name provided to insert.");

	DBFile file;
	string path = string(dbfile_dir) + string(tableToInsert) + ".bin";
	file.Open(path.c_str());
	Schema s(catalog_path, tableToInsert);
	file.Load(s, filename);
	file.Close();
}

void ExecuteDropQuery() {

	FATALIF(tableToInsert==NULL, "No table name provided to drop.");

	// Delete the binary files.
	string toDel = string(dbfile_dir)+string(tableToInsert)+".bin";
	FATALIF(remove(toDel.c_str()) != 0, "File Does not Exist.");
	// Delete metafile.
	toDel = toDel+".meta";
	remove(toDel.c_str());

	// Remove the catalog entry.
	ifstream old;
	old.open(string(catalog_path));
	ofstream temp;
	string tempCatFile = "kdjanfjandalfj";
	temp.open(tempCatFile);

	string line, toCheck, relToDel(tableToInsert);
	while(getline(old, line)){
		if(line == "BEGIN" && getline(old, toCheck)) {
			if(toCheck == relToDel) {
				while(line != "END") {
					getline(old, line);
				}
			} else {
				temp<<line<<endl<<toCheck<<endl;
			}
		} else {
			temp<<line<<endl;
		}
	}
	old.close();
	temp.close();
	rename(tempCatFile.c_str(), catalog_path);
}

void ExecuteQuery() {
	switch(queryType) {
	case SELECT_QUERY:
		ExecuteSelectQuery();
		break;
	case CREATE_QUERY:
		ExecuteCreateQuery();
		break;
	case INSERT_QUERY:
		ExecuteInsertQuery();
		break;
	case DROP_QUERY:
		ExecuteDropQuery();
		break;
	case SET_QUERY:
		// Nothing to do, just some settings have changed.
		break;
	default:
		cerr<<"Unknown type of query\n";
		exit(-1);
	}
	return;
}
