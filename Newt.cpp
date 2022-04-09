#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;

const int MAX_PATH_LENGTH = 1024;
DbEnv* _DB_ENV;

class NewtShell {
	public:
	int run(int argc, char* argv[]) {

		if (argc != 2)
		{
			cout << "Usage: sql5300 DbEnvPath" << endl;
			exit(-1);
		}

		char raw_path[MAX_PATH_LENGTH];
		char* resolved_path = realpath(argv[1], raw_path);

		if (resolved_path == NULL) {
			cout << "Invalid DbEnvPath" << endl;
			exit(-1);
		} 

		// DbEnv env(0U);
		// env.set_message_stream(&cout);
		// env.set_error_stream(&cerr);
		// env.open(raw_path, DB_CREATE | DB_INIT_MPOOL, 0);

		DbEnv* env = new DbEnv(0U);
		env->set_message_stream(&std::cout);
		env->set_error_stream(&std::cerr);

		env->open(raw_path, DB_CREATE | DB_INIT_MPOOL, 0);

		_DB_ENV = env;

	// initialize_schema_tables();

		cout << "(sql5300: running with database environment at " << raw_path << ")" << endl;

		while(true) {
			cout << "SQL> ";
			
			string query;
			getline(cin, query);

			if (query.length() == 0)
			{
				continue;
			}

			transform(query.begin(), query.end(), query.begin(), ::tolower);

			if (query == "quit")
			{
				return 0;
			} 
			

			// parse a given query
			SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

			// check whether the parsing was successful
			if (result->isValid()) {
				// printf("Parsed successfully!\n");
				// printf("Number of statements: %lu\n", result->size());

				for (uint i = 0; i < result->size(); ++i) {
					string statement = execute(result->getStatement(i));
					cout << statement << endl;
					
				}

				delete result;
			} else {
				fprintf(stderr, "Given string is not a valid SQL query.\n");
				fprintf(stderr, "%s (L%d:%d)\n", 
						result->errorMsg(),
						result->errorLine(),
						result->errorColumn());
				delete result;
			}

		}

	}

	private:
	string execute (const SQLStatement *stmt) {
		
		switch (stmt -> type()) {
			case kStmtSelect:
				return executeSelect((const SelectStatement *) stmt);
			case kStmtInsert:
				return executeInsert((const InsertStatement *) stmt);
			case kStmtCreate:
				return executeCreate((const CreateStatement *) stmt);
			default:
				return "Not implemneted";
		}
	}

	string executeSelect(const SelectStatement *stmt) { 
		//TODO 
		// string ret("SELECT ");
		// bool doComma = false;
		// for (Expr *expr : *stmt->selectList) {
		//     if (doComma)
		//         ret += ", ";
		//     ret += expression(expr);
		//     doComma = true;
		// }
		// ret += " FROM " + table_ref(stmt->fromTable);
		// if (stmt->whereClause != NULL)
		//     ret += " WHERE " + expression(stmt->whereClause);
		// return ret;
	}

	string executeInsert(const InsertStatement *stmt) { 
		//TODO 
	}

	string executeCreate(const CreateStatement *stmt) { 
		//TODO 
		return "In execute create";
	}

};

int main (int argc, char* argv[]) {
	NewtShell n;
	n.run(argc, argv);

	return 0;
}

