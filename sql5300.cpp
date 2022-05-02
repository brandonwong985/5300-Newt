#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

#pragma GCC diagnostic ignored "-Wswitch"

using namespace std;
using namespace hsql;

const int MAX_PATH_LENGTH = 1024;
DbEnv* _DB_ENV;

class NewtShell {
	public:
	int run(int argc, char* argv[]) {
		if (argc != 2) {
			cout << "Usage: sql5300 DbEnvPath" << endl;
			exit(-1);
		}

		char raw_path[MAX_PATH_LENGTH];
		char* resolved_path = realpath(argv[1], raw_path);

		if (resolved_path == NULL) {
			cout << "Invalid DbEnvPath" << endl;
			exit(-1);
		} 

		DbEnv* env = new DbEnv(0U);
		env->set_message_stream(&std::cout);
		env->set_error_stream(&std::cerr);

		env->open(raw_path, DB_CREATE | DB_INIT_MPOOL, 0);

		_DB_ENV = env;

		cout << "(sql5300: running with database environment at " << raw_path << ")" << endl;

		while(true) {
			cout << "SQL> ";			
			string query;
			getline(cin, query);

			if (query.length() == 0) {
				continue;
			}

			transform(query.begin(), query.end(), query.begin(), ::tolower);

			if (query == "quit") {
				return 0;
			} 
			
			// parse a given query
			SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

			// check whether the parsing was successful
			if (result->isValid()) {
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
			case kStmtCreate:
				return executeCreate((const CreateStatement *) stmt);
			default:
				return "Not implemneted";
		}
	}

	string executeSelect(const SelectStatement *stmt) { 
		//TODO 
		string ret("SELECT ");
		bool doComma = false;
		for (Expr *expr : *stmt->selectList) {
		    if (doComma)
		        ret += ", ";
		    ret += expression(expr);
		    doComma = true;
		}
		ret += " FROM " + get_table(stmt->fromTable);
		if (stmt->whereClause != NULL)
		    ret += " WHERE " + expression(stmt->whereClause);
		return ret;
	}
	string get_table(const TableRef *table) {
    		string ret;
    		switch (table->type) {
    			case kTableName:
    				ret += table->name;
				if (table->alias != NULL){
                    			 ret += " AS ";
                   			 ret += table->alias;
               			 }	
    				break;
			case kTableJoin:
    				ret += get_join_type(table);
    				break;
			case kTableCrossProduct:
           			bool doComma = false;
            			for (TableRef *tbl : *table->list) {
                			if (doComma)
                    				ret += ", ";
                			ret += get_table(tbl);
                			doComma = true;
            			}
    		}	
		return ret;	
	}

	string get_join_type(const TableRef *table){
		string ret;
		ret += get_table(table->join->left);
		switch(table->join->type){
			case kJoinInner:
               		ret += " JOIN ";
               		break;
           		case kJoinLeft:
                   		 ret += " LEFT JOIN ";
                   		 break;
		}
		ret += get_table(table->join->right);
		if (table->join->condition != NULL){
                	ret += " ON " ;
                	ret += expression(table->join->condition);
		}
		return ret;
	}

	string executeCreate(const CreateStatement *stmt) { 
		string ret("CREATE TABLE ");
		string table_name = stmt->tableName;	
		ret += table_name;
		ret += " (";
		bool doComma = false;
		for (ColumnDefinition *col: *stmt->columns) {
			if (doComma)
			    ret += ", ";
			ret += get_column(col);
			doComma  = true;
		}
		ret += ")";
		return ret;
	}

	string get_column (const ColumnDefinition *col){
		string ret;
		switch (col->type) {
			case ColumnDefinition::TEXT:
				ret += col->name;
			        ret += " TEXT";	
				break;
			case ColumnDefinition::INT:
				ret += col->name;
				ret += " INT";
				break;
			case ColumnDefinition::DOUBLE:
				ret += col->name;
				ret += " DOUBLE";
				break;
			default:
				ret += col->name;
				ret += " UNKNOWN";
				break;
				
		}

		return ret;	
	}

	string expression(const Expr *expr) {
	   	string ret;
   		 switch (expr->type) {
        	case kExprStar:
            		ret += "*";
           		break;
        	case kExprColumnRef:
            		if (expr->table != NULL)
                		ret += string(expr->table) + ".";
           		ret += expr->name;
            		break;
        	case kExprLiteralString:
            		ret += string("\"") + expr->name + "\"";
            		break;
        	case kExprLiteralFloat:
            		ret += to_string(expr->fval);
            		break;
        	case kExprLiteralInt:
            		ret += to_string(expr->ival);
            		break;
        	case kExprFunctionRef:
            		ret += string(expr->name) + "?" + expr->expr->name;
            		break;
        	case kExprOperator:
            		ret += operator_expression(expr);
            	break;
        	default:
            		ret += "???";
            		break;
   		 }
    	if (expr->alias != NULL)
        	ret += string(" AS ") + expr->alias;
    	
	return ret;
}

string operator_expression(const Expr *expr) {
    if (expr == NULL)
        return "null";

    string ret;
    if (expr->opType == Expr::NOT)
        ret += "NOT ";
    ret += expression(expr->expr) + " ";
    switch (expr->opType) {
        case Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        case Expr::AND:
            ret += "AND";
            break;
        case Expr::OR:
            ret += "OR";
            break;
        case Expr::NONE:
        	ret += "NONE";
        	break;
        case Expr::BETWEEN:
        	ret += " BETWEEN ";
        	break;
        case Expr::CASE:
        	ret += " CASE ";
        	break;
        case Expr::NOT_EQUALS:
        	ret += " != ";
        	break;
        case Expr::LESS_EQ:
        	ret += " <= ";
        	break;
        case Expr::GREATER_EQ:
        	ret += " >= ";
        	break;
        case Expr::LIKE:
        	ret += " LIKE ";
        	break;
        case Expr::NOT_LIKE:
        	ret += " NOT LIKE ";
        	break;
        case Expr::IN:
        	ret += " IN ";
        	break;
        case Expr::NOT:
        	ret += " NOT ";
        	break;
        case Expr::UMINUS:
        	ret += " UMINUS ";
        	break;
        case Expr::ISNULL:
        	ret += " ISNULL ";
        	break;
        case Expr::EXISTS:
        	ret += " EXISTS ";
        	break;
                default:
                        ret += "???";
                        break;
    }
    if (expr->expr2 != NULL)
        ret += " " + expression(expr->expr2);
    return ret;
}



};

int main (int argc, char* argv[]) {
	NewtShell n;
	n.run(argc, argv);

	return 0;
}

