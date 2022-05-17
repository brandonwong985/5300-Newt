/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Helen Huang, Yao Yao
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include <cassert>
#include <cstring>

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// Make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n != 0 ? "true" : "false");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

// Destructor
QueryResult::~QueryResult() {
    if (column_names != nullptr){
        delete column_names;
    }
    
    if (column_attributes != nullptr){
        delete column_attributes;
    }

    if (rows != nullptr){
        for (auto row: *rows){
            delete row;
        }
        delete rows;
    }
}

// Execute passed SQL statement
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr){
        SQLExec::tables = new Tables();
    }

    if (SQLExec::indices == nullptr){
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Set the column data type definition
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("Data type not implemented");
    }
}

// Execute sql create statements
// creates a table/index upon request
QueryResult *SQLExec::create(const CreateStatement *statement) {
    // checks for 2 conditions -> create table and create index
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("unknown create type");
    }
}

// Execute the sql create table statement
// create a table
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    // Updated table schema
    ValueDict row;
    Identifier table_name = statement->tableName;
    row["table_name"] = table_name;   
    Handle table_handle = SQLExec::tables->insert(&row);

    Identifier column_name;
    ColumnAttribute column_attribute; 
    ColumnNames column_names;
    ColumnAttributes column_attributes;

    DbRelation &table = SQLExec::tables->get_table(table_name); 
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);  

    // fill column names and column attributes
    for (auto const &col: *statement->columns){
        column_definition((const ColumnDefinition *)col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    try{        
        // Updated column schema

        Handles col_handle;   
        for (uint i = 0; i < column_names.size(); i++){
            row["column_name"] = column_names[i];
            if (column_attributes[i].get_data_type() == ColumnAttribute::INT){
                row["data_type"] = Value("INT");
            }else if (column_attributes[i].get_data_type() == ColumnAttribute::TEXT){
                row["data_type"] = Value("TEXT");
            }else{
                throw SQLExecError("Column data type not implemented");
            }
            col_handle.push_back(columns.insert(&row));
        }
        try{
            // Create table
            table.create();
        }catch (...){
            try{
                // Undo insertions into _columns
                for (uint i = 0; i < column_names.size(); i++){
                    columns.del(col_handle.at(i));
                }
            }catch (...) { }
            throw;
        }
    }
    catch (...){        
        try{
            // Undo insertion into _tables
            SQLExec::tables->del(table_handle);
        }catch (...){ }
        throw;
    }
    return new QueryResult("created " + table_name);
}

// Executes the sql drop statement
QueryResult *SQLExec::drop(const DropStatement *statement) {
    // checks for 2 conditions -> drop table and drop index
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

// Executes the sql drop table statement
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    if (statement->type != hsql::DropStatement::kTable){
        throw SQLExecError("Unrecognized DROP type");
    }

    if (Value(statement->name) == Tables::TABLE_NAME || Value(statement->name) == Columns::TABLE_NAME){
        throw SQLExecError("Cannot drop a schema table");
    }

    ValueDict where;
    Identifier table_name = statement->name;
    
    // Get table in _tables to drop
    DbRelation &table = SQLExec::tables->get_table(table_name);   
    where["table_name"] = Value(table_name);

    // Get rows in _columns to drop
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles *handles = columns.select(&where);
    for (auto const &handle: *handles){
        columns.del(handle);
    }
    delete handles;

    table.drop();
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(string("dropped ") + table_name);
}

// Executes the sql show statements
QueryResult *SQLExec::show(const ShowStatement *statement) {
    // checks for 2 conditions -> show tables and show columns
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

// Executes the sql show tables statement 
QueryResult *SQLExec::show_tables() {
    // get all tables
    Handles *handles = SQLExec::tables->select();
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();
    int count = 0;

    column_names->push_back("table_name");
    
    for (auto const &handle: *handles){
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table = row->at("table_name").s;
        if (table == Tables::TABLE_NAME || table == Columns::TABLE_NAME || table == Indices::TABLE_NAME){
            continue;
        }
        count++;
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(column_names, new ColumnAttributes(), rows, "successfully returned " + to_string(count) + " rows"); 
}

// Executes the show columns statement
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {   
    Identifier table_name = statement->tableName;
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);   

    ColumnNames *column_names = new ColumnNames();
    ColumnAttributes *column_attributes = new ColumnAttributes();
    ValueDicts *rows = new ValueDicts();

    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");
    column_attributes->push_back(ColumnAttribute::TEXT);
    
    int count = 0;
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *handles = columns.select(&where); ;

    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names); 
        rows->push_back(row);
        count++;
    }
    delete handles;

    return new QueryResult(column_names, column_attributes, rows,  "successfully returned " + to_string(count) + " rows");
}

//========================================Indices===========================================

// Executes sql create index statement
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
     Identifier table_name = statement->tableName;
     Identifier index_name = statement->indexName;
     Identifier index_type;
     DbIndex &index = SQLExec::indices->get_index(table_name, index_name);  
     bool unique = false;

     try {
         index_type = statement->indexType;
     }catch (exception& e){
         index_type = "BTREE";
     }
    
    if (index_type == "BTREE"){
        unique = true;
    }

     ValueDict row;
     int seq = 0;

     row["table_name"] = table_name;
     row["index_name"] = index_name;
     row["seq_in_index"] = seq;
     row["index_type"] = index_type;
     row["is_unique"] = unique;

     Handles handles;
     try{
         for (auto const &col: *statement->indexColumns){
             seq += 1;
             row["seq_in_index"] = seq;
             row["column_name"] = Value(col);           
             handles.push_back(SQLExec::indices->insert(&row));
         }
         // Create index
         index.create();
     } catch (exception& e){
         try{
             for (unsigned int i = 0; i < handles.size(); i++){
                 SQLExec::indices->del(handles.at(i));
             }
         }catch (...){ }
         throw;
     }
     return new QueryResult("created index " + index_name);
}

// Executes the sql show index statement
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames *column_names = new ColumnNames();
    ValueDicts *rows = new ValueDicts();

    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    int count = 0;
    ValueDict where;
    where["table_name"] = Value(table_name);
    Handles *handles = SQLExec::indices->select(&where);

    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names); 
        rows->push_back(row);
        count++;
    }
    delete handles;

    return new QueryResult(column_names, new ColumnAttributes(), rows,  "successfully returned " + to_string(count) + " rows");
}

// Executes the sql drop index statement 
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier tableName = statement->name;
    Identifier indexName = statement->indexName;

    DbIndex &index = SQLExec::indices->get_index(tableName, indexName);

    // first drop the index
    try {
        index.drop();
    } catch (...) {
        return new QueryResult("index " + indexName + " not found.");
    }

    ValueDict where;
    // then delete the rows
    where["table_name"] = Value(tableName);
    where["index_name"] = Value(indexName);
    Handles *handles = SQLExec::indices->select(&where);

    for (auto const &handle: *handles) {
        SQLExec::indices->del(handle);
    }

    delete handles;

    return new QueryResult("dropped index " + indexName);
}

// Tests all implemented sql statements for tables
bool test_sql_tables(){
    string queries [] = {
            "show tables",
            "show columns from _tables",
            "show columns from _columns",
            "create table foo (id int, data text, x int, y int, z int)",
            "create table foo (goober int)",
            "create table goo (x int, x text)",
            "show tables",
            "show columns from foo",
            "drop table foo",
            "show tables",
            "show columns from foo"
    };

    string expected_results [] = {
            "successfully returned 0 rows",
            "successfully returned 1 rows",
            "successfully returned 3 rows",
            "created foo",
            "error",
            "error",
            "successfully returned 1 rows",
            "successfully returned 5 rows",
            "dropped foo",
            "successfully returned 0 rows",
            "successfully returned 0 rows"
    };

    for(uint i = 0; i < 11; i++){
        SQLParserResult *parse = SQLParser::parseSQLString(queries[i]);
        if(!parse->isValid()){
            return false;
        }else{

            for(uint j = 0; j < parse->size(); j++){
                try
                {
                    const SQLStatement *statement = parse->getStatement(j);
                    QueryResult *result = SQLExec::execute(statement);
                    assert(strcmp(result->get_message().c_str(), expected_results[i].c_str()) == 0);

                    // cout << "result: " << result->get_message().c_str() << endl;

                    delete result;
                } catch (SQLExecError &e)
                {
                    cout << "Error: " << e.what() << endl;
                }

            }
        }
        delete parse;
    }

    return true;
}

// Tests all implemented sql statements for indices
bool test_sql_indices(){
    string queries [] = {
        "create table ha (x int, y int, z int)",
        "create index fx on ha (x,y)",
        "show index from ha",
        "drop index fx from ha",
        "show index from ha",
        "create index fx on ha (x)",
        "show index from ha",
        "create index fyz on ha (y,z)",
        "show index from ha",
        "drop index fx from ha",
        "show index from ha",
        "drop index fyz from ha",
        "show index from ha",
        "drop table ha"
    };

    string expected_results [] = {
        "created ha",
        "created index fx",
        "successfully returned 2 rows",
        "dropped index fx",
        "successfully returned 0 rows",
        "created index fx",
        "successfully returned 1 rows",
        "created index fyz",
        "successfully returned 3 rows",
        "dropped index fx",
        "successfully returned 2 rows",
        "dropped index fyz",
        "successfully returned 0 rows",
        "dropped ha"
    };
    
    for (uint i = 0; i < 14; i++){
        SQLParserResult *parse = SQLParser::parseSQLString(queries[i]);
        if (!parse->isValid()) {
            return false;
        } else {
            for (uint j = 0; j < parse->size(); ++j) {
                const SQLStatement *statement = parse->getStatement(j);
                QueryResult *result = SQLExec::execute(statement);
                assert(strcmp(result->get_message().c_str(), expected_results[i].c_str()) == 0);
                delete result;           
            }
        }
        delete parse;
    }  
    return true;
}