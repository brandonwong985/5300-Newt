/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

// make query result be printable
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


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr){
        SQLExec::tables = new Tables();
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

QueryResult *SQLExec::create(const CreateStatement *statement) {
    // checks for 2 conditions -> create table and create index
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("not implemented");
    }
}

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

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
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

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
     Identifier table_name = statement->tableName;
     Identifier index_name = statement->indexName;
     Identifier index_type;
     DbIndex &index = SQLExec::indices->get_index(table_name, index_name);  
     bool unique = true;

     try {
         index_type = statement->indexType;
     }catch (exception& e){
         index_type = "BTREE";
     }

     if (index_type != "BTREE"){ 
         unique = false;
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

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier tableName = statement->tableName;
    Identifier indexName = statement->indexName;

    DbIndex &index = SQLExec::indices->get_index(tableName, indexName);

    try {
        index.drop();
    } catch (...) {
        return new QueryResult("index " + indexName + " not found.");
    }

    ValueDict where;

    where["table_name"] = Value(tableName);
    where["index_name"] = Value(indexName);
    Handles *handles = SQLExec::indices->select(&where);

    for (auto const &handle: *handles) {
        SQLExec::indices->del(handle);
    }

    delete handles;

    return new QueryResult("dropped index " + indexName);  // FIXME
}

