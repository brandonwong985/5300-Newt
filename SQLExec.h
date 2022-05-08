/**
 * @file SQLExec.h - SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#pragma once

#include <exception>
#include <string>
#include "SQLParser.h"
#include "schema_tables.h"

/**
 * @class SQLExecError - exception for SQLExec methods
 */
class SQLExecError : public std::runtime_error {
public:
    explicit SQLExecError(std::string s) : runtime_error(s) {}
};


/**
 * @class QueryResult - data structure to hold all the returned data for a query execution
 */
class QueryResult {
public:
    QueryResult() : column_names(nullptr), column_attributes(nullptr), rows(nullptr), message("") {}

    QueryResult(std::string message) : column_names(nullptr), column_attributes(nullptr), rows(nullptr),
                                       message(message) {}

    QueryResult(ColumnNames *column_names, ColumnAttributes *column_attributes, ValueDicts *rows, std::string message)
            : column_names(column_names), column_attributes(column_attributes), rows(rows), message(message) {}

    virtual ~QueryResult();

    ColumnNames *get_column_names() const { return column_names; }

    ColumnAttributes *get_column_attributes() const { return column_attributes; }

    ValueDicts *get_rows() const { return rows; }

    const std::string &get_message() const { return message; }

    friend std::ostream &operator<<(std::ostream &stream, const QueryResult &qres);

protected:
    ColumnNames *column_names;
    ColumnAttributes *column_attributes;
    ValueDicts *rows;
    std::string message;
};


/**
 * @class SQLExec - execution engine
 */
class SQLExec {
public:
    /**
     * Execute the given SQL statement.
     * @param statement   the Hyrise AST of the SQL statement to execute
     * @returns           the query result (freed by caller)
     */
    static QueryResult *execute(const hsql::SQLStatement *statement);

protected:
    // the one place in the system that holds the _tables table
    static Tables *tables;

    // Execute SQL statement for creating a table
    static QueryResult *create(const hsql::CreateStatement *statement);

    // Execute SQL statement for dropping a table
    static QueryResult *drop(const hsql::DropStatement *statement);

    // Execute SQL statement for showing tables/showing columns from table
    static QueryResult *show(const hsql::ShowStatement *statement);

    // Execute SQL statement for showing showing tables
    static QueryResult *show_tables();

    // Execute SQL statemnet foe showing columns
    static QueryResult *show_columns(const hsql::ShowStatement *statement);

    /**
     * Pull out column name and attributes from AST's column definition clause
     * @param col                AST column definition
     * @param column_name        returned by reference
     * @param column_attributes  returned by reference
     */
    static void
    column_definition(const hsql::ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute);
};

