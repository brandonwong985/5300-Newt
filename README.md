# 5300-Newt
Team Newt's DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2022  
## Sprint Invierno
Group Members:  
* Brandon Wong
* Diego Hoyos  

**Note:** If tests fail, make sure to remove any existing database files from the database environment before running tests again to ensure successful subsequent testing.

To build the program, enter:
<br />
$ make

To run this program, enter: 
<br />
$ ./sql5300 <repository>

Note: <repository> can be "~/cpsc5300/data"

To perform a clean run, execute: 
<br />
$ make clean
<br />

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop.
- <code>Milestone2</code> This milestone is a C++ program that represents a storage engine made up of three layers: DbBlock, DbFile, and DbRelation and its implementation for the Heap Storage Engine's version of: SlottedPage, HeapFile, and HeapTable.
- <code>Milestone3</code> This milestone implements the follwoing:
    CREATE TABLE
    DROP TABLE
    SHOW TABLES
    SHOW COLUMNS
- <code>Milestone4</code> This milestone implements the following:
    CREATE INDEX index_name ON table_name [USING {BTREE | HASH}] (col1, col2, ...)
    SHOW INDEX FROM table_name
    DROP INDEX index_name ON table_name
- <code>Milestone5</code> implements INSERT, DELETE, and very simple SELECT statements.
- <code>Milestone6</code> Implementation of B+ Tree Index -- just insert and lookup  

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux)
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.
