# 5300-Newt

CPSC 5300 Sprint Verano, Seattle University, Spring 2022

Authors: Anh Tran, Sanchita Jain

**HAND-OFF VIDEO**: 


**Milestone 1:**
--------------------------------

This milestone is a C++ program that runs from the command line and allows users to input statement, parsing the input statement into a formatted SQL statement.

To build the program, enter:
<br />
$ make

To run the program, enter: 
<br />
$ ./sql5300 env_path
<br />
where the env_path is the directory holding Berkeley DB database files.

When run, the terminal will appear a SQL entry where users can enter the input statement like below: 
<br />
SQL>

To exit the program, enter: 
<br />
SQL> quit


**Milestone 2:**
-------------------------------
This milestone is a C++ program that represents a storage engine made up of three layers: DbBlock, DbFile, and DbRelation and its implementation for the Heap Storage Engine's version of: SlottedPage, HeapFile, and HeapTable.

To build the program, enter:
<br />
$ make

To run the tests for this program, enter: 
<br />
$ ./test 

To perform a clean run, execute: 
<br />
$ make clean
<br />


