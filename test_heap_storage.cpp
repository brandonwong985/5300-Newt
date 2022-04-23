#include "db_cxx.h"
#include "heap_storage.h"
#include <string.h>
#include <memory>

using namespace std;

DbEnv *_DB_ENV;

//bool test_heap_storage();

int main(){
  _DB_ENV = new DbEnv(0U);
  _DB_ENV->set_message_stream(&cout);
	_DB_ENV->set_error_stream(&cerr);
	_DB_ENV->open("/home/st/trananh/PhysicalDatabase/data", DB_CREATE | DB_INIT_MPOOL, 0);

  
  char* testStr1 = (char*) "Here, cometh the age of inquiry, not of nature but Nature Within.";
  char* testStr2 = (char*) "Lord of the pillared halls. What reign do you have over us?";
  char* testStr3 = (char*) "As the path stretches into darkness, into fear, doubt, and loneliness.";
  char* testStr4 = (char*) "Beyond the Circles";
  char* testStr5 = (char*) "Last light of the Alder days... when all of creation was raw and violent...";
  char* testStr6 = (char*) "I seek the Lamb, so that I may graze the Divine grassland.";
  char* testStr7 = (char*) "Ista, before the days. Whole.";
    
  Dbt testBlk1(testStr1, strlen(testStr1) + 1);
  Dbt testBlk2(testStr2, strlen(testStr2) + 1);
  Dbt testBlk3(testStr3, strlen(testStr3) + 1);
  Dbt testBlk4(testStr4, strlen(testStr4) + 1);
  Dbt testBlk5(testStr5, strlen(testStr5) + 1);
  Dbt testBlk6(testStr6, strlen(testStr6) + 1);
  Dbt testBlk7(testStr7, strlen(testStr7) + 1);


  cout << "*************************************TEST SLOTTEDPAGE**************************************" <<endl;
  Dbt placebo(new char[20*DbBlock::BLOCK_SZ], 20*DbBlock::BLOCK_SZ);
  SlottedPage page = SlottedPage(placebo, 0, true);

  // Test ADD
  u_int16_t id;
  u_int16_t id2; 

  cout << "Adding str1: " << endl << testStr1 << endl;      
  id = page.add(&testBlk1);
  cout << "ID: " << id << endl;
  cout << "Content: " << (char*)page.get(id)->get_data() << endl;
  cout << endl;   

  cout << "Adding str2: " << endl << testStr2 << endl;
  id2 = page.add(&testBlk2);
  cout << "ID: " << id2 << endl;
  cout << "Content: " << (char*)page.get(id2)->get_data() << endl;   
  cout << endl;

  // Test Replacement
  cout << "test put" << endl;
  page.put(id2, testBlk4);
  for (RecordID &i : *page.ids()) {
    cout << "ID: " << i << endl;
    cout << "Content: " << (char*)page.get(i)->get_data() << endl;
    cout << endl;    
  }
  cout << "expected" << endl;
  cout << testStr1 << endl;
  cout << testStr4 << endl;
  cout << endl;

  page.put(id, testBlk5);
  for (RecordID &i : *page.ids()) {
    cout << "ID: " << i << endl;
    cout << "Content: " << (char*)page.get(i)->get_data() << endl;
    cout << endl;    
  }
  cout << "expected" << endl;
  cout << testStr5 << endl;
  cout << testStr4 << endl;
  cout << endl;
  cout << endl;

  // Test iteration
  cout << "test iteration" << endl;
  for (RecordID &i : *page.ids()) {
    cout << i << endl;
  }
  cout << "actual ids" << endl;
  cout << id << " " << id2 << endl;
  cout << endl;

  // Test DELETE
  cout << "test delete" << endl;
  page.del(id2);
  if (page.get(id2) == nullptr) {
    cout << "id2 successfully removed" << endl;
  } 
  cout << endl;
  
  cout << "test iteration of new list" << endl;
  for (RecordID &i : *page.ids()) {
    cout << i << endl;
  }
  cout << "actual id" << endl;
  cout << id << endl;
  cout << endl;
  
  cout << "Add statement 3" << endl;
  page.add(&testBlk3);
  for (RecordID &i : *page.ids()) {
    cout << "ID: " << i << endl;
    cout << "Content: " << (char*)page.get(i)->get_data() << endl;
    cout << endl;    
  }
  cout << "expected" << endl;
  cout << testStr5 << endl;
  cout << testStr3 << endl;
  cout << endl;

  cout << "******************************TEST HEAPFILE****************************************************" << endl;
  cout << endl;
  cout << "\nCreating/Opening HeapFile..." << endl;
  HeapFile hFile("TestRelation");
    
  try {
    hFile.create();
    cout << "TestRelation.poo created" << std::endl; 
  } catch(...) {
    hFile.open();
    cout << "TestRelation.poo opened" << std::endl; 
  }

  return 0;
}



// test function -- returns true if all tests pass
// bool test_heap_storage() {
// 	ColumnNames column_names;
// 	column_names.push_back("a");
// 	column_names.push_back("b");
// 	ColumnAttributes column_attributes;
// 	ColumnAttribute ca(ColumnAttribute::INT);
// 	column_attributes.push_back(ca);
// 	ca.set_data_type(ColumnAttribute::TEXT);
// 	column_attributes.push_back(ca);
//     HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
//     table1.create();
//     std::cout << "create ok" << std::endl;
//     table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
//     std::cout << "drop ok" << std::endl;

//     HeapTable table("_test_data_cpp", column_names, column_attributes);
//     table.create_if_not_exists();
//     std::cout << "create_if_not_exsts ok" << std::endl;

//     ValueDict row;
//     row["a"] = Value(12);
//     row["b"] = Value("Hello!");
//     std::cout << "try insert" << std::endl;
//     table.insert(&row);
//     std::cout << "insert ok" << std::endl;
//     Handles* handles = table.select();
//     std::cout << "select ok " << handles->size() << std::endl;
//     ValueDict *result = table.project((*handles)[0]);
//     std::cout << "project ok" << std::endl;
//     Value value = (*result)["a"];
//     if (value.n != 12)
//     	return false;
//     value = (*result)["b"];
//     if (value.s != "Hello!")
// 		return false;
//     table.drop();

//     return true;
// }
