CXX := g++
CXXFLAGS := -I/usr/local/db6/include -std=c++17 -Wall -g
LFLAGS := -L/usr/local/db6/lib -ldb_cxx -lsqlparser

test: test_heap_storage.o heap_storage.o 
	$(CXX) $(LFLAGS) -o $@ $^

test_heap_storage.o: test_heap_storage.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

sql5300: sql5300.o heap_storage.o
	$(CXX) $(LFLAGS) -o $@ $^

sql5300.o: sql5300.cpp
	$(CXX) $(CXXFLAGS) -c -o $@ $<

heap_storage.o : heap_storage.cpp heap_storage.h storage_engine.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean : 
	rm -f *.o sql5300 test