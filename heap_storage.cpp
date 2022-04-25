#include "db_cxx.h"
#include "storage_engine.h"
#include "heap_storage.h"
#include <cstring>
#include <iostream>

using namespace std;
typedef u_int16_t u16;

/**********************************************SLOTTEDPAGE CLASS***********************************************/

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size())) {
        cerr<< "not enough room for new record" << endl;
        return -1;
    }
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;    

}           

 Dbt* SlottedPage::get(RecordID record_id) {   
    u16 size;    
    u16 loc;    

    get_header(size, loc, record_id);    
    if (loc == 0) {    
        return nullptr; // this is just a tombstone, record has been deleted
    }
    char* data = new char[size];
    memcpy(data,this->address(loc), size);

    return new Dbt(data, size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    //CHECKME
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16) data.get_size();

    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra)) {
            cerr << "Not enough room in block" << endl;
            exit(1);
        } 
    } 

    if (size != 0) {
        this->del(record_id);
    }

    this->end_free -= new_size;
    loc = this->end_free + 1; // place at the end of the free list
    put_header();
    put_header(record_id, new_size, loc);
    memcpy(this->address(loc), data.get_data(), new_size);


    // if (new_size > size) {
    //     u16 extra = new_size - size;
    //     if (!has_room(extra)) {
    //         cerr << "Not enough room in block" << endl;
    //         exit(1);
    //     }
    //     slide(loc + new_size, loc + size);
    //     memcpy(this->address(loc - extra), data.get_data(), new_size);
        
    // } else {
    //     memcpy(this->address(loc), data.get_data(), new_size);
    //     slide(loc + new_size, loc + size);
    // }
    // get_header(record_id, size, loc);
    // put_header(record_id, new_size, loc);

}

void SlottedPage::del(RecordID record_id) {
    u16 size;
    u16 loc;

    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

RecordIDs * SlottedPage::ids(void) {
    u16 size;
    u16 loc;

    RecordIDs* records = new RecordIDs;
    for (u16 i = 1; i <= num_records; i++) {
        get_header(size,loc,i);
        if (size != 0) 
            records->push_back(i);  // add id  
    }

    return records;
}

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    if (id > num_records) {
        cerr<< "Record Not Found id: " + id << endl;
        exit(1);
    } 
    size = get_n(4 * id); // num_records 
    loc = get_n(4 * id + 2); // end_free 
}

bool SlottedPage::has_room(u_int16_t size) {
    u16 available = end_free - (num_records + 1) * 4;
    return size <= available;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u16 shift = end - start;
    if (shift == 0) {
        return;
    }
    
    // Slide Data
    u16 leftMemSize = (start - 1) - end_free;
    memcpy(this->address(end_free + shift + 1), this->address(end_free + 1), leftMemSize);

    // Fix up headers
    for (RecordID &i : *this->ids()) {
        u16 size;
        u16 loc;
        get_header(size, loc, i);
        if (loc <= start) { // left of gap
            loc += shift;
            put_header(i, size, loc);
        }
    }

    end_free += shift;
    put_header(); // update block header   
            
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { 
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

    
/**********************************************HEAPFILE CLASS***********************************************/

HeapFile::~HeapFile() {
    if (!closed) {
        close();
    }
}

void HeapFile::create() {
    //CHECKME
    db_open(DB_CREATE|DB_EXCL);
    SlottedPage* tmp(this->get_new());
    this->put(tmp);

}

void HeapFile::drop() {
    //CHECKME
    close();

    if (std::remove(dbfilename.c_str()) != 0)
        throw "Cannot drop relation";
}

void HeapFile::open() {
    //CHECKME
    db_open();
}

void HeapFile::close() {
    //CHECKME
    closed = true;
    u_int32_t flags = 0;
    db.close(flags);
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new() {
    char* block = new char[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage * HeapFile::get(BlockID block_id) {
    //CHECKME
    if (block_id == 0) {
         return this->get_new();
    }
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);

    return new SlottedPage(data, block_id);
}

void HeapFile::put(DbBlock *block) {
    //CHECKME
    BlockID id = block->get_block_id();
    Dbt key(&id, sizeof(id));
    db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs * HeapFile::block_ids() {
    //CHECKME
    BlockIDs* result = new BlockIDs;

    for (BlockID i = 1; i <= this->last; i++)
        result->push_back(i);

    return result;
}

void HeapFile::db_open(uint flags) {
    //CHECKME
    if(!this->closed) {
        return;
    }

    db.set_message_stream(&cout);
	db.set_error_stream(&cerr);
	db.set_re_len(DbBlock::BLOCK_SZ); // Set record length to 4K

    int success = db.open(nullptr, dbfilename.c_str(), name.c_str(), DB_RECNO, flags, 0644);
    if (success != 0) {
        this->close();
    }
    this->closed = false;
}
/**********************************************HEAPTABLE CLASS***********************************************/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
          :DbRelation(table_name, column_names, column_attributes), file(table_name) { }

void HeapTable::create() {
    file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        file.open();
    } 
    catch (...) {
        file.create();
    }
}

void HeapTable::open() {
    file.open();
}

void HeapTable::close() {
    file.close();
}

void HeapTable::drop() {
    file.drop();
}

Handles * HeapTable::select() {}

// code by Kevin Lundeen
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handle HeapTable::insert(const ValueDict *row) {

    file.open();
    SlottedPage *block = file.get(file.get_last_block_id());
    Dbt *data = marshal(row);
    BlockID block_id;
    RecordID record_id;

    if (validate(row)){
        try{
            record_id = block->add(data);
        }
        catch(...){
            block = this->file.get_new();
            record_id = block->add(data);
        }

    }
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

ValueDict * HeapTable::project(Handle handle) {}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(block_id);
    ValueDict* row = unmarshal(data);
    
    if (column_names->empty())
        return row;
    ValueDict* res = new ValueDict();
    for (int i = 0; i < res->size(); i++)
        res[i] = row[i];
    return res;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
// code by Kevin Lundeen
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }

    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

//similar to marshal
ValueDict* HeapTable::unmarshal(Dbt *data) {
    ValueDict *row = new ValueDict;
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name : column_names) {
        ColumnAttribute ca = column_attributes[col_num++];
        Value val;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            val.n = *(int32_t *)(bytes + offset);
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            val.s = std::string(bytes + offset);
            offset += size;
        }
        else
            throw DbRelationError("Only know how to marshal INT and TEXT");

        (*row)[column_name] = val;
    }
    return row;
}

bool *HeapTable::validate(const ValueDict *row) {
    int val=0;
    for (auto const& column_name: this->column_names) {
        if (row->find(column_name) == row->end()) 
            val ==1;
    }
}

Handle HeapTable::append(const ValueDict *row) {};

