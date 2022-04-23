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
    //TODO
}

void HeapFile::drop() {
    //TODO
}

void HeapFile::open() {
    //TODO
}

void HeapFile::close() {
    //TODO
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
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
    //TODO
}

void HeapFile::put(DbBlock *block) {
    //TODO
}

BlockIDs * HeapFile::block_ids() {
    //TODO
}

void HeapFile::db_open(uint flags) {
    //CHECKME
    if(!this->closed) {
        return;
    }
    db.set_message_stream(&cout);
	db.set_error_stream(&cerr);
	db.set_re_len(DbBlock::BLOCK_SZ); // Set record length to 4K
}

/**********************************************HEAPTABLE CLASS***********************************************/



bool test_heap_storage::test_heap_storage() { }