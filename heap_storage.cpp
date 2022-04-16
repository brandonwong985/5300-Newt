#pragma once

#include "heap_storage.h"
#include "db_cxx.h"
#include "storage_engine.h"

class SlottedPage : public DbBlock {
    
    typedef u_int16_t u16;
    
    public:
    SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
        if (is_new) {
            this->num_records = 0;
            this->end_free = DbBlock::BLOCK_SZ - 1;
            put_header();
        } else {
            get_header(this->num_records, this->end_free);
        }
    }

    virtual ~SlottedPage() {}

    // Add a new record to the block. Return its id.
    RecordID SlottedPage::add(const Dbt* data) {
        if (!has_room(data->get_size()))
            throw DbBlockNoRoomError("not enough room for new record");
        u16 id = ++this->num_records;
        u16 size = (u16) data->get_size();
        this->end_free -= size;
        u16 loc = this->end_free + 1;
        put_header();
        put_header(id, size, loc);
        memcpy(this->address(loc), data->get_data(), size);
        return id;
    }

    Dbt *get(RecordID record_id) {
        //TODO
    }

    void put(RecordID record_id, const Dbt &data) {
        //TODO
    }

    void del(RecordID record_id) {
        //TODO
    }

    RecordIDs *ids(void) {
        //TODO
    }

protected:
    u_int16_t num_records; // 2 Bytes
    u_int16_t end_free; // 2 Bytes


    void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0) {
        //TODO
        size = get_n(4 * id); // num_records 
        loc = get_n(4 * id + 2); // end_free 
    }

    bool has_room(u_int16_t size) {
        //TODO
        u16 available = end_free - (num_records + 1) * 4
        return size <= available
    }

    void slide(u_int16_t start, u_int16_t end) {
        //TODO
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
        if (id == 0) { // called the put_header() version and using the default params
            size = this->num_records;
            loc = this->end_free;
        }
        put_n(4*id, size);
        put_n(4*id + 2, loc);
    }

    
};



bool test_heap_storage() {return true;}

