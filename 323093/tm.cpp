/**
 * @file   tm.c
 * @author Sébastien Rouault <sebastien.rouault@epfl.ch>
 *
 * @section LICENSE
 *
 * Copyright © 2018-2019 Sébastien Rouault.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * any later version. Please see https://gnu.org/licenses/gpl.html
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * @section DESCRIPTION
 *
 * Lock-based transaction manager implementation used as the reference.
**/

// Compile-time configuration
// #define USE_MM_PAUSE
// #define USE_PTHREAD_LOCK
// #define USE_TICKET_LOCK
#define USE_RW_LOCK

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <atomic>
// Internal headers
#include <tm.hpp>

#include <iostream>
using namespace std;

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
**/
#undef likely
#ifdef __GNUC__
    #define likely(prop) \
        __builtin_expect((prop) ? 1 : 0, 1)
#else
    #define likely(prop) \
        (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
**/
#undef unlikely
#ifdef __GNUC__
    #define unlikely(prop) \
        __builtin_expect((prop) ? 1 : 0, 0)
#else
    #define unlikely(prop) \
        (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
**/
#undef as
#ifdef __GNUC__
    #define as(type...) \
        __attribute__((type))
#else
    #define as(type...)
    #warning This compiler has no support for GCC attributes
#endif

// -------------------------------------------------------------------------- //

#define word_size = sizeof(uintptr_t)

typedef struct segment {
    atomic<int> lock;
    void* start;
    size_t size;
} segment_t;

typedef struct region {
    void* start;
    atomic<int> seg_lock;
    vector<segment_t*> segments;
    size_t size;
    size_t align;
} region_t;

typedef struct log{
    size_t size;
    void* location;
    void* old_data;
    struct log* next;
} log_t;

typedef struct transaction {
    log_t* logs;
    vector<segment_t*> to_free;
    vector<segment_t*> to_alloc;
    bool is_ro;
    vector<atomic<int>*> locks;
} transaction_t;

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept{
    region_t* region = new (std::nothrow) region_t();
    if (unlikely(region == NULL)) {
        return invalid_shared;
    }

    segment_t* seg = new (std::nothrow) segment_t();
    if (unlikely(seg == NULL)) {
        delete region;
        return invalid_shared;
    }

    if (unlikely(posix_memalign(&(region->start), align, size) != 0)){
        delete seg;
        delete region;
        return invalid_shared;
    }

    memset(region->start, 0, size);
    region->align = align;
    region->size = size;
    region->seg_lock = 0;

    seg->lock = 0;
    seg->start = region->start;
    seg->size = size;

    region->segments.push_back(seg);

    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    region_t* region = (region_t*) shared;
    for (auto seg : region->segments){
        free(seg->start);
        delete seg;
    }
    delete region;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return ((region_t*) shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    return ((region_t*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    return ((region_t*) shared)->align;
}

//================================================================
//Helper functions
//================================================================
void free_segments(region_t *region, vector<segment*> to_free){
    int expected_lock = 0;
    while(atomic_compare_exchange_strong(&region->seg_lock, &expected_lock, 1)==false)
        expected_lock=0;

    int index = 0;
    for(auto seg : region->segments){
        for(auto seg_to_free : to_free){
            if(seg == seg_to_free){
                region->segments.erase(region->segments.begin() + index);
                free(seg->start);
                delete seg;
            }
        }
        ++index;
    }

    expected_lock = 1;
    atomic_compare_exchange_strong(&region->seg_lock, &expected_lock, 0);
}

void rollback(transaction_t* trans){
    if (!trans->is_ro){
        log_t* change = trans->logs;
        log_t* tmp;
        while(change != NULL){
            memcpy(change->location, change->old_data, change->size);
            free(change->old_data);
            tmp = change->next;
            delete change;
            change = tmp;
        }
    }

    for (auto lock : trans->locks) {
        int expected_lock = 1;
        atomic_compare_exchange_strong(lock, &expected_lock, 0);
    }
    
    delete trans;
    return;
}

bool check_lock(transaction_t* trans, atomic<int>* lock){
    for(auto candidate : trans->locks){
       if (candidate == lock) {
            return true;
       }
    }
    return false;
}

segment_t *find_target_seg(region_t *region, transaction_t *trans, const void *pos){
    for(auto seg : region->segments){
        if(pos >= seg->start && pos < (char*)seg->start + seg->size){
            return seg;
        }
    }
    for(auto seg: trans->to_alloc){
        if(pos >= seg->start && pos < (char*)seg->start + seg->size){
            return seg;
        }
    }
    return NULL;
}

//================================================================
// End of Helper functions
//================================================================

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared as(unused), bool is_ro) noexcept {
    transaction_t* trans = new (std::nothrow) transaction_t();
    if(unlikely(trans == NULL)){
       return invalid_tx;
    }
    trans->is_ro = is_ro;
    trans->logs = NULL;
    return (tx_t) trans;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) noexcept {
    region_t *region = (region_t*) shared;
    transaction_t *trans = (transaction_t*) tx;

    if (!trans ->is_ro){
        free_segments(region, trans->to_free);

        log_t* change = trans->logs;
        log_t* tmp;
        while(change != NULL){
            free(change->old_data);
            tmp = change->next;
            delete change;
            change = tmp;
        }

        for(auto seg: trans->to_alloc)
            region->segments.push_back(seg);
    }

    for (auto lock : trans->locks) {
       int expected_lock = 1;
       atomic_compare_exchange_strong(lock, &expected_lock, 0);
    }
    delete trans;
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    region_t* region = (region_t*) shared;
    transaction_t* trans = (transaction_t*) tx;
    segment_t* target_seg = find_target_seg(region, trans, source);

    if (target_seg==NULL){
        rollback(trans);
        return false;
    }

    if (!check_lock(trans, &target_seg->lock)){
        int expected_lock = 0;
        if(atomic_compare_exchange_strong(&target_seg->lock, &expected_lock, 1)==false){
            rollback(trans);
            return false;
        } else {
            trans->locks.push_back(&target_seg->lock);
        }
    }

    memcpy(target, source, size);
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) noexcept {
    region_t* region = (region_t*) shared;
    transaction_t* trans = (transaction_t*) tx;
    segment_t* target_seg = find_target_seg(region, trans, target);

    if (target_seg==NULL){
        rollback(trans);
        return false;
    }

    if (!check_lock(trans, &target_seg->lock)){
        int expected_lock = 0;
        if(atomic_compare_exchange_strong(&target_seg->lock, &expected_lock, 1)==false){
            rollback(trans);
            return false;
        } else {
            trans->locks.push_back(&target_seg->lock);
        }
    }

    log_t* change = new (std::nothrow) log_t();
    if (unlikely(change == NULL)){
        rollback(trans);
        return false;
    }
    change->old_data = malloc(sizeof(byte) * size);
    if (unlikely(change->old_data == NULL)){
        rollback(trans);
        return false;
    }
    memcpy(change->old_data, target, size);
    change->size = size;
    change->location = target;
    change->next = trans->logs;

    //remember the log
    trans->logs = change;
    //copy the memory
    memcpy(target, source, size);
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size, void** target) noexcept {
    region_t* region = (region_t*) shared;
    transaction_t* trans = (transaction_t*) tx;

    segment_t* seg = new (std::nothrow) segment_t();
    if (unlikely(seg == NULL)) {
        return Alloc::nomem;
    }

    if (unlikely(posix_memalign((void**) &(seg->start), region->align, size) != 0)){
        delete seg;
        return Alloc::nomem;
    }
    memset(seg->start, 0, size);
    *target = seg->start;
    seg->size = size;
    seg->lock = 0;
    trans->to_alloc.push_back(seg);

    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) noexcept {
    region_t* region = (region_t*) shared;
    transaction_t* trans = (transaction_t*) tx;

    for(auto seg : region->segments){
        if(target == seg->start){
            if (!check_lock(trans, &seg->lock)){
                int expected_lock = 0;
                if(atomic_compare_exchange_strong(&seg->lock, &expected_lock, 1)==false){
                    rollback(trans);
                    return false;
                }
            }
            trans->locks.push_back(&seg->lock);
            trans->to_free.push_back(seg);
            return true;
        }
    }
    //if the address is not in the given region, abort the transaction
    rollback(trans);
    return false;
}