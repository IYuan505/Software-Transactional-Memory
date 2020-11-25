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
#include <condition_variable>
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

#define VALID_BIT 1
#define ACCESS_MASK 30
#define WRITTEN_BIT 32
#define RESERVED_BITS 6

typedef struct batcher batcher_t;
typedef struct access_log access_log_t;
typedef struct transaction transaction_t;
typedef struct segment_entry segment_entry_t;
typedef struct segment segment_t;
typedef struct region region_t;


struct batcher {
    pthread_mutex_t batcher_lock;
    pthread_cond_t batcher_cv;
    int remaining;
    int waiting;
    int timestamp;
};

struct access_log {
    char* position;
    size_t size;
    access_log_t* next;
};

struct transaction {
    access_log_t* access_log;
    segment_entry_t* to_alloc_segment_entry;
    segment_entry_t* to_free_segment_entry;
    bool is_read_only;
    int timestamp;
};

struct segment_entry {
    segment_t* segment;
    segment_entry_t* next;
};

struct segment {
    char* start;
    atomic<int>* control;
    size_t size;
};

struct region {
    batcher_t batcher;
    mutex segment_lock;
    segment_entry_t* segment_entry;
    size_t align;
};

/* ================================================================
                      Segment allocation
   ================================================================ */
segment_t* alloc_segment(size_t size, size_t align){
    segment_t* segment = (segment_t*) malloc(sizeof(segment_t));
    if (unlikely(segment == NULL)) {
        return NULL;
    }

    size_t target_size = size << 1;
    if (unlikely(posix_memalign((void**)&(segment->start), align, target_size) != 0)) {
        free(segment);
        return NULL;
    }
    memset(segment->start, 0, target_size);
    
    size_t control_size = size / align * sizeof(atomic<int>);
    segment->control = (atomic<int>*)malloc(control_size);
    if (unlikely(segment->control==NULL)){
        free(segment->start);
        free(segment);
        return NULL;
    }
    memset(segment->control, 0, control_size);

    segment->size = size;
    return segment;
}

segment_entry_t* alloc_segment_entry(segment_t* segment){
    segment_entry_t* segment_entry = (segment_entry_t*) malloc(sizeof(segment_entry_t));
    if (unlikely(segment_entry==NULL)){
        return NULL;
    }
    segment_entry->segment = segment;
    segment_entry->next = NULL;
    return segment_entry;
}

/* ================================================================
                      Batcher enter and leave
   ================================================================ */

void batcher_enter(batcher_t* batcher, transaction_t* trans){
    pthread_mutex_lock(&batcher->batcher_lock);
    if (batcher->remaining == 0){
        batcher->remaining = 1;
    }
    else {
        batcher->waiting++;
        pthread_cond_wait(&batcher->batcher_cv, &batcher->batcher_lock);
    }
    trans->timestamp = batcher->timestamp;
    pthread_mutex_unlock(&batcher->batcher_lock);
}

void batcher_leave(batcher_t* batcher){
    pthread_mutex_lock(&batcher->batcher_lock);
    batcher->remaining--;
    if (batcher->remaining==0){
        batcher->timestamp++;
        batcher->remaining = batcher->waiting;
        batcher->waiting = 0;
        pthread_cond_broadcast(&batcher->batcher_cv);
    }
    pthread_mutex_unlock(&batcher->batcher_lock);
        
}

/* ================================================================
                        Read and write word
   ================================================================ */


inline segment_t* find_target_seg_helper(segment_entry_t* segment_entry, const void* position){
    while(segment_entry != NULL){
        if (position >= segment_entry->segment->start
            && position < segment_entry->segment->start + segment_entry->segment->size)
            return segment_entry->segment;
        segment_entry = segment_entry->next;
    }
    return NULL;
}

inline segment_t* find_target_seg(region_t* region, transaction_t* trans, const void* position){
    segment_t* return_segment = NULL;
    return_segment = find_target_seg_helper(region->segment_entry, position);
    if (return_segment) return return_segment;
    return_segment = find_target_seg_helper(trans->to_alloc_segment_entry, position);
    return return_segment;
}

inline bool has_access(transaction_t* trans, char* position){
    access_log_t* access_log = trans->access_log;
    while (access_log != NULL){
        // printf("%x, %x, %x\n", position, access_log->position, access_log->position + access_log->size);
        if (position >= access_log->position
            && position < access_log->position + access_log->size)
            return true;
        access_log = access_log->next;
    }
    return false;
}


/* ================================================================
                        Cleanup and commit
   ================================================================ */
inline void free_in_region(region_t* region, transaction_t* trans, bool is_rollback) {
    segment_entry_t* to_free_segment_entry = trans->to_free_segment_entry;
    if (to_free_segment_entry==NULL) return;
    segment_entry_t* tmp;
    if (is_rollback){
        while(to_free_segment_entry != NULL){
            tmp = to_free_segment_entry->next;
            free(to_free_segment_entry);
            to_free_segment_entry = tmp;
        }
    }
    else {
        region->segment_lock.lock();
        while(to_free_segment_entry != NULL){
            segment_entry_t* segment_entry = region->segment_entry;
            segment_entry_t* previous_segment_entry = NULL;
            while(segment_entry != NULL){
                if (segment_entry->segment == to_free_segment_entry->segment){
                    if (previous_segment_entry == NULL){
                        region->segment_entry = segment_entry->next;
                    }
                    else
                        previous_segment_entry->next = segment_entry->next;
                    free(segment_entry->segment->start);
                    free(segment_entry->segment->control);
                    free(segment_entry->segment);
                    free(segment_entry);
                    break;
                }
                previous_segment_entry = segment_entry;
                segment_entry = segment_entry->next;
            }

            tmp = to_free_segment_entry->next;
            free(to_free_segment_entry);
            to_free_segment_entry = tmp;
        }
        region->segment_lock.unlock();
    }
}
inline void alloc_in_region(region_t* region, transaction_t* trans, bool is_rollback) {
    segment_entry_t* to_alloc_segment_entry = trans->to_alloc_segment_entry;
    if (to_alloc_segment_entry==NULL) return;
    segment_entry_t* tmp;
    if (is_rollback){
        while(to_alloc_segment_entry != NULL){
            tmp = to_alloc_segment_entry->next;
            free(to_alloc_segment_entry->segment->start);
            free(to_alloc_segment_entry->segment->control);
            free(to_alloc_segment_entry->segment);
            free(to_alloc_segment_entry);
            to_alloc_segment_entry = tmp;
        }
    }
    else {
        while(to_alloc_segment_entry->next != NULL){
            to_alloc_segment_entry = to_alloc_segment_entry->next;
        }
        region->segment_lock.lock();
        to_alloc_segment_entry->next = region->segment_entry;
        region->segment_entry = trans->to_alloc_segment_entry;
        region->segment_lock.unlock();
    }
}

inline void clear_access_log(transaction_t* trans){
    access_log_t* access_log = trans->access_log;
    access_log_t* tmp;
    while(access_log != NULL){
        tmp = access_log->next;
        free(access_log);
        access_log = tmp;
    }
}

inline void rollback(region_t* region, transaction_t* trans) {
    // printf("ROLLBACK===================================\n");
    free_in_region(NULL, trans, true);
    alloc_in_region(NULL, trans, true);
    clear_access_log(trans);
    free(trans);
    batcher_leave(&region->batcher);
}

inline void commit(region_t* region, transaction_t* trans) {
    // printf("COMMIT\n");
    free_in_region(region, trans, false);
    alloc_in_region(region, trans, false);
    clear_access_log(trans);
    free(trans);
    batcher_leave(&region->batcher);
}

/* ================================================================
                       End of helper functions
   ================================================================ */


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    region_t* region = (region_t*) malloc(sizeof(region_t));
    if (unlikely(region == NULL)) {
        return invalid_shared;
    }

    segment_t* segment = alloc_segment(size, align);
    if (segment==NULL){
        free(region);
        return invalid_shared;
    }

    segment_entry_t* segment_entry = alloc_segment_entry(segment);
    if (segment_entry==NULL){
        free(region);
        free(segment->start);
        free(segment->control);
        free(segment);
        return invalid_shared;
    }

    pthread_mutex_init(&region->batcher.batcher_lock, NULL);
    region->batcher.remaining = 0;
    region->batcher.waiting = 0;
    region->batcher.timestamp = 0;
    region->segment_entry = segment_entry;
    region->align = align;
    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    region_t* region = (region_t*) shared;
    segment_entry_t* segment_entry = region->segment_entry;
    segment_entry_t* tmp;
    while(segment_entry!=NULL){
        tmp = segment_entry->next;
        free(segment_entry->segment->start);
        free(segment_entry->segment->control);
        free(segment_entry->segment);
        free(segment_entry);
        segment_entry = tmp;
    }
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) noexcept {
    return (void*)((region_t*) shared)->segment_entry->segment->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
    return ((region_t*) shared)->segment_entry->segment->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
    return ((region_t*) shared)->align;
}


/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    region_t *region = (region_t*) shared;
    transaction_t* trans = (transaction_t*) malloc(sizeof(transaction_t));
    if (unlikely(trans == NULL)) {
        return invalid_tx;
    }

    batcher_enter(&region->batcher, trans);
    trans->access_log = NULL;
    trans->to_alloc_segment_entry = NULL;
    trans->to_free_segment_entry = NULL;
    trans->is_read_only = is_ro;

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
    commit(region, trans);
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

    size_t align = region->align;
    int index = (char*)source - target_seg->start;
    char* source_pos = (char*)source + index;
    char* target_pos = (char*)target;
    atomic<int>* control = target_seg->control + (index / align);

restart:
    int control_value = atomic_load(control);
    int valid = control_value & VALID_BIT;
    int access_cnt = (control_value & ACCESS_MASK)>>1;
    int written = control_value & WRITTEN_BIT;
    int timestamp = control_value>>RESERVED_BITS;

    if (timestamp < trans->timestamp){
        if (written){
            valid = 1 - valid;
            written = 0;
        }
        access_cnt = 0;
        timestamp = trans->timestamp;
        control_value = (trans->timestamp << RESERVED_BITS ) | (valid*VALID_BIT);
        atomic_store_explicit(control, control_value, memory_order_release);
    }

    bool in_access_set = has_access(trans, source_pos);
    if (trans->is_read_only){
        memcpy(target_pos, source_pos + (valid * align), align);
        return true;
    }
    else {
        if (written){
            if (in_access_set){
                memcpy(target_pos, source_pos + ((1 - valid) * align), align);
            }
            else
                return false;
        }
        else {
            memcpy(target_pos, source_pos + (valid * align), align);
            if (!in_access_set)
                access_cnt += 1;
            int target_control_value = (timestamp<<RESERVED_BITS) | (access_cnt<<1) | valid;
            if(unlikely(atomic_compare_exchange_strong(control, &control_value, target_control_value)==false))
                goto restart;
        }
        access_log_t* access_log = (access_log_t*) malloc(sizeof(access_log_t));
        if (unlikely(access_log == NULL)){
            rollback(region, trans);
            return false;
        }
        access_log->position = source_pos;
        access_log->size = size + size;
        access_log->next = trans->access_log;
        trans->access_log = access_log;
    }

    
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

    size_t align = region->align;
    int index = (char*)target - target_seg->start;
    char* source_pos = (char*)source;
    char* target_pos = (char*)target + index;
    atomic<int>* control = target_seg->control + (index / align);

restart:
    int control_value = atomic_load_explicit(control, memory_order_consume);
    int valid = control_value & VALID_BIT;
    int access_cnt = (control_value & ACCESS_MASK)>>1;
    int written = control_value & WRITTEN_BIT;
    int timestamp = control_value>>RESERVED_BITS;

    if (timestamp < trans->timestamp){
        if (written){
            valid = 1 - valid;
            written = 0;
        }
        access_cnt = 0;
        timestamp = trans->timestamp;
        control_value = (timestamp << RESERVED_BITS ) | (valid*VALID_BIT);
        atomic_store_explicit(control, control_value, memory_order_release);
    }

    bool in_access_set = has_access(trans, target_pos);
    if (written){
        if (in_access_set){
            memcpy(target_pos + ((1-valid) * align), source_pos, align);
        }
        else
            return false;
    }
    else {

        if ( (in_access_set && access_cnt>1) || (!in_access_set && access_cnt>0)) return false;
        else {
            memcpy(target_pos + ((1-valid) * align), source_pos, align);
            if (!in_access_set)
                access_cnt += 1;
            int target_control_value = (timestamp<<RESERVED_BITS) | WRITTEN_BIT | (access_cnt<<1) | valid;
            if(unlikely(atomic_compare_exchange_strong(control, &control_value, target_control_value)==false)){
                goto restart;
            }
        }
    }

    
    access_log_t* access_log = (access_log_t*) malloc(sizeof(access_log_t));
    if (unlikely(access_log == NULL)){
        rollback(region, trans);
        return false;
    }
    access_log->position = target_pos;
    access_log->size = size + size;
    access_log->next = trans->access_log;
    trans->access_log = access_log;
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

    segment_t* segment = alloc_segment(size, region->align);
    if (segment==NULL){
        return Alloc::nomem;
    }

    segment_entry_t* segment_entry = alloc_segment_entry(segment);
    if (segment_entry==NULL){
        free(segment->start);
        free(segment->control);
        free(segment);
        return Alloc::nomem;
    }

    segment_entry->next = trans->to_alloc_segment_entry;
    trans->to_alloc_segment_entry = segment_entry;
    *target = segment->start;
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
    segment_t* target_seg = find_target_seg(region, trans, target);

    segment_entry_t* segment_entry = alloc_segment_entry(target_seg);
    if (segment_entry==NULL){
        rollback(region, trans);
        return false;
    }

    segment_entry->next = trans->to_free_segment_entry;
    trans->to_free_segment_entry = segment_entry;

    return true;

}
