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

#define RESERVED_BIT 1
#define WRITE_BIT 1
#define CLEAR_MAST (~1)

typedef struct log log_t;
typedef struct segment_entry segment_entry_t;
typedef struct read_write_entry read_write_entry_t;
typedef struct transaction transaction_t;
typedef struct segment segment_t;
typedef struct region region_t;

struct log {
    size_t size;
    void* location;
    void* old_data;
    struct log* next;
};

struct segment_entry {
    segment_t* segment;
    segment_entry_t* next;
};

struct read_write_entry {
    atomic<int>* lock;
    read_write_entry_t* next;
};

struct transaction {
    log_t* logs;
    segment_entry_t* to_free_entry;
    segment_entry_t* to_alloc_entry;
    read_write_entry_t* read_entry;
    read_write_entry_t* write_entry;
    bool is_ro;
    int start_timestamp;
};

struct segment {
    /* First bit: write bit, remaining bits: timestamp*/
    atomic<int> lock;
    void* start;
    size_t size;
};

struct region {
    void* start;
    atomic<int> timestamp;
    segment_entry_t* segment_entry;
    pthread_mutex_t segment_lock;
    size_t size;
    size_t align;
};

/* ================================================================
                        Helper functions
   ================================================================ */

static inline
segment_t *find_target_segment(region_t *region, transaction_t *trans, const void *pos) {
    segment_entry_t* segment_entry = region->segment_entry;
    while (segment_entry != NULL) {
        if (pos >= segment_entry->segment->start
                && pos < (char*)segment_entry->segment->start + segment_entry->segment->size) {
            return segment_entry->segment;
        }
        segment_entry = segment_entry->next;
    }

    segment_entry = trans->to_alloc_entry;
    while (segment_entry != NULL) {
        if (pos >= segment_entry->segment->start
                && pos < (char*)segment_entry->segment->start + segment_entry->segment->size) {
            return segment_entry->segment;
        }
        segment_entry = segment_entry->next;
    }

    return NULL;
}


static inline
bool has_written(transaction_t* trans, atomic<int>* lock) {
    read_write_entry_t* write_entry = trans->write_entry;
    while (write_entry != NULL) {
        if (write_entry->lock == lock) {
            return true;
        }
        write_entry = write_entry->next;
    }
    return false;
}

static inline
bool validate_read(transaction_t* trans, atomic<int>* lock) {

    if (lock!=NULL && has_written(trans, lock)==true) return true;

    bool in_read = false;
    int start_timestamp = trans->start_timestamp;
    read_write_entry_t* read_entry = trans->read_entry;
    int lock_value, cur_timestamp;
    while (read_entry != NULL) {
        if (lock == read_entry->lock)
            in_read = true;
        lock_value = atomic_load(read_entry->lock);
        cur_timestamp = lock_value >> RESERVED_BIT;

        if ((!has_written(trans, read_entry->lock) && lock_value & WRITE_BIT) 
            || cur_timestamp > start_timestamp)
            return false;
        read_entry = read_entry->next;
    }

    if (lock != NULL && in_read == false) {
        lock_value = atomic_load(lock);
        cur_timestamp = lock_value >> RESERVED_BIT;

        if ( (!has_written(trans, lock) && lock_value & WRITE_BIT) 
            ||cur_timestamp > start_timestamp)
            return false;

        read_write_entry_t* read_entry = (read_write_entry_t*) malloc(sizeof(read_write_entry_t));
        if (unlikely(read_entry == NULL)) {
            return false;
        }
        read_entry->lock = lock;
        read_entry->next = trans->read_entry;
        trans->read_entry = read_entry;
    }

    return true;
}

static inline
bool acquire_write_lock(transaction_t* trans, segment_t* target_segment) {
    if (has_written(trans, &target_segment->lock) == false) {
restart:
        
        int lock_value = atomic_load(&target_segment->lock);
        int cur_timestamp = lock_value >> RESERVED_BIT;

        if (lock_value & WRITE_BIT ||
                cur_timestamp > trans->start_timestamp) return false;

        int target_lock_value = lock_value | WRITE_BIT;
        if (unlikely(atomic_compare_exchange_strong(&target_segment->lock, &lock_value, target_lock_value) == false))
            goto restart;


        read_write_entry_t* write_entry = (read_write_entry_t*) malloc(sizeof(read_write_entry_t));
        if (unlikely(write_entry == NULL)) {
            return false;
        }
        write_entry->lock = &target_segment->lock;
        write_entry->next = trans->write_entry;
        trans->write_entry = write_entry;
    }
    return true;
}

static inline
void clean_read_set(transaction_t* trans) {
    read_write_entry_t* read_entry = trans->read_entry;
    read_write_entry_t* tmp;
    while (read_entry != NULL) {
        tmp = read_entry->next;
        free(read_entry);
        read_entry = tmp;
    }
}

static inline
void clean_write_set(transaction_t* trans) {
    read_write_entry_t* write_entry = trans->write_entry;
    read_write_entry_t* tmp;
    while (write_entry != NULL) {
        int lock_value = atomic_load(write_entry->lock);
        int target_lock_value = lock_value & CLEAR_MAST;
        atomic_compare_exchange_strong(write_entry->lock, &lock_value, target_lock_value);
        tmp = write_entry->next;
        free(write_entry);
        write_entry = tmp;
    }
}

static inline
void update_write_set(transaction_t* trans, int expected_timestamp) {
    read_write_entry_t* write_entry = trans->write_entry;
    read_write_entry_t* tmp;
    int target_lock_value = expected_timestamp << RESERVED_BIT;
    while (write_entry != NULL) {
        atomic_store(write_entry->lock, target_lock_value);
        tmp = write_entry->next;
        free(write_entry);
        write_entry = tmp;
    }
}

static inline
void clean_segment_entry(segment_entry_t* head) {
    segment_entry_t* segment_entry = head;
    segment_entry_t* tmp;
    while (segment_entry != NULL) {
        tmp = segment_entry -> next;
        free(segment_entry);
        segment_entry = tmp;
    }
}


static inline
void alloc_segments(region_t *region, segment_entry_t* to_alloc_entry) {
    if (to_alloc_entry==NULL) return;

    segment_entry_t* last_entry = to_alloc_entry;
    while (last_entry->next != NULL) {
        last_entry = last_entry->next;
    }
    pthread_mutex_lock(&region->segment_lock);
    last_entry->next = region -> segment_entry;
    region -> segment_entry = to_alloc_entry;
    pthread_mutex_unlock(&region->segment_lock);
}

static inline
void free_segments(region_t *region, segment_entry_t* to_free_entry) {
    if (to_free_entry== NULL) return;
    
    pthread_mutex_lock(&region->segment_lock);
    segment_entry_t* tmp;
    segment_entry_t* segment_entry = region -> segment_entry;
    segment_entry_t* previous = NULL;
    while (to_free_entry != NULL) {
        previous = NULL;
        while (segment_entry != NULL) {
            if (segment_entry->segment == to_free_entry->segment) {
                if (previous == NULL) {
                    region -> segment_entry = segment_entry -> next;
                }
                else {
                    previous -> next = segment_entry -> next;
                }
                free(segment_entry->segment->start);
                free(segment_entry->segment);
                free(segment_entry);
                break;
            }
            previous = segment_entry;
            segment_entry = segment_entry->next;
        }
        tmp = to_free_entry->next;
        free(to_free_entry);
        to_free_entry = tmp;
    }
    pthread_mutex_unlock(&region->segment_lock);
}

static inline
void rollback(transaction_t* trans) {
    clean_read_set(trans);

    if (trans->is_ro == false) {
        clean_segment_entry(trans->to_free_entry);
        clean_segment_entry(trans->to_alloc_entry);

        log_t* write_log = trans->logs;
        log_t* tmp;
        while (write_log != NULL) {
            memcpy(write_log->location, write_log->old_data, write_log->size);
            free(write_log->old_data);
            tmp = write_log->next;
            free(write_log);
            write_log = tmp;
        }
        clean_write_set(trans);
    }

    free(trans);
}

static inline
bool commit(region_t* region, transaction_t* trans){
    if (validate_read(trans, NULL) == false) {
        rollback(trans);
        return false;
    }

    int expected_timestamp = atomic_fetch_add(&region->timestamp, 1) +1;
    clean_read_set(trans);

    if (trans ->is_ro == false) {
        free_segments(region, trans->to_free_entry);
        alloc_segments(region, trans->to_alloc_entry);
        log_t* write_log = trans->logs;
        log_t* tmp;
        while (write_log != NULL) {
            free(write_log->old_data);
            tmp = write_log->next;
            free(write_log);
            write_log = tmp;
        }
        update_write_set(trans, expected_timestamp);
    }
    free(trans);
    return true;
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

    segment_t* segment = (segment_t*) malloc(sizeof(segment_t));
    if (unlikely(segment == NULL)) {
        free(region);
        return invalid_shared;
    }

    if (unlikely(posix_memalign(&(region->start), align, size) != 0)) {
        free(segment);
        free(region);
        return invalid_shared;
    }

    memset(region->start, 0, size);
    region->timestamp = 0;
    pthread_mutex_init(&region->segment_lock, NULL);
    region->size = size;
    region->align = align;

    segment->lock = 0;
    segment->start = region->start;
    segment->size = size;

    segment_entry_t* segment_entry = (segment_entry_t*) malloc(sizeof(segment_entry_t));
    if (unlikely(segment_entry == NULL)) {
        return invalid_shared;
    }
    segment_entry->segment = segment;
    segment_entry->next = NULL;

    region->segment_entry = segment_entry;

    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    region_t* region = (region_t*) shared;
    segment_entry_t* segment_entry = region->segment_entry;
    segment_entry_t* tmp;
    while (segment_entry != NULL) {
        free(segment_entry->segment->start);
        free(segment_entry->segment);
        tmp = segment_entry->next;
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
    trans->logs = NULL;
    trans->to_free_entry = NULL;
    trans->to_alloc_entry = NULL;
    trans->read_entry = NULL;
    trans->write_entry = NULL;
    trans->is_ro = is_ro;
    trans->start_timestamp = atomic_load(&region->timestamp);
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
    return commit(region, trans);
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
    segment_t* target_segment = find_target_segment(region, trans, source);

    if (target_segment == NULL) {
        rollback(trans);
        return false;
    }

    if (validate_read(trans, &target_segment->lock) == false) {
        rollback(trans);
        return false;
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
    segment_t* target_segment = find_target_segment(region, trans, target);

    if (target_segment == NULL) {
        rollback(trans);
        return false;
    }

    if (acquire_write_lock(trans, target_segment) == false) {
        rollback(trans);
        return false;
    }

    log_t* write_log = (log_t*) malloc(sizeof(log_t));
    if (unlikely(write_log == NULL)) {
        rollback(trans);
        return false;
    }

    write_log->old_data = malloc(size);
    if (unlikely(write_log->old_data == NULL)) {
        free(write_log);
        rollback(trans);
        return false;
    }

    memcpy(write_log->old_data, target, size);
    write_log->size = size;
    write_log->location = target;
    write_log->next = trans->logs;

    trans->logs = write_log;
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

    segment_t* segment = (segment_t*) malloc(sizeof(segment_t));
    if (unlikely(segment == NULL)) {
        return Alloc::nomem;
    }

    if (unlikely(posix_memalign((void**) & (segment->start), region->align, size) != 0)) {
        free(segment);
        return Alloc::nomem;
    }
    memset(segment->start, 0, size);
    segment->size = size;
    segment->lock = 0;
    *target = segment->start;

    segment_entry_t* segment_entry = (segment_entry_t*) malloc(sizeof(segment_entry_t));
    if (unlikely(segment_entry == NULL)) {
        free(segment);
        free(segment->start);
        return Alloc::nomem;
    }

    segment_entry->segment = segment;
    segment_entry->next = trans->to_alloc_entry;
    trans->to_alloc_entry = segment_entry;

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
    segment_t* target_segment = find_target_segment(region, trans, target);

    if (target_segment == NULL) {
        rollback(trans);
        return false;
    }

    segment_entry_t* segment_entry = (segment_entry_t*) malloc(sizeof(segment_entry_t));
    if (unlikely(segment_entry == NULL)) {
        rollback(trans);
        return false;
    }
    segment_entry->segment = target_segment;
    segment_entry->next = trans->to_free_entry;
    trans->to_free_entry = segment_entry;

    return true;

}
