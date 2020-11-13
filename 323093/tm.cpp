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
    segment_entry_t* to_free_head;
    segment_entry_t* to_alloc_head;
    segment_entry_t* to_alloc_tail;
    read_write_entry_t* read_head;
    read_write_entry_t* write_head;
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
    segment_entry_t* segment_head;
    atomic<int> seg_lock;
    size_t size;
    size_t align;
};


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    region_t* region = new (std::nothrow) region_t();
    if (unlikely(region == NULL)) {
        return invalid_shared;
    }

    segment_t* seg = new (std::nothrow) segment_t();
    if (unlikely(seg == NULL)) {
        delete region;
        return invalid_shared;
    }

    if (unlikely(posix_memalign(&(region->start), align, size) != 0)) {
        delete seg;
        delete region;
        return invalid_shared;
    }

    memset(region->start, 0, size);
    region->timestamp = 0;
    region->seg_lock = 0;
    region->size = size;
    region->align = align;

    seg->lock = 0;
    seg->start = region->start;
    seg->size = size;

    segment_entry_t* seg_entry = new (std::nothrow) segment_entry_t();
    if (unlikely(seg_entry == NULL)) {
        return invalid_shared;
    }
    seg_entry->segment = seg;
    seg_entry->next = NULL;

    region->segment_head = seg_entry;

    return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared ) noexcept {
    region_t* region = (region_t*) shared;
    segment_entry_t* segment_entry = region->segment_head;
    segment_entry_t* tmp;
    while (segment_entry != NULL) {
        free(segment_entry->segment->start);
        delete segment_entry->segment;
        tmp = segment_entry->next;
        delete segment_entry;
        segment_entry = tmp;
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

/* ================================================================
                        Helper functions
   ================================================================ */

static inline
segment_t *find_target_seg(region_t *region, transaction_t *trans, const void *pos) {
    segment_entry_t* segment_entry = region->segment_head;
    while (segment_entry != NULL) {
        if (pos >= segment_entry->segment->start
                && pos < (char*)segment_entry->segment->start + segment_entry->segment->size) {
            return segment_entry->segment;
        }
        segment_entry = segment_entry->next;
    }

    segment_entry = trans->to_alloc_head;
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
void alloc_segments(region_t *region, segment_entry_t* to_alloc_head, segment_entry_t* to_alloc_tail) {
    if (to_alloc_tail != NULL) {
        to_alloc_tail->next = region -> segment_head;
        region -> segment_head = to_alloc_head;
    }
}

static inline
void free_segments(region_t *region, segment_entry_t* to_free_head) {
    int expected_lock = 0;
    while (atomic_compare_exchange_strong(&region->seg_lock, &expected_lock, 1) == false)
        expected_lock = 0;

    segment_entry_t* iterator = to_free_head;
    segment_entry_t* tmp;
    segment_entry_t* segment = region -> segment_head;
    segment_entry_t* previous = NULL;
    while (iterator != NULL) {
        previous = NULL;
        while (segment != NULL) {
            if (segment->segment == iterator->segment) {
                if (previous == NULL) {
                    region -> segment_head = segment -> next;
                }
                else {
                    previous -> next = segment -> next;
                }
                delete segment;
                break;
            }
            previous = segment;
            segment = segment->next;
        }
        tmp = iterator->next;
        delete iterator;
        iterator = tmp;
    }

    expected_lock = 1;
    atomic_compare_exchange_strong(&region->seg_lock, &expected_lock, 0);
}

/* ================================================================
                    Validation and commit
   ================================================================ */

static inline
bool validate_read(transaction_t * trans) {
    int start_timestamp = trans->start_timestamp;
    read_write_entry_t* read_entry = trans->read_head;
    int lock_value, cur_timestamp;
    while (read_entry != NULL) {
        lock_value = atomic_load(read_entry->lock);
        cur_timestamp = lock_value >> RESERVED_BIT;

        if (lock_value & WRITE_BIT || cur_timestamp > start_timestamp)
            return false;
        read_entry = read_entry->next;
    }
    return true;
}

static inline
int update_global_timestamp(region_t* region) {
restart:
    int cur_timestamp = atomic_load(&region->timestamp);
    int expected_timestamp = cur_timestamp + 1;
    if (unlikely(atomic_compare_exchange_strong(&region->timestamp, &cur_timestamp, expected_timestamp) == false))
        goto restart;
    return expected_timestamp;
}

static inline
void update_write_set(transaction_t* trans, int expected_timestamp) {
    read_write_entry_t* write_entry = trans->write_head;
    read_write_entry_t* tmp;
    int target_lock_value = expected_timestamp << RESERVED_BIT;
    while (write_entry != NULL) {
        int lock_value = atomic_load(write_entry->lock);
        atomic_compare_exchange_strong(write_entry->lock, &lock_value, target_lock_value);
        tmp = write_entry->next;
        delete write_entry;
        write_entry = tmp;
    }
}

/* ================================================================
              Functions used to rollback or commit
   ================================================================ */

static inline
void free_read_set(transaction_t* trans) {
    read_write_entry_t* read_entry = trans->read_head;
    read_write_entry_t* tmp;
    while (read_entry != NULL) {
        tmp = read_entry->next;
        delete read_entry;
        read_entry = tmp;
    }
}

static inline
void free_write_set(transaction_t* trans) {
    read_write_entry_t* write_entry = trans->write_head;
    read_write_entry_t* tmp;
    while (write_entry != NULL) {
        int lock_value = atomic_load(write_entry->lock);
        int target_lock_value = lock_value & CLEAR_MAST;
        atomic_compare_exchange_strong(write_entry->lock, &lock_value, target_lock_value);
        tmp = write_entry->next;
        delete write_entry;
        write_entry = tmp;
    }
}

static inline
void free_segment_entry(segment_entry_t* head) {
    segment_entry_t* segment_entry = head;
    segment_entry_t* tmp;
    while (segment_entry != NULL) {
        tmp = segment_entry -> next;
        delete segment_entry;
        segment_entry = tmp;
    }
}

static inline
void free_write_log_and_write_back(log_t* write_log, bool write) {
    log_t* tmp;
    while (write_log != NULL) {
        if (write)
            memcpy(write_log->location, write_log->old_data, write_log->size);
        free(write_log->old_data);
        tmp = write_log->next;
        delete write_log;
        write_log = tmp;
    }
}

static inline
void rollback(transaction_t* trans) {
    free_read_set(trans);

    if (trans->is_ro == false) {
        free_write_set(trans);
        free_segment_entry(trans->to_free_head);
        free_segment_entry(trans->to_alloc_head);
        free_write_log_and_write_back(trans->logs, true);
    }

    delete trans;
}

/* ================================================================
                Functions used by tm_read and tm_write
   ================================================================ */

static inline
read_write_entry_t* has_read_then_remove(transaction_t* trans, atomic<int>* lock) {
    read_write_entry_t* read_entry = trans->read_head;
    read_write_entry_t* previous = NULL;
    while (read_entry != NULL) {
        if (read_entry->lock == lock) {
            if (previous == NULL) {
                trans->read_head = read_entry->next;
            }
            else {
                previous->next = read_entry->next;
            }
            return read_entry;
        }
        previous = read_entry;
        read_entry = read_entry->next;
    }
    return NULL;
}

static inline
bool has_written(transaction_t* trans, atomic<int>* lock) {
    read_write_entry_t* write_entry = trans->write_head;
    while (write_entry != NULL) {
        if (write_entry->lock == lock) {
            return true;
        }
        write_entry = write_entry->next;
    }
    return false;
}


static inline
bool add_new_read(transaction_t * trans, segment_t * seg) {
    atomic<int>* lock = &seg->lock;

    read_write_entry_t* read_entry = has_read_then_remove(trans, lock);

    if (read_entry == NULL) {
        int lock_value = atomic_load(lock);
        int cur_timestamp = lock_value >> RESERVED_BIT;
        int start_timestamp = trans->start_timestamp;

        if (lock_value & WRITE_BIT || cur_timestamp > start_timestamp)
            return false;

        read_entry = new (std::nothrow) read_write_entry_t();
        if (unlikely(read_entry == NULL)) {
            return false;
        }
    }

    read_entry->lock = lock;
    read_entry->next = trans->read_head;
    trans->read_head = read_entry;
    return true;
}


static inline
bool add_new_write(transaction_t* trans, segment_t* target_seg) {
    if (has_written(trans, &target_seg->lock) == false) {
restart:
        read_write_entry_t* read_entry = has_read_then_remove(trans, &target_seg->lock);
        if (read_entry != NULL) delete read_entry;

        int lock_value = atomic_load(&target_seg->lock);
        int cur_timestamp = lock_value >> RESERVED_BIT;

        if (lock_value & WRITE_BIT ||
                cur_timestamp > trans->start_timestamp) return false;

        int target_lock_value = lock_value | WRITE_BIT;
        if (unlikely(atomic_compare_exchange_strong(&target_seg->lock, &lock_value, target_lock_value) == false))
            goto restart;


        read_write_entry_t* write_entry = new (std::nothrow) read_write_entry_t();
        if (unlikely(write_entry == NULL)) {
            return false;
        }
        write_entry->lock = &target_seg->lock;
        write_entry->next = trans->write_head;
        trans->write_head = write_entry;
    }
    return true;
}


/* ================================================================
                       End of helper functions
   ================================================================ */

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
    region_t *region = (region_t*) shared;
    transaction_t* trans = new (std::nothrow) transaction_t();
    if (unlikely(trans == NULL)) {
        return invalid_tx;
    }
    trans->logs = NULL;
    trans->to_free_head = NULL;
    trans->to_alloc_head = NULL;
    trans->to_alloc_tail = NULL;
    trans->read_head = NULL;
    trans->write_head = NULL;
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

    if (validate_read(trans) == false) {
        rollback(trans);
        return false;
    }

    int expected_timestamp = update_global_timestamp(region);

    free_read_set(trans);
    if (trans ->is_ro == false) {
        update_write_set(trans, expected_timestamp);
        free_segments(region, trans->to_free_head);
        alloc_segments(region, trans->to_alloc_head, trans->to_alloc_tail);
        free_write_log_and_write_back(trans->logs, false);
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

    if (target_seg == NULL ||
            validate_read(trans) == false ||
            (has_written(trans, &target_seg->lock) == false
             && add_new_read(trans, target_seg) == false)) {
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
    segment_t* target_seg = find_target_seg(region, trans, target);

    if (target_seg == NULL ||
            add_new_write(trans, target_seg) == false) {
        rollback(trans);
        return false;
    }

    log_t* write_log = new (std::nothrow) log_t();
    if (unlikely(write_log == NULL)) {
        rollback(trans);
        return false;
    }

    write_log->old_data = malloc(size);
    if (unlikely(write_log->old_data == NULL)) {
        delete write_log;
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

    segment_t* seg = new (std::nothrow) segment_t();
    if (unlikely(seg == NULL)) {
        return Alloc::nomem;
    }

    if (unlikely(posix_memalign(&seg->start, region->align, size) != 0)) {
        delete seg;
        return Alloc::nomem;
    }
    memset(seg->start, 0, size);
    seg->size = size;
    seg->lock = 0;
    *target = seg->start;

    segment_entry_t* seg_entry = new (std::nothrow) segment_entry_t();
    if (unlikely(seg_entry == NULL)) {
        delete seg;
        free(seg->start);
        return Alloc::nomem;
    }

    seg_entry->segment = seg;
    seg_entry->next = trans->to_alloc_head;
    trans->to_alloc_head = seg_entry;

    if (trans->to_alloc_tail == NULL) {
        trans->to_alloc_tail = seg_entry;
    }


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

    if (target_seg == NULL) {
        rollback(trans);
        return false;
    }

    segment_entry_t* seg_entry = new (std::nothrow) segment_entry_t();
    if (unlikely(seg_entry == NULL)) {
        rollback(trans);
        return false;
    }
    seg_entry->segment = target_seg;
    seg_entry->next = trans->to_free_head;
    trans->to_free_head = seg_entry;

    return true;

}
