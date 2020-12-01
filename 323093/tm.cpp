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
#define _POSIX_C_SOURCE 200809 L
#ifdef __STDC_NO_ATOMICS__
#error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <vector>
// Internal headers
#include <iostream>
#include <tm.hpp>

using namespace std;

// -------------------------------------------------------------------------- //

/** Define a proposition as likely true.
 * @param prop Proposition
 **/
#undef likely
#ifdef __GNUC__
#define likely(prop) __builtin_expect((prop) ? 1 : 0, 1)
#else
#define likely(prop) (prop)
#endif

/** Define a proposition as likely false.
 * @param prop Proposition
 **/
#undef unlikely
#ifdef __GNUC__
#define unlikely(prop) __builtin_expect((prop) ? 1 : 0, 0)
#else
#define unlikely(prop) (prop)
#endif

/** Define one or several attributes.
 * @param type... Attribute names
 **/
#undef as
#ifdef __GNUC__
#define as(type...) __attribute__((type))
#else
#define as(type...)
#warning This compiler has no support
for
  GCC attributes
#endif

// -------------------------------------------------------------------------- //

/** Define the structure of the lock value
 *  2 reserved bits
 *  1st bit is written bit
 *  2nd bit is which one is the currrent valid block
 **/
#define RESERVE_BITS 2
#define WRITE_BIT 1
#define VALID_SHIFT 1
#define VALID_BIT 2
#define CLEAR_MASK (~1)
/** Define the structure of the data block
 *  The last block is reserved for temporary writing
 *  The remaining blocks are used both for current valid block and archieve.
 **/
#define VERSION_NUM 3
#define WRITE_BLOCK_INDEX 2

/** Define the maximal value of timestamp **/
#define MAX_TIMESTAMP 1 << 30

#define BLOCK_SHIFT 5
#define BLOCK_SIZE 32

typedef struct segment_entry segment_entry_t;
typedef struct read_entry read_entry_t;
typedef struct write_entry write_entry_t;
typedef struct transaction transaction_t;
typedef struct segment segment_t;
typedef struct region region_t;

struct segment_entry {
  segment_t *segment;
  segment_entry_t *next;
};

struct read_entry {
  atomic<int> *lock;
  segment_t *segment;
  int read_timestamp;
  short block_index;
  read_entry_t *next;
};

struct write_entry {
  atomic<int> *lock;
  segment_t *segment;
  short block_index;
  write_entry_t *next;
};

struct transaction {
  read_entry_t *read_entry;
  write_entry_t *write_entry;
  segment_entry_t *to_free_entry;
  segment_entry_t *to_alloc_entry;
  bool is_ro;
  int start_timestamp;
};

struct segment {
  void *start;
  atomic<int> *lock;
  void *end;
  size_t size;
  size_t *block_size;
  int *archieve_timestamp;
};

struct region {
  segment_entry_t *segment_entry;
  segment_entry_t *freed_segment_entry;
  atomic<int> segment_lock;
  atomic<int> timestamp;
  void *start;
  size_t size;
  size_t align;
};

/* ================================================================
                        Helper functions
   ================================================================ */
static inline segment_entry_t *alloc_segment(size_t size, size_t align) {
  segment_t *segment = (segment_t *)malloc(sizeof(segment_t));
  if (unlikely(segment == NULL)) {
    return NULL;
  }
  if (unlikely(posix_memalign(&(segment->start), align, size * VERSION_NUM) !=
               0)) {
    free(segment);
    return NULL;
  }
  int block_num = (size >> BLOCK_SHIFT) + 1;
  segment->lock = (atomic<int> *)malloc(sizeof(atomic<int>) * block_num);
  if (unlikely(segment->lock == NULL)) {
    free(segment->start);
    free(segment);
    return NULL;
  }
  segment->block_size = (size_t *)malloc(sizeof(size_t) * block_num);
  if (unlikely(segment->block_size == NULL)) {
    free(segment->start);
    free(segment->lock);
    free(segment);
    return NULL;
  }
  segment->archieve_timestamp = (int *)malloc(sizeof(int) * block_num);
  if (unlikely(segment->archieve_timestamp == NULL)) {
    free(segment->start);
    free(segment->lock);
    free(segment->block_size);
    free(segment);
    return NULL;
  }

  memset(segment->start, 0, size * VERSION_NUM);
  segment->end = (char *)segment->start + size;
  segment->size = size;
  int tmp_size = size;
  for (int i = 0; i < block_num; ++i) {
    segment->lock[i] = 0;
    segment->block_size[i] = tmp_size < BLOCK_SIZE ? tmp_size : BLOCK_SIZE;
    tmp_size -= segment->block_size[i];
    segment->archieve_timestamp[i] = MAX_TIMESTAMP;
  }

  segment_entry_t *segment_entry =
      (segment_entry_t *)malloc(sizeof(segment_entry_t));
  if (unlikely(segment_entry == NULL)) {
    free(segment->start);
    free(segment->lock);
    free(segment->block_size);
    free(segment->archieve_timestamp);
    free(segment);
    return NULL;
  }
  segment_entry->segment = segment;
  segment_entry->next = NULL;
  return segment_entry;
}

static inline segment_t *
find_target_segment(region_t *region, transaction_t *trans, const void *pos) {
  segment_entry_t *segment_entry;
  segment_entry = region->segment_entry;
  while (segment_entry != NULL) {
    if (pos >= segment_entry->segment->start &&
        pos < segment_entry->segment->end) {
      return segment_entry->segment;
    }
    segment_entry = segment_entry->next;
  }
  segment_entry = trans->to_alloc_entry;
  while (segment_entry != NULL) {
    if (pos >= segment_entry->segment->start &&
        pos < segment_entry->segment->end) {
      return segment_entry->segment;
    }
    segment_entry = segment_entry->next;
  }
  return NULL;
}

static inline read_entry_t *has_read(transaction_t *trans, atomic<int> *lock) {
  read_entry_t *read_entry = trans->read_entry;
  while (read_entry != NULL) {
    if (read_entry->lock == lock)
      return read_entry;
    read_entry = read_entry->next;
  }
  return NULL;
}

static inline write_entry_t *has_written(transaction_t *trans,
                                         atomic<int> *lock) {
  write_entry_t *write_entry = trans->write_entry;
  while (write_entry != NULL) {
    if (write_entry->lock == lock) {
      return write_entry;
    }
    write_entry = write_entry->next;
  }
  return NULL;
}

/* ================================================================
                       Rollback and commit
   ================================================================ */

static inline void clean_read_set(transaction_t *trans) {
  read_entry_t *read_entry = trans->read_entry;
  read_entry_t *read_entry_tmp;
  while (read_entry != NULL) {
    read_entry_tmp = read_entry->next;
    free(read_entry);
    read_entry = read_entry_tmp;
  }
}

static inline void rollback(transaction_t *trans) {
  if (trans->is_ro) {
    free(trans);
  } else {
    clean_read_set(trans);
    write_entry_t *write_entry = trans->write_entry;
    write_entry_t *write_entry_tmp;
    while (write_entry != NULL) {
      int lock_value = atomic_load(write_entry->lock);
      int target_lock_value = lock_value & CLEAR_MASK;
      atomic_compare_exchange_strong(write_entry->lock, &lock_value,
                                     target_lock_value);
      write_entry_tmp = write_entry->next;
      free(write_entry);
      write_entry = write_entry_tmp;
    }

    segment_entry_t *segment_entry = trans->to_free_entry;
    segment_entry_t *segment_entry_tmp;
    while (segment_entry != NULL) {
      segment_entry_tmp = segment_entry->next;
      free(segment_entry);
      segment_entry = segment_entry_tmp;
    }

    segment_entry = trans->to_alloc_entry;
    while (segment_entry != NULL) {
      free(segment_entry->segment->start);
      free(segment_entry->segment->lock);
      free(segment_entry->segment->block_size);
      free(segment_entry->segment->archieve_timestamp);
      free(segment_entry->segment);
      segment_entry_tmp = segment_entry->next;
      free(segment_entry);
      segment_entry = segment_entry_tmp;
    }
    free(trans);
  }
}

static inline bool commit(region_t *region, transaction_t *trans) {
  if (trans->is_ro) {
    free(trans);
    return true;
  } else {
    read_entry_t *read_entry = trans->read_entry;
    int lock_value, cur_timestamp;
    while (read_entry != NULL) {
      lock_value = atomic_load(read_entry->lock);
      cur_timestamp = lock_value >> RESERVE_BITS;
      if ((has_written(trans, read_entry->lock) == NULL &&
           lock_value & WRITE_BIT) ||
          cur_timestamp != read_entry->read_timestamp) {
        rollback(trans);
        return false;
      }
      read_entry = read_entry->next;
    }

    int expected_timestamp = atomic_fetch_add(&region->timestamp, 1) + 1;
    clean_read_set(trans);
    write_entry_t *write_entry = trans->write_entry;
    write_entry_t *write_entry_tmp;
    int target_lock_value = expected_timestamp << RESERVE_BITS;
    while (write_entry != NULL) {
      int lock_value = atomic_load(write_entry->lock);
      int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;
      write_entry->segment->archieve_timestamp[write_entry->block_index] =
          lock_value >> RESERVE_BITS;
      memcpy((char *)write_entry->segment->start +
                 (1 - valid_block) * write_entry->segment->size +
                 write_entry->block_index * BLOCK_SIZE,
             (char *)write_entry->segment->start +
                 WRITE_BLOCK_INDEX * write_entry->segment->size +
                 write_entry->block_index * BLOCK_SIZE,
             write_entry->segment->block_size[write_entry->block_index]);
      atomic_store(write_entry->lock,
                   target_lock_value | ((1 - valid_block) << VALID_SHIFT));
      write_entry_tmp = write_entry->next;
      free(write_entry);
      write_entry = write_entry_tmp;
    }

    segment_entry_t *to_free_entry = trans->to_free_entry;
    segment_entry_t *to_alloc_entry = trans->to_alloc_entry;
    if (to_free_entry != NULL) {
      int expected_lock_value = 0;
      while (unlikely(atomic_compare_exchange_strong(&region->segment_lock,
                                                     &expected_lock_value,
                                                     1) == false))
        expected_lock_value = 0;
      segment_entry_t *tmp;
      segment_entry_t *segment_entry = region->segment_entry;
      segment_entry_t *previous = NULL;
      while (to_free_entry != NULL) {
        previous = NULL;
        while (segment_entry != NULL) {
          if (segment_entry->segment == to_free_entry->segment) {
            if (previous == NULL) {
              region->segment_entry = segment_entry->next;
            } else {
              previous->next = segment_entry->next;
            }
            segment_entry->next = region->freed_segment_entry;
            region->freed_segment_entry = segment_entry;
            break;
          }
          previous = segment_entry;
          segment_entry = segment_entry->next;
        }
        tmp = to_free_entry->next;
        free(to_free_entry);
        to_free_entry = tmp;
      }
      atomic_store(&region->segment_lock, 0);
    }

    if (to_alloc_entry != NULL) {
      while (to_alloc_entry->next != NULL) {
        to_alloc_entry = to_alloc_entry->next;
      }
      int expected_lock_value = 0;
      while (unlikely(atomic_compare_exchange_strong(&region->segment_lock,
                                                     &expected_lock_value,
                                                     1) == false))
        expected_lock_value = 0;
      to_alloc_entry->next = region->segment_entry;
      region->segment_entry = trans->to_alloc_entry;
      atomic_store(&region->segment_lock, 0);
    }
    free(trans);
    return true;
  }
}

/* ================================================================
                       End of helper functions
   ================================================================ */

/** Create (i.e. allocate + init) a new shared memory region, with one first
 *non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in
 *bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared
 *memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t size, size_t align) noexcept {
  segment_entry_t *segment_entry = alloc_segment(size, align);
  if (unlikely(segment_entry == NULL))
    return invalid_shared;

  region_t *region = (region_t *)malloc(sizeof(region_t));
  if (unlikely(region == NULL)) {
    return invalid_shared;
  }
  region->segment_entry = segment_entry;
  region->freed_segment_entry = NULL;
  region->segment_lock = 0;
  region->timestamp = 0;
  region->start = segment_entry->segment->start;
  region->size = size;
  region->align = align;
  return region;
}
/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t shared) noexcept {
  region_t *region = (region_t *)shared;
  segment_entry_t *segment_entry = region->segment_entry;
  segment_entry_t *tmp;
  while (segment_entry != NULL) {
    free(segment_entry->segment->start);
    free(segment_entry->segment->lock);
    free(segment_entry->segment->block_size);
    free(segment_entry->segment->archieve_timestamp);
    free(segment_entry->segment);
    tmp = segment_entry->next;
    free(segment_entry);
    segment_entry = tmp;
  }
  segment_entry = region->freed_segment_entry;
  while (segment_entry != NULL) {
    free(segment_entry->segment->start);
    free(segment_entry->segment->lock);
    free(segment_entry->segment->block_size);
    free(segment_entry->segment->archieve_timestamp);
    free(segment_entry->segment);
    tmp = segment_entry->next;
    free(segment_entry);
    segment_entry = tmp;
  }
  free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the
 *shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t shared) noexcept { return ((region_t *)shared)->start; }

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared) noexcept { return ((region_t *)shared)->size; }

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the
 *given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared) noexcept {
  return ((region_t *)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)malloc(sizeof(transaction_t));
  if (unlikely(trans == NULL)) {
    return invalid_tx;
  }
  trans->to_free_entry = NULL;
  trans->to_alloc_entry = NULL;
  trans->read_entry = NULL;
  trans->write_entry = NULL;
  trans->is_ro = is_ro;
  trans->start_timestamp = atomic_load(&region->timestamp);
  return (tx_t)trans;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;
  return commit(region, trans);
}

/** [thread-safe] Read operation in the given transaction, source in the shared
 *region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
 **/
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size,
             void *target) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;
  segment_t *target_segment = find_target_segment(region, trans, source);

  if (unlikely(target_segment == NULL)) {
    rollback(trans);
    return false;
  }

  short block_index =
      ((char *)source - (char *)target_segment->start) >> BLOCK_SHIFT;
  atomic<int> *lock = &target_segment->lock[block_index];
  int lock_value = atomic_load(lock);
  int cur_timestamp = lock_value >> RESERVE_BITS;
  int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;

  if (trans->is_ro) {

    if (cur_timestamp > trans->start_timestamp || lock_value & WRITE_BIT) {
      if (likely(cur_timestamp > trans->start_timestamp &&
                 target_segment->archieve_timestamp[block_index] <=
                     trans->start_timestamp)) {
        memcpy(target,
               (char *)source + (1 - valid_block) * target_segment->size, size);
        return true;
      }
      rollback(trans);
      return false;
    }
    memcpy(target, (char *)source + valid_block * target_segment->size, size);
    return true;

  } else {

    write_entry_t *write_entry = has_written(trans, lock);
    if (write_entry != NULL) {
      memcpy(target, (char *)source + WRITE_BLOCK_INDEX * target_segment->size,
             size);
      return true;
    } else {

      read_entry_t *read_entry = has_read(trans, lock);
      if (read_entry == NULL) {
        if (lock_value & WRITE_BIT) {
          rollback(trans);
          return false;
        }
        read_entry_t *read_entry = (read_entry_t *)malloc(sizeof(read_entry_t));
        if (unlikely(read_entry == NULL)) {
          rollback(trans);
          return false;
        }
        read_entry->lock = lock;
        read_entry->read_timestamp = cur_timestamp;
        read_entry->block_index = block_index;
        read_entry->next = trans->read_entry;
        trans->read_entry = read_entry;
      }
      memcpy(target, (char *)source + valid_block * target_segment->size, size);
      return true;
    }
  }
}

/** [thread-safe] Write operation in the given transaction, source in a private
 *region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the
 *alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
 **/
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size,
              void *target) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;
  segment_t *target_segment = find_target_segment(region, trans, target);
  if (unlikely(target_segment == NULL)) {
    rollback(trans);
    return false;
  }

  short block_index =
      ((char *)target - (char *)target_segment->start) >> BLOCK_SHIFT;
restart:
  atomic<int> *lock = &target_segment->lock[block_index];
  int lock_value = atomic_load(lock);
  int cur_timestamp = lock_value >> RESERVE_BITS;
  int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;

  write_entry_t *write_entry = has_written(trans, lock);

  if (write_entry != NULL) {
    memcpy((char *)target + WRITE_BLOCK_INDEX * target_segment->size, source,
           size);
    return true;
  } else {

    read_entry_t *read_entry = has_read(trans, lock);
    if (lock_value & WRITE_BIT ||
        (read_entry != NULL && cur_timestamp != read_entry->read_timestamp)) {
      rollback(trans);
      return false;
    }

    int target_lock_value = lock_value | WRITE_BIT;
    if (unlikely(atomic_compare_exchange_strong(lock, &lock_value,
                                                target_lock_value) == false))
      goto restart;

    write_entry = (write_entry_t *)malloc(sizeof(write_entry_t));
    if (unlikely(write_entry == NULL)) {
      rollback(trans);
      return false;
    }
    write_entry->lock = lock;
    write_entry->segment = target_segment;
    write_entry->block_index = block_index;
    memcpy((char *)target_segment->start +
               WRITE_BLOCK_INDEX * target_segment->size +
               block_index * BLOCK_SIZE,
           (char *)target_segment->start + valid_block * target_segment->size +
               block_index * BLOCK_SIZE,
           target_segment->block_size[block_index]);
    write_entry->next = trans->write_entry;
    trans->write_entry = write_entry;

    memcpy((char *)target + WRITE_BLOCK_INDEX * target_segment->size, source,
           size);
    return true;
  }
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive
 *multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first
 *byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not
 *(abort_alloc)
 **/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size,
               void **target) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;

  segment_entry_t *segment_entry = alloc_segment(size, region->align);
  if (unlikely(segment_entry == NULL)) {
    return Alloc::nomem;
  }
  *target = segment_entry->segment->start;
  segment_entry->next = trans->to_alloc_entry;
  trans->to_alloc_entry = segment_entry;
  return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t shared, tx_t tx, void *target) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;
  segment_t *target_segment = find_target_segment(region, trans, target);

  if (unlikely(target_segment == NULL)) {
    rollback(trans);
    return false;
  }

  segment_entry_t *segment_entry =
      (segment_entry_t *)malloc(sizeof(segment_entry_t));
  if (unlikely(segment_entry == NULL)) {
    rollback(trans);
    return false;
  }
  segment_entry->segment = target_segment;
  segment_entry->next = trans->to_free_entry;
  trans->to_free_entry = segment_entry;
  return true;
}