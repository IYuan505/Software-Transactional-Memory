/**
 * @file   tm.cpp
 * @author LIANG Qiyuan <qiyuan.liang@epfl.ch>
 *
 * @section DESCRIPTION
 * This is a  ************************************
 *              3-versioned transactional memory
 *            ************************************
 * implemention, with 1 current version,
 * 1 archieve version and 1 writable version.
 **/

// Requested features
#define _POSIX_C_SOURCE 200809L
#ifdef __STDC_NO_ATOMICS__
#error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <atomic>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
// Internal headers
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
#warning This compiler has no support for GCC attributes
#endif

// -------------------------------------------------------------------------- //

/** Define the structure of the lock value
 *  4 reserved bits
 *  1st bit is written bit, telling whether this lock is written now.
 *  2nd bit is writing archieve bit, telling whether the archieve is being
 *  written now.
 *  3rd and 4th bit tell which one is the currrent valid block
 **/
#define RESERVE_BITS 4
#define WRITE_BIT 1
#define CLEAR_WRITE_MASK (~1)
#define ARCHIEVE_BIT 2
#define VALID_SHIFT 2
#define VALID_BIT 12

/** Define the structure of the data block:
 *  take a circular step, current one is valid block,
 *  the (current + 1) is the writable block, the (curretn + 2)
 *  is the archieve block.
 **/
#define VERSION_NUM 3

/** Define the maximal value of timestamp **/
#define MAX_TIMESTAMP 1 << 30

/** Define the minimal level of parallelism
 *  Each 8 bytes has a lock to control the access
 **/
#define BLOCK_SHIFT 3
#define BLOCK_SIZE 8

/** Define the default read write entry size
 **/
#define DEFAULT_READ_SIZE 16
#define DEFAULT_WRITE_SIZE 4

/** Define the name of the used struct **/
typedef struct segment_entry segment_entry_t;
typedef struct read_entry read_entry_t;
typedef struct write_entry write_entry_t;
typedef struct transaction transaction_t;
typedef struct segment segment_t;
typedef struct region region_t;

struct segment_entry { /* segment entry, a linked list wrapper of the segment */
  segment_t *segment;  /* pointer to the address of contained segment */
  segment_entry_t *next; /* pointer to the next segment_entry_t */
};

struct read_entry {   /* Read set entry, used by read-write transactions only */
  atomic<int> *lock;  /* address of the lock, fast access */
  int read_timestamp; /* timestamp at the time of reading */
};

struct write_entry {  /* Write set entry */
  atomic<int> *lock;  /* address of the lock, fast access */
  segment_t *segment; /* segment of writing */
  short block_index;  /* index of the reading block (8 bytes) inside the
                         segment */
};

struct transaction {          /* Transaction */
  read_entry_t *read_entry;   /* read set of the transaction, an array
                                 (Used only by read-write transactions) */
  write_entry_t *write_entry; /* write set of the transaction, an array */
  segment_t
      *last_accessed_segment;     /* last accessed segment, improve locality. */
  segment_entry_t *to_free_entry; /* segment to free, a linked list. Delay
                                     the free until the commit time */
  segment_entry_t *to_alloc_entry; /* segment to alloc, a linked lsit. Delay
                                      the alloc until the commit time */
  short read_cnt;                  /* number of read entries */
  short read_capacity;             /* size of the read_entry array */
  short write_cnt;                 /* number of write entries */
  short write_capacity;            /* size of the write_entry array */
  bool is_ro;                      /* is read-only or not */
  int start_timestamp; /* timestamp of the transaction when it starts */
};

struct segment { /* Segment, create by tm_create and tm_alloc */
  void *start;   /* start address of the shared memory */
  void *end; /* (fake) end address of the shared memory (real end address is 3
                  times larger in size) */
  atomic<int> *lock; /* array of locks, one lock per block (8 bytes) */
  size_t size; /* (fake) the allocation size of the segment (real size is 3
                  times larger, one for temporary writing, one for current
                  version, one for archieve version) */
  int *archieve_timestamp; /* array of timestamp, record the timestamp for the
                              archieve version */
};

struct region { /* Region, central control unit */
  segment_entry_t
      *segment_entry; /* allocated segment of the region, a linked list. */
  segment_entry_t
      *freed_segment_entry; /* freed segment of the region, a linked list (Delay
                               the free untilt tm_destroy(), to ensure safe
                               memory reclaimation */
  atomic<int> segment_lock; /* a lock to protect concurrent accesses to the
                               shared segment_entry list */
  atomic<int> timestamp;    /* global timestamp */
  void *start;              /* start address of the first allocated segment */
  size_t size;              /* size of the first allocated segment*/
  size_t align;             /* align of the region */
};

/* ================================================================
                        Helper functions
   ================================================================ */

/** Alloc the segment and segment_entry (linked-list wrapper)
 *  @param size  Size of the allocation, multiple of align
 *  @param align Alignment (in bytes)
 *  @return      The linked-list entry of segment. If fails, return NULL.
 **/
static inline segment_entry_t *alloc_segment(size_t size, size_t align) {
  /* Allocate the memory for the segment. */
  segment_t *segment = (segment_t *)malloc(sizeof(segment_t));
  if (unlikely(segment == NULL)) {
    return NULL;
  }
  /* The size of the allocation is VERSION_NUM larger, one for current valid
     block, one for archieve block, the other for temporary writing. */
  if (unlikely(posix_memalign(&(segment->start), align, size * VERSION_NUM) !=
               0)) {
    free(segment);
    return NULL;
  }
  /* The number of blocks in each segment. */
  int block_num = (size >> BLOCK_SHIFT);
  /* "<< 1", make the lock 8 byte aligned manually */
  segment->lock = (atomic<int> *)malloc(sizeof(atomic<int>) * block_num << 1);
  if (unlikely(segment->lock == NULL)) {
    free(segment->start);
    free(segment);
    return NULL;
  }
  segment->archieve_timestamp = (int *)malloc(sizeof(int) * block_num);
  if (unlikely(segment->archieve_timestamp == NULL)) {
    free(segment->start);
    free(segment->lock);
    free(segment);
    return NULL;
  }

  memset(segment->start, 0, size * VERSION_NUM);
  segment->end = (char *)segment->start + size;
  segment->size = size;
  for (int i = 0; i < block_num; ++i) {
    /* Initialize the lock */
    segment->lock[i << 1] = 0;
    /* Initialize all archieve timestamps above the max timestamp the execution
       can reach. This is an indicator for invalid archieve version. */
    segment->archieve_timestamp[i] = MAX_TIMESTAMP;
  }

  /* Allocate the memory for the linked-list wrapper of segment. */
  segment_entry_t *segment_entry =
      (segment_entry_t *)malloc(sizeof(segment_entry_t));
  if (unlikely(segment_entry == NULL)) {
    free(segment->start);
    free(segment->lock);
    free(segment->archieve_timestamp);
    free(segment);
    return NULL;
  }
  segment_entry->segment = segment;
  segment_entry->next = NULL;
  return segment_entry;
}

/** Find the segment for the given address.
 *  @param region The address of the region, storing the allocated segment.
 *  @param trans  The address of the calling transaction, storing the to alloc
 *                segments, which are not in region now.
 *  @param pos    The position to search for.
 *  @return       The segment contains the pos in its allocation. If not
 *                found, return NULL
 **/
static inline segment_t *
find_target_segment(region_t *region, transaction_t *trans, const void *pos) {
  /* First search the last accessed segment, improve the locality. */
  if (likely(trans->last_accessed_segment != NULL &&
             pos >= trans->last_accessed_segment->start &&
             pos < trans->last_accessed_segment->end)) {
    return trans->last_accessed_segment;
  }

  /* Search the "pos" inside the allocated segments of the "region". */
  segment_entry_t *segment_entry;
  segment_entry = region->segment_entry;
  while (segment_entry != NULL) {
    if (pos >= segment_entry->segment->start &&
        pos < segment_entry->segment->end) {
      trans->last_accessed_segment = segment_entry->segment;
      return segment_entry->segment;
    }
    segment_entry = segment_entry->next;
  }

  /* Search the "pos" inside the to-alloc segments of the "trans". */
  segment_entry = trans->to_alloc_entry;
  while (segment_entry != NULL) {
    if (pos >= segment_entry->segment->start &&
        pos < segment_entry->segment->end) {
      trans->last_accessed_segment = segment_entry->segment;
      return segment_entry->segment;
    }
    segment_entry = segment_entry->next;
  }
  return NULL;
}

/** Check whether the transaction has read this block before or not.
 *  @param trans  The address of the calling transaction.
 *  @param lock   The address of the lock of the target block.
 *  @return       If this transaction has read this block, return the
 *                address of the read_entry. Else, return NULL.
 **/
static inline read_entry_t *has_read(transaction_t *trans, atomic<int> *lock) {
  read_entry_t *read_entry = trans->read_entry;
  int read_cnt = trans->read_cnt;
  /* Search in the reverse order, better locality. */
  while (read_cnt--) {
    if (read_entry[read_cnt].lock == lock)
      return read_entry + read_cnt;
  }
  return NULL;
}

/** Check whether the transaction has written this block before or not.
 *  @param trans  The address of the calling transaction.
 *  @param lock   The address of the lock of the target block.
 *  @return       If this transaction has written this block, return the
 *                address of the write_entry. Else, return NULL.
 **/
static inline write_entry_t *has_written(transaction_t *trans,
                                         atomic<int> *lock) {
  write_entry_t *write_entry = trans->write_entry;
  int write_cnt = trans->write_cnt;
  /* Search in the reverse order, better locality. */
  while (write_cnt--) {
    if (write_entry[write_cnt].lock == lock) {
      return write_entry + write_cnt;
    }
  }
  return NULL;
}

/** Rollback the given transaction, clean all memory allocation, release all
 *  write-locks.
 *  @param trans  The address of the calling transaction
 *  @return       void
 **/
static inline void rollback(transaction_t *trans) {
  if (trans->is_ro) {
    /* If it is read-only, nothing but free the "trans" */
    free(trans);
  } else {
    /* If it is read-write "trans", we have to clean up all allocated memory*/

    /* Clean the read set. */
    free(trans->read_entry);

    /* Clean the write set. */
    write_entry_t *write_entry = trans->write_entry;
    int write_cnt = trans->write_cnt;
    while (write_cnt--) {
      /* Restoret the previous lock value. */
      int lock_value = atomic_load(write_entry[write_cnt].lock);
      int target_lock_value = lock_value & CLEAR_WRITE_MASK;
      atomic_compare_exchange_strong(write_entry[write_cnt].lock, &lock_value,
                                     target_lock_value);
    }
    free(trans->write_entry);

    /* Clean the to-free segment entries. */
    segment_entry_t *segment_entry = trans->to_free_entry;
    segment_entry_t *segment_entry_tmp;
    while (segment_entry != NULL) {
      segment_entry_tmp = segment_entry->next;
      free(segment_entry);
      segment_entry = segment_entry_tmp;
    }

    /* Clean the to-alloc segment entries. */
    segment_entry = trans->to_alloc_entry;
    while (segment_entry != NULL) {
      free(segment_entry->segment->start);
      free(segment_entry->segment->lock);
      free(segment_entry->segment->archieve_timestamp);
      free(segment_entry->segment);
      segment_entry_tmp = segment_entry->next;
      free(segment_entry);
      segment_entry = segment_entry_tmp;
    }
    free(trans);
  }
}

/** Commit the given transaction, clean all memory allocation, update the
 *  global timestamp, update all write-locks, allocate and free segments
 *  inside the region.
 *  @param region The address of the region
 *  @param trans  The address of the calling transaction
 *  @return       True if the commit is successful, false on failure.
 **/
static inline bool commit(region_t *region, transaction_t *trans) {
  if (trans->is_ro) {
    /* If it is read-only, we can commit directly. */
    free(trans);
    return true;
  } else {
    /* If it is read-write, we have to validate all reads, update
       the write-locks to current timestamp, free segments and
       allocate segments accordingly. */

    /* Fast path check, if it is the same as the start timestamp, no need to
       check again. */
    if (atomic_load(&region->timestamp) != trans->start_timestamp) {
      /* Validate the read. */
      read_entry_t *read_entry = trans->read_entry;
      int read_cnt = trans->read_cnt;
      int lock_value, cur_timestamp;
      for (int i = 0; i < read_cnt; ++i) {
        /* If the timestamp is still the same as that when we read
           the block, it means that no other thread has modifies
           that memory region since our read. We can succussfuly
           continue to the next step. */
        lock_value = atomic_load(read_entry[i].lock);
        cur_timestamp = lock_value >> RESERVE_BITS;
        if ((has_written(trans, read_entry[i].lock) == NULL &&
             lock_value & WRITE_BIT) ||
            cur_timestamp != read_entry[i].read_timestamp) {
          rollback(trans);
          return false;
        }
      }
    }
    /* Clean the read set. */
    free(trans->read_entry);

    /* We are safe now, increment the global timestamp. */
    int expected_timestamp = atomic_fetch_add(&region->timestamp, 1) + 1;

    /* Write the writes now (updating the valid index) */
    write_entry_t *write_entry = trans->write_entry;
    int write_cnt = trans->write_cnt;
    int target_lock_value = expected_timestamp << RESERVE_BITS;
    while (write_cnt--) {
      int lock_value = atomic_load(write_entry[write_cnt].lock);
      /* The current valid block */
      int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;
      /* Circular update */
      int new_valid_block = (valid_block + 1) % VERSION_NUM;
      /* Tell other threads that we are writing to the archieve now. */
      atomic_store(write_entry[write_cnt].lock, lock_value | ARCHIEVE_BIT);
      /* Update the archieve timestamp as current timestamp */
      write_entry[write_cnt]
          .segment->archieve_timestamp[write_entry[write_cnt].block_index] =
          lock_value >> RESERVE_BITS;
      /* Set up the new lock value, change the valid block index,
         and update the current timestamp. */
      atomic_store(write_entry[write_cnt].lock,
                   target_lock_value | (new_valid_block << VALID_SHIFT));
    }
    free(trans->write_entry);

    segment_entry_t *to_free_entry = trans->to_free_entry;
    segment_entry_t *to_alloc_entry = trans->to_alloc_entry;
    /* Logically free the segment if not NULL. */
    if (to_free_entry != NULL) {
      /* A lock to protect concurrent modifying. */
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
            /* Free the segment logically. Delay the physical
               free until the "tm_destroy" is called. */
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

    /* Alloc the segments inside region. */
    if (to_alloc_entry != NULL) {
      /* Find the last alloc entry. */
      while (to_alloc_entry->next != NULL) {
        to_alloc_entry = to_alloc_entry->next;
      }
      int expected_lock_value = 0;
      while (unlikely(atomic_compare_exchange_strong(&region->segment_lock,
                                                     &expected_lock_value,
                                                     1) == false))
        expected_lock_value = 0;
      /* Insert the new allocation at the head of the linked list. */
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
 *  non-free-able allocated segment of the requested size and alignment.
 *  @param size  Size of the first shared segment of memory to allocate (in
 *               bytes), must be a positive multiple of the alignment
 *  @param align Alignment (in bytes, must be a power of 2) that the shared
 *               memory region must support
 *  @return      Opaque shared memory region handle, 'invalid_shared' on failure
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
    free(segment_entry->segment->archieve_timestamp);
    free(segment_entry->segment);
    tmp = segment_entry->next;
    free(segment_entry);
    segment_entry = tmp;
  }
  /* Physically free the freed segment. */
  segment_entry = region->freed_segment_entry;
  while (segment_entry != NULL) {
    free(segment_entry->segment->start);
    free(segment_entry->segment->lock);
    free(segment_entry->segment->archieve_timestamp);
    free(segment_entry->segment);
    tmp = segment_entry->next;
    free(segment_entry);
    segment_entry = tmp;
  }
  free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the
 *  shared memory region.
 *  @param shared Shared memory region to query
 *  @return Start address of the first allocated segment
 **/
void *tm_start(shared_t shared) noexcept { return ((region_t *)shared)->start; }

/** [thread-safe] Return the size (in bytes) of the first allocated segment of
 *  the shared memory region.
 *  @param shared Shared memory region to query
 *  @return First allocated segment size
 **/
size_t tm_size(shared_t shared) noexcept { return ((region_t *)shared)->size; }

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the
 *  given shared memory region.
 *  @param shared Shared memory region to query
 *  @return Alignment used globally
 **/
size_t tm_align(shared_t shared) noexcept {
  return ((region_t *)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 *  @param shared Shared memory region to start a transaction on
 *  @param is_ro  Whether the transaction is read-only
 *  @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)malloc(sizeof(transaction_t));
  if (unlikely(trans == NULL)) {
    return invalid_tx;
  }
  if (!is_ro) {
    trans->read_entry =
        (read_entry_t *)malloc(sizeof(read_entry_t) * DEFAULT_READ_SIZE);
    if (unlikely(trans->read_entry == NULL)) {
      free(trans);
      return invalid_tx;
    }
    trans->write_entry =
        (write_entry_t *)malloc(sizeof(write_entry_t) * DEFAULT_WRITE_SIZE);
    if (unlikely(trans->write_entry == NULL)) {
      free(trans->read_entry);
      free(trans);
      return invalid_tx;
    }
    trans->read_cnt = 0;
    trans->read_capacity = DEFAULT_READ_SIZE;
    trans->write_cnt = 0;
    trans->write_capacity = DEFAULT_WRITE_SIZE;
  }
  trans->last_accessed_segment = NULL;
  trans->to_free_entry = NULL;
  trans->to_alloc_entry = NULL;
  trans->is_ro = is_ro;
  /* Retrieve the start timestamp. */
  trans->start_timestamp = atomic_load(&region->timestamp);
  return (tx_t)trans;
}

/** [thread-safe] End the given transaction.
 *  @param shared Shared memory region associated with the transaction
 *  @param tx     Transaction to end
 *  @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;
  return commit(region, trans);
}

/** [thread-safe] Read operation in the given transaction, source in the shared
 *  region and target in a private region.
 *  @param shared Shared memory region associated with the transaction
 *  @param tx     Transaction to use
 *  @param source Source start address (in the shared region)
 *  @param size   Length to copy (in bytes), must be a positive multiple of the
 *                alignment
 *  @param target Target start address (in a private region)
 *  @return Whether the whole transaction can continue
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

  /* Calculate the index of the current position inside the segment. */
  short block_index =
      ((char *)source - (char *)target_segment->start) >> BLOCK_SHIFT;
restart:
  /* Read the lock value, get the necessary information. */
  /* "<< 1", make the lock 8 byte aligned manually */
  atomic<int> *lock = &target_segment->lock[block_index << 1];
  int lock_value = atomic_load(lock);
  int cur_timestamp = lock_value >> RESERVE_BITS;
  /* To tell which one is current valid, writable and archieve block. */
  int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;
  int writable_block = (valid_block + 1) % VERSION_NUM;
  int archieve_block = (valid_block + 2) % VERSION_NUM;

  if (trans->is_ro) {
    /* If the current valid block has a higher timestamp or the WRITE_BIT
       is set, it means this block has been written or committed concurrently
       by other transactions. */
    if (cur_timestamp > trans->start_timestamp || lock_value & WRITE_BIT) {
      /* In this case, we check whether we have a valid archieve version
         to read from. */
      if (likely(cur_timestamp > trans->start_timestamp &&
                 target_segment->archieve_timestamp[block_index] <=
                     trans->start_timestamp)) {
        memcpy(target, (char *)source + archieve_block * target_segment->size,
               size);
        if (unlikely(lock_value != atomic_load(lock) ||
                     ((lock_value & ARCHIEVE_BIT) != 0)))
          goto restart;
        return true;
      }
      /* If invalid archieve version, not much we can do. */
      rollback(trans);
      return false;
    }
    /* If the current valid block is available, read from it directly. */
    memcpy(target, (char *)source + valid_block * target_segment->size, size);
    if (unlikely(lock_value != atomic_load(lock)))
      goto restart;
    return true;

  } else {
    write_entry_t *write_entry = has_written(trans, lock);
    /* If we have already written to the block, get the written value. */
    if (write_entry != NULL) {
      memcpy(target, (char *)source + writable_block * target_segment->size,
             size);
      return true;
    } else {
      /* If another read-write entry has acquried the lock, abort. */
      if (lock_value & WRITE_BIT) {
        rollback(trans);
        return false;
      }
      /* Early validation, check when the current timestamp is larger
         than the start timestamp of the transaction. */
      if (cur_timestamp > trans->start_timestamp) {
        read_entry_t *read_entry = trans->read_entry;
        int read_cnt = trans->read_cnt;
        int read_lock_value, read_cur_timestamp;
        /* Check early read first. */
        for (int i = 0; i < read_cnt; ++i) {
          read_lock_value = atomic_load(read_entry[i].lock);
          read_cur_timestamp = read_lock_value >> RESERVE_BITS;
          if ((has_written(trans, read_entry[i].lock) == NULL &&
               read_lock_value & WRITE_BIT) ||
              read_cur_timestamp != read_entry[i].read_timestamp) {
            rollback(trans);
            return false;
          }
        }
        /* If the validation is successful, update the start
           timestamp. Indeed it should be named end_timestamp,
           but for memory efficiency, we reuse it. */
        trans->start_timestamp = cur_timestamp;
      }

      /* Allow duplications in the read-set. */
      read_entry_t *read_entry = trans->read_entry + trans->read_cnt;
      read_entry->lock = lock;
      read_entry->read_timestamp = cur_timestamp;
      trans->read_cnt++;
      if (unlikely(trans->read_cnt == trans->read_capacity)) {
        trans->read_capacity <<= 1;
        read_entry_t *new_read_entry = (read_entry_t *)realloc(
            trans->read_entry, sizeof(read_entry_t) * trans->read_capacity);
        if (unlikely(new_read_entry == NULL)) {
          rollback(trans);
          return false;
        }
        trans->read_entry = new_read_entry;
      }

      /* Read the current valid block into the target. */
      memcpy(target, (char *)source + valid_block * target_segment->size, size);
      if (unlikely(lock_value != atomic_load(lock)))
        goto restart;
      return true;
    }
  }
}

/** [thread-safe] Write operation in the given transaction, source in a private
 *  region and target in the shared region.
 *  @param shared Shared memory region associated with the transaction
 *  @param tx     Transaction to use
 *  @param source Source start address (in a private region)
 *  @param size   Length to copy (in bytes), must be a positive multiple of the
 *                alignment
 *  @param target Target start address (in the shared region)
 *  @return       Whether the whole transaction can continue
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

  /* Calculate the index of the current position inside the segment. */
  short block_index =
      ((char *)target - (char *)target_segment->start) >> BLOCK_SHIFT;

  /* Read the lock value, get the necessary information. */
  /* "<< 1", make the lock 8 byte aligned manually */
  atomic<int> *lock = &target_segment->lock[block_index << 1];
  int lock_value = atomic_load(lock);
  int cur_timestamp = lock_value >> RESERVE_BITS;
  /* To tell which one is current valid, writable block. */
  int valid_block = (lock_value & VALID_BIT) >> VALID_SHIFT;
  int writable_block = (valid_block + 1) % VERSION_NUM;

  write_entry_t *write_entry = has_written(trans, lock);

  /* If we has written to this block, overwrite our previous writes. */
  if (write_entry != NULL) {
    memcpy((char *)target + writable_block * target_segment->size, source,
           size);
    return true;
  } else {
    /* If the lock is already acquired by other threads, abort. */
    if (lock_value & WRITE_BIT) {
      rollback(trans);
      return false;
    }
    /* Or if we have read this block before and the cur_timestamp is
       different from our previous reading, which means some other
       transactions has updated this block between the read and write,
       not much we can do, abort. */
    read_entry_t *read_entry = has_read(trans, lock);
    if ((read_entry != NULL && cur_timestamp != read_entry->read_timestamp)) {
      rollback(trans);
      return false;
    }

    /* Declare the ownership of this block. */
    int target_lock_value = lock_value | WRITE_BIT;
    if (unlikely(atomic_compare_exchange_strong(lock, &lock_value,
                                                target_lock_value) == false)) {
      rollback(trans);
      return false;
    }

    /* Setup the write-entry for necessary information. */
    write_entry = trans->write_entry + trans->write_cnt;
    write_entry->lock = lock;
    write_entry->segment = target_segment;
    write_entry->block_index = block_index;
    trans->write_cnt++;
    if (unlikely(trans->write_cnt == trans->write_capacity)) {
      trans->write_capacity <<= 1;
      write_entry_t *new_write_entry = (write_entry_t *)realloc(
          trans->write_entry, sizeof(write_entry_t) * trans->write_capacity);
      if (unlikely(new_write_entry == NULL)) {
        rollback(trans);
        return false;
      }
      trans->write_entry = new_write_entry;
    }

    memcpy((char *)target + writable_block * target_segment->size, source,
           size);
    return true;
  }
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive
 *               multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first
 *               byte of the newly allocated, aligned segment
 * @return       Whether the whole transaction can continue (success/nomem),
 *               or not (abort_alloc)
 **/
Alloc tm_alloc(shared_t shared, tx_t tx as(unused), size_t size,
               void **target) noexcept {
  region_t *region = (region_t *)shared;
  transaction_t *trans = (transaction_t *)tx;

  segment_entry_t *segment_entry = alloc_segment(size, region->align);
  if (unlikely(segment_entry == NULL)) {
    return Alloc::nomem;
  }
  /* Put the allocated segment to the to_alloc_entry list, alloc
     when the transaction commits. */
  *target = segment_entry->segment->start;
  segment_entry->next = trans->to_alloc_entry;
  trans->to_alloc_entry = segment_entry;
  return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment
 *               to deallocate
 * @return       Whether the whole transaction can continue
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
  /* Put the freed segment to the to_free_entry list, free
     from the region when the transaction commits. */
  segment_entry->segment = target_segment;
  segment_entry->next = trans->to_free_entry;
  trans->to_free_entry = segment_entry;
  return true;
}