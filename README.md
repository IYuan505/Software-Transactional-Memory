# 3-versioned software transactional memory

15.25x speed up with 20-core CPUs.

## Introduction
Software transactional memory (STM) is a concurrency control mechanism that allows multiple threads to access shared memory concurrently while ensuring that the memory accesses are atomic and consistent.

In software transactional memory, shared memory accesses are grouped into transactions. A transaction is a sequence of memory accesses that appear to execute atomically as if they were a single instruction. If a thread tries to access shared memory while another thread is in the middle of a transaction, the second thread will block until the first thread has completed the transaction, or it could kill the first thread and continue execution. This ensures that the shared memory accesses within a transaction are atomic and consistent.

STM is often used in concurrent programming to manage access to shared memory and to ensure that the shared memory remains in a consistent state. It can be implemented in software or hardware. STM is a relatively recent development in the field of concurrency control, and it has the potential to simplify the design and implementation of concurrent programs by eliminating the need for low-level synchronization mechanisms such as locks and semaphores.

## Goal
The goal of this project is to improve the performance of STM as much as possible, to be evaluated on a 20-core machine with 20 concurrent threads. The machine's specifications are the following:

* **CPU**: 2 (dual-socket) Intel(R) Xeon(R) 10-core CPU E5-2680 v2 at 2.80GHz (×2 hyperthreading ⇒ 40 virtual cores)
* **RAM**: 256GB
* **OS**: GNU/Linux (distribution: Ubuntu 18.04 LTS)


## Baseline
The baseline implementation of the software transactional memory could be found in the `reference` folder. It uses a single global lock to ensure atomicity and consistency. Before each transaction begins, it acquires a lock, and only releases the lock after the transaction gets committed.

## Main Achievement
1. Use 2 Phase Locking together with oblivious read (Timestamp Ordering) to ensure the atomicity of the transaction.
2. Implemented a 3-versioned transaction memory, to increase the possibility that a read-only transaction successfully commits.
3. Fine-grained concurrency control, up to an 8-byte scale. Given the OLTP nature of the transactional memory, the extremely fine granularity improves the performance a lot. 
4. Adjust the data structure, such that both hardware-wise and software-wise cache performance gets improved.

## Final Result
The final implementation achieves a 15.25x speedup compared to the baseline implementation. The best performant source code could be found in the `best` folder.
