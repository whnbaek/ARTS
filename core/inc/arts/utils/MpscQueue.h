/******************************************************************************
** This material was prepared as an account of work sponsored by an agency   **
** of the United States Government.  Neither the United States Government    **
** nor the United States Department of Energy, nor Battelle, nor any of      **
** their employees, nor any jurisdiction or organization that has cooperated **
** in the development of these materials, makes any warranty, express or     **
** implied, or assumes any legal liability or responsibility for the accuracy,*
** completeness, or usefulness or any information, apparatus, product,       **
** software, or process disclosed, or represents that its use would not      **
** infringe privately owned rights.                                          **
**                                                                           **
** Reference herein to any specific commercial product, process, or service  **
** by trade name, trademark, manufacturer, or otherwise does not necessarily **
** constitute or imply its endorsement, recommendation, or favoring by the   **
** United States Government or any agency thereof, or Battelle Memorial      **
** Institute. The views and opinions of authors expressed herein do not      **
** necessarily state or reflect those of the United States Government or     **
** any agency thereof.                                                       **
**                                                                           **
**                      PACIFIC NORTHWEST NATIONAL LABORATORY                **
**                                  operated by                              **
**                                    BATTELLE                               **
**                                     for the                               **
**                      UNITED STATES DEPARTMENT OF ENERGY                   **
**                         under Contract DE-AC05-76RL01830                  **
**                                                                           **
** Copyright 2019 Battelle Memorial Institute                                **
** Licensed under the Apache License, Version 2.0 (the "License");           **
** you may not use this file except in compliance with the License.          **
** You may obtain a copy of the License at                                   **
**                                                                           **
**    https://www.apache.org/licenses/LICENSE-2.0                            **
**                                                                           **
** Unless required by applicable law or agreed to in writing, software       **
** distributed under the License is distributed on an "AS IS" BASIS, WITHOUT **
** WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the  **
** License for the specific language governing permissions and limitations   **
******************************************************************************/

/**
 * MPSC (Multiple Producer Single Consumer) Queue
 *
 * A lock-free queue based on Dmitry Vyukov's MPSC algorithm, designed for
 * multiple producer threads to enqueue items and a single consumer thread
 * to dequeue items. This is used as an "inbox" for each worker thread in
 * the CHASE_LEV_DEQUE_MPSC_QUEUE scheduler policy.
 *
 * Key properties:
 * - Wait-free producers: Push completes in bounded steps
 * - Obstruction-free consumer: Pop may spin if producer is preempted
 * - Uses C11 atomics with proper memory ordering for ARM64 compatibility
 * - Cache-line padded to prevent false sharing on high-core-count systems
 *
 * Multiple threads can push EDTs to a worker's inbox (for affinity-based
 * scheduling), while only the owning worker thread can pop from its inbox.
 */

#ifndef ARTS_UTILS_MPSC_QUEUE_H
#define ARTS_UTILS_MPSC_QUEUE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>

/* Cache line size - use 128 bytes for safety on modern servers
 * (some CPUs prefetch adjacent lines) */
#define ARTS_MPSC_CACHE_LINE_SIZE 128

/* Architecture-specific CPU pause/yield hint for spin loops */
#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>
  #define ARTS_CPU_RELAX() _mm_pause()
#elif defined(__aarch64__) || defined(_M_ARM64)
  #define ARTS_CPU_RELAX() __asm__ volatile("yield" ::: "memory")
#elif defined(__powerpc64__)
  #define ARTS_CPU_RELAX() __asm__ volatile("or 27,27,27" ::: "memory")
#else
  #define ARTS_CPU_RELAX() ((void)0)
#endif

/**
 * Node structure for the MPSC queue (intrusive linked list).
 * Uses C11 atomic for the next pointer to ensure proper memory ordering.
 */
struct artsMpscNode {
  _Atomic(struct artsMpscNode *) next;
};

/**
 * MPSC Queue structure.
 *
 * Uses Vyukov's MPSC algorithm with proper cache line isolation:
 * - Producer hot data (tail) on its own cache line
 * - Consumer hot data (head) on its own cache line
 * This prevents false sharing on high-core-count systems (64-256 cores).
 */
struct artsMpscQueue {
  /* Producer hot cache line: tail is modified by all producers via atomic exchange */
  _Atomic(struct artsMpscNode *) tail;
  char pad1[ARTS_MPSC_CACHE_LINE_SIZE - sizeof(_Atomic(struct artsMpscNode *))];

  /* Consumer hot cache line: head is only modified by the single consumer */
  struct artsMpscNode *head;
  char pad2[ARTS_MPSC_CACHE_LINE_SIZE - sizeof(struct artsMpscNode *)];

  /* Stub node for empty queue state (on separate cache line) */
  struct artsMpscNode stub;
  char pad3[ARTS_MPSC_CACHE_LINE_SIZE - sizeof(struct artsMpscNode)];
} __attribute__((aligned(ARTS_MPSC_CACHE_LINE_SIZE)));

/**
 * Create a new MPSC queue.
 * @return Pointer to the newly created queue.
 */
struct artsMpscQueue *artsMpscQueueNew(void);

/**
 * Initialize an MPSC queue in-place.
 * @param queue The queue to initialize.
 */
void artsMpscQueueInit(struct artsMpscQueue *queue);

/**
 * Push an item to the queue (can be called from multiple producer threads).
 *
 * This operation is wait-free: it completes in a bounded number of steps
 * regardless of contention from other threads.
 *
 * Memory ordering: Uses acquire-release semantics to ensure that all writes
 * to the node's data are visible to the consumer after a successful pop.
 *
 * @param queue The queue to push to.
 * @param node The node to push (embedded in the EDT structure).
 */
void artsMpscQueuePush(struct artsMpscQueue *queue, struct artsMpscNode *node);

/**
 * Pop an item from the queue (only the single consumer thread should call this).
 *
 * This operation is obstruction-free: if a producer is preempted mid-push,
 * the consumer will spin briefly with exponential backoff before yielding.
 *
 * Memory ordering: Uses acquire semantics to synchronize with the producer's
 * release, ensuring all node data is visible after a successful pop.
 *
 * @param queue The queue to pop from.
 * @return The popped node, or NULL if the queue is empty.
 */
struct artsMpscNode *artsMpscQueuePop(struct artsMpscQueue *queue);

#ifdef __cplusplus
}
#endif

#endif /* ARTS_UTILS_MPSC_QUEUE_H */
