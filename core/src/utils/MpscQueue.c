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

#include "arts/utils/MpscQueue.h"
#include "arts/arts.h"
#include <sched.h> /* for sched_yield() */

/**
 * MPSC Queue Implementation - Vyukov Algorithm with C11 Atomics
 *
 * This is a lock-free Multiple Producer Single Consumer queue based on
 * Dmitry Vyukov's MPSC algorithm, using C11 atomics for correctness on
 * weakly-ordered architectures (ARM64, PowerPC).
 *
 * Key memory ordering:
 * - Push uses acquire-release on the atomic exchange (serialization point)
 * - Push uses release on the linkage store (publishes data to consumer)
 * - Pop uses acquire on next pointer reads (synchronizes with producer)
 *
 * The algorithm:
 * - Push: Atomically swap tail to claim position, then link previous->next
 * - Pop: Read head->next; if NULL but tail != head, producer is mid-push
 *
 * On high-core-count systems (64-256 cores), the structure is padded to
 * prevent false sharing between producer (tail) and consumer (head) data.
 */

/* Spin loop constants for producer preemption handling */
#define MPSC_MAX_SPINS 1000
#define MPSC_YIELD_THRESHOLD 100

void artsMpscQueueInit(struct artsMpscQueue *queue) {
  /* Initialize stub node's next pointer to NULL */
  atomic_init(&queue->stub.next, NULL);

  /* Both head and tail point to stub initially (empty queue state) */
  atomic_init(&queue->tail, &queue->stub);
  queue->head = &queue->stub;
}

struct artsMpscQueue *artsMpscQueueNew(void) {
  struct artsMpscQueue *queue = (struct artsMpscQueue *)artsCallocAlign(
      1, sizeof(struct artsMpscQueue), ARTS_MPSC_CACHE_LINE_SIZE);
  artsMpscQueueInit(queue);
  return queue;
}

void artsMpscQueuePush(struct artsMpscQueue *queue, struct artsMpscNode *node) {
  /*
   * Step 1: Initialize the new node's next pointer to NULL.
   * RELAXED ordering is sufficient because no other thread can see this
   * node yet - we haven't published it.
   */
  atomic_store_explicit(&node->next, NULL, memory_order_relaxed);

  /*
   * Step 2: Atomically exchange tail to point to our new node.
   * This is the linearization point - establishes total order among producers.
   *
   * ACQ_REL ordering:
   * - RELEASE: Ensures the node initialization (step 1) is visible before
   *   any thread reads our position in the queue
   * - ACQUIRE: Synchronizes with previous producer's history
   */
  struct artsMpscNode *prev =
      atomic_exchange_explicit(&queue->tail, node, memory_order_acq_rel);

  /*
   * Step 3: Link the previous tail to our node.
   * This is the critical synchronization point with the consumer.
   *
   * RELEASE ordering: Ensures all prior writes (node data, step 1) are
   * visible to any thread that performs an ACQUIRE load on prev->next.
   * This publishes both the node and its contents to the consumer.
   */
  atomic_store_explicit(&prev->next, node, memory_order_release);
}

struct artsMpscNode *artsMpscQueuePop(struct artsMpscQueue *queue) {
  /* head is only accessed by the single consumer, no atomic needed */
  struct artsMpscNode *head = queue->head;

  /*
   * Read the next pointer with ACQUIRE ordering.
   * This synchronizes with the producer's RELEASE store in step 3 of push.
   * If we see a non-NULL value, all writes that happened before the
   * producer's release store are now visible to us.
   */
  struct artsMpscNode *next =
      atomic_load_explicit(&head->next, memory_order_acquire);

  /* If head is the stub node, we need to advance past it */
  if (head == &queue->stub) {
    if (next == NULL) {
      /* Queue is truly empty */
      return NULL;
    }
    /* Move head past the stub */
    queue->head = next;
    head = next;
    next = atomic_load_explicit(&head->next, memory_order_acquire);
  }

  /* Fast path: there's a next node available */
  if (next != NULL) {
    queue->head = next;
    return head;
  }

  /*
   * Slow path: next is NULL, but we need to check if queue is truly empty
   * or if a producer is mid-push (has done exchange but not linkage yet).
   */
  struct artsMpscNode *tail =
      atomic_load_explicit(&queue->tail, memory_order_acquire);

  if (head == tail) {
    /* Queue is truly empty */
    return NULL;
  }

  /*
   * Inconsistent state detected: tail has moved (producer did exchange)
   * but head->next is still NULL (producer hasn't completed linkage).
   *
   * This happens when a producer is preempted between steps 2 and 3.
   * We must wait for the producer to complete. Use exponential backoff
   * with CPU_RELAX to reduce contention, then yield to OS if needed.
   */
  int spins = 0;
  while ((next = atomic_load_explicit(&head->next, memory_order_acquire)) ==
         NULL) {
    if (spins < MPSC_MAX_SPINS) {
      ARTS_CPU_RELAX();
      spins++;
    } else {
      /*
       * Producer is likely descheduled. Yield our time slice to let
       * it run and complete the linkage. This prevents priority inversion
       * on heavily loaded systems.
       */
      sched_yield();
      spins = 0;
    }
  }

  /* Producer completed linkage, we can proceed */
  queue->head = next;
  return head;
}
