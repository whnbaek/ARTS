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
#include "arts/runtime/sync/EventFunctions.h"
#include "arts/arts.h"
#include "arts/gas/Guid.h"
#include "arts/gas/OutOfOrder.h"
#include "arts/gas/RouteTable.h"
#include "arts/introspection/Counter.h"
#include "arts/introspection/Introspection.h"
#include "arts/runtime/Globals.h"
#include "arts/runtime/Runtime.h"
#include "arts/runtime/compute/EdtFunctions.h"
#include "arts/runtime/network/RemoteFunctions.h"
#include "arts/system/Debug.h"
#include "arts/utils/Atomics.h"
#include "arts/utils/LinkList.h"
#include <assert.h>
#include <time.h>

#define DPRINTF(...)

extern __thread struct artsEdt *currentEdt;

bool artsEventCreateInternal(artsGuid_t *guid, unsigned int route,
                             unsigned int dependentCount,
                             unsigned int latchCount, bool destroyOnFire,
                             artsGuid_t eventData) {
  unsigned int eventSize =
      sizeof(struct artsEvent) + sizeof(struct artsDependent) * dependentCount;
  ARTSSETMEMSHOTTYPE(artsEventMemorySize);
  void *eventPacket = artsCalloc(eventSize);
  ARTSSETMEMSHOTTYPE(artsDefaultMemorySize);

  if (eventSize) {
    struct artsEvent *event = eventPacket;
    event->header.type = ARTS_EVENT;
    event->header.size = eventSize;
    event->dependentCount = 0;
    event->dependent.size = dependentCount;
    event->latchCount = latchCount;
    event->destroyOnFire = (destroyOnFire) ? dependentCount : -1;
    event->data = eventData;

    if (route == artsGlobalRankId) {
      if (*guid) {
        artsRouteTableAddItem(eventPacket, *guid, artsGlobalRankId, false);
        artsRouteTableFireOO(*guid, artsOutOfOrderHandler);
      } else {
        *guid = artsGuidCreateForRank(route, ARTS_EVENT);
        artsRouteTableAddItem(eventPacket, *guid, artsGlobalRankId, false);
      }
    } else
      artsRemoteMemoryMove(route, *guid, eventPacket, eventSize,
                           ARTS_REMOTE_EVENT_MOVE_MSG, artsFree);

    return true;
  }
  return false;
}

artsGuid_t artsEventCreate(unsigned int route, unsigned int latchCount) {
  ARTSEDTCOUNTERTIMERSTART(eventCreateCounter);
  artsGuid_t guid = NULL_GUID;
  artsEventCreateInternal(&guid, route, INITIAL_DEPENDENT_SIZE, latchCount,
                          false, NULL_GUID);
  ARTSEDTCOUNTERTIMERENDINCREMENT(eventCreateCounter);
  return guid;
}

artsGuid_t artsEventCreateWithGuid(artsGuid_t guid, unsigned int latchCount) {
  ARTSEDTCOUNTERTIMERSTART(eventCreateCounter);
  unsigned int route = artsGuidGetRank(guid);
  bool ret = artsEventCreateInternal(&guid, route, INITIAL_DEPENDENT_SIZE,
                                     latchCount, false, NULL_GUID);
  ARTSEDTCOUNTERTIMERENDINCREMENT(eventCreateCounter);
  return (ret) ? guid : NULL_GUID;
}

void artsEventFree(struct artsEvent *event) {
  struct artsDependentList *trail, *current = event->dependent.next;
  while (current) {
    trail = current;
    current = current->next;
    artsFree(trail);
  }
  artsFree(event);
}

void artsEventDestroy(artsGuid_t guid) {
  struct artsEvent *event = (struct artsEvent *)artsRouteTableLookupItem(guid);
  if (event != NULL) {
    artsRouteTableRemoveItem(guid);
    artsEventFree(event);
  }
}

void artsEventSatisfySlot(artsGuid_t eventGuid, artsGuid_t dataGuid,
                          uint32_t slot) {
  ARTSEDTCOUNTERTIMERSTART(signalEventCounter);
  if (currentEdt && currentEdt->invalidateCount > 0) {
    artsOutOfOrderEventSatisfySlot(currentEdt->currentEdt, eventGuid, dataGuid,
                                   slot, true);
    return;
  }
  PRINTF("Signal Event:%u, Data:%u at %u\n", eventGuid, dataGuid, slot);
  struct artsEvent *event =
      (struct artsEvent *)artsRouteTableLookupItem(eventGuid);
  if (!event) {
    unsigned int rank = artsGuidGetRank(eventGuid);
    if (rank != artsGlobalRankId) {
      artsRemoteEventSatisfySlot(eventGuid, dataGuid, slot);
    } else {
      artsOutOfOrderEventSatisfySlot(eventGuid, eventGuid, dataGuid, slot,
                                     false);
    }
  } else {
    if (event->fired) {
      PRINTF("ARTS_EVENT_LATCH_T already fired guid: %lu data: %lu slot: %u\n",
             eventGuid, dataGuid, slot);
      artsDebugGenerateSegFault();
    }

    unsigned int res;
    if (slot == ARTS_EVENT_LATCH_INCR_SLOT) {
      res = artsAtomicAdd(&event->latchCount, 1U);
    } else if (slot == ARTS_EVENT_LATCH_DECR_SLOT) {
      if (dataGuid != NULL_GUID)
        event->data = dataGuid;
      res = artsAtomicSub(&event->latchCount, 1U);
    } else {
      PRINTF("Bad latch slot %u\n", slot);
      artsDebugGenerateSegFault();
    }

    /// When the latch count reaches 0, fire the event
    if (!res) {
      /// If the event is already fired, we should not fire it again
      if (artsAtomicSwapBool(&event->fired, true)) {
        PRINTF(
            "ARTS_EVENT_LATCH_T already fired guid: %lu data: %lu slot: %u\n",
            eventGuid, dataGuid, slot);
        artsDebugGenerateSegFault();
      }
      /// If the event is not fired, we need to fire it
      else {
        struct artsDependentList *dependentList = &event->dependent;
        struct artsDependent *dependent = event->dependent.dependents;
        int i, j;
        /// Capture current state
        unsigned int lastKnown = artsAtomicFetchAdd(&event->dependentCount, 0U);
        event->pos = lastKnown + 1;
        i = 0;
        int totalSize = 0;
        /// Process all dependents up to lastKnown
        while (i < lastKnown) {
          j = i - totalSize;
          while (i < lastKnown && j < dependentList->size) {
            while (!dependent[j].doneWriting)
              ;
            if (dependent[j].type == ARTS_EDT) {
              artsSignalEdt(dependent[j].addr, dependent[j].slot, event->data);
            } else if (dependent[j].type == ARTS_EVENT) {
#ifdef COUNT
              // THIS IS A TEMP FIX... problem is recursion...
              artsCounterTimerEndIncrement(artsGetCounter(
                  (artsThreadInfo.currentEdtGuid) ? signalEventCounterOn
                                                  : signalEventCounter));
              uint64_t start = artsCounterGetStartTime(artsGetCounter(
                  (artsThreadInfo.currentEdtGuid) ? signalEventCounterOn
                                                  : signalEventCounter));
#endif
              artsEventSatisfySlot(dependent[j].addr, event->data,
                                   dependent[j].slot);
#ifdef COUNT
              // THIS IS A TEMP FIX... problem is recursion...
              artsCounterSetEndTime(
                  artsGetCounter((artsThreadInfo.currentEdtGuid)
                                     ? signalEventCounterOn
                                     : signalEventCounter),
                  start);
#endif
            } else if (dependent[j].type == ARTS_CALLBACK) {
              artsEdtDep_t arg;
              arg.guid = event->data;
              arg.ptr = artsRouteTableLookupItem(event->data);
              arg.mode = ARTS_NULL;
              dependent[j].callback(arg);
            }
            j++;
            i++;
          }
          totalSize += dependentList->size;
          while (i < lastKnown && dependentList->next == NULL)
            ;
          dependentList = dependentList->next;
          dependent = dependentList->dependents;
        }
        if (!event->destroyOnFire) {
          artsEventFree(event);
          artsRouteTableRemoveItem(eventGuid);
        }
      }
    }
  }
  artsUpdatePerformanceMetric(artsEventSignalThroughput, artsThread, 1, false);
  ARTSEDTCOUNTERTIMERENDINCREMENT(signalEventCounter);
}

struct artsDependent *artsDependentGet(struct artsDependentList *head,
                                       int position) {
  struct artsDependentList *list = head;
  volatile struct artsDependentList *temp;

  while (1) {
    /// If the position is greater than the size of the list, we need to
    /// allocate a new list
    if (position >= list->size) {
      if (position - list->size == 0) {
        if (list->next == NULL) {
          temp = artsCalloc(sizeof(struct artsDependentList) +
                            sizeof(struct artsDependent) * list->size * 2);
          temp->size = list->size * 2;
          list->next = (struct artsDependentList *)temp;
        }
      }

      // EXPONENTIONAL BACK OFF THIS
      while (list->next == NULL) {
      }

      position -= list->size;
      list = list->next;
    } else
      break;
  }
  return list->dependents + position;
}

void artsAddDependence(artsGuid_t source, artsGuid_t destination,
                       uint32_t slot) {
  PRINTF("Add Dependence from %u to %u at %u\n", source, destination, slot);
  artsType_t mode = artsGuidGetType(destination);
  struct artsHeader *sourceHeader = artsRouteTableLookupItem(source);
  if (sourceHeader == NULL) {
    unsigned int rank = artsGuidGetRank(source);
    if (rank != artsGlobalRankId) {
      artsRemoteAddDependence(source, destination, slot, mode, rank);
    } else {
      artsOutOfOrderAddDependence(source, destination, slot, mode, source);
    }
    return;
  }

  struct artsEvent *event = (struct artsEvent *)sourceHeader;
  if (mode == ARTS_EDT) {
    struct artsDependentList *dependentList = &event->dependent;
    struct artsDependent *dependent;
    unsigned int position = artsAtomicFetchAdd(&event->dependentCount, 1U);
    dependent = artsDependentGet(dependentList, position);
    dependent->type = ARTS_EDT;
    dependent->addr = destination;
    dependent->slot = slot;
    COMPILER_DO_NOT_REORDER_WRITES_BETWEEN_THIS_POINT();
    dependent->doneWriting = true;

    unsigned int destroyEvent = (event->destroyOnFire != -1)
                                    ? artsAtomicSub(&event->destroyOnFire, 1U)
                                    : 1;
    if (event->fired) {
      while (event->pos == 0)
        ;
      if (position >= event->pos - 1) {
        artsSignalEdt(destination, slot, event->data);
        if (!destroyEvent) {
          artsEventFree(event);
          artsRouteTableRemoveItem(source);
        }
      }
    }
  } else if (mode == ARTS_EVENT) {
    struct artsDependentList *dependentList = &event->dependent;
    struct artsDependent *dependent;
    unsigned int position = artsAtomicFetchAdd(&event->dependentCount, 1U);
    dependent = artsDependentGet(dependentList, position);
    dependent->type = ARTS_EVENT;
    dependent->addr = destination;
    dependent->slot = slot;
    COMPILER_DO_NOT_REORDER_WRITES_BETWEEN_THIS_POINT();
    dependent->doneWriting = true;

    unsigned int destroyEvent = (event->destroyOnFire != -1)
                                    ? artsAtomicSub(&event->destroyOnFire, 1U)
                                    : 1;
    if (event->fired) {
      while (event->pos == 0)
        ;
      if (event->pos - 1 <= position) {
        artsEventSatisfySlot(destination, event->data, slot);
        if (!destroyEvent) {
          artsEventFree(event);
          artsRouteTableRemoveItem(source);
        }
      }
    }
  }
  return;
}

void artsAddLocalEventCallback(artsGuid_t source, eventCallback_t callback) {
  struct artsEvent *event =
      (struct artsEvent *)artsRouteTableLookupItem(source);
  if (event && artsGuidGetType(source) == ARTS_EVENT) {
    struct artsDependentList *dependentList = &event->dependent;
    struct artsDependent *dependent;
    unsigned int position = artsAtomicFetchAdd(&event->dependentCount, 1U);
    dependent = artsDependentGet(dependentList, position);
    dependent->type = ARTS_CALLBACK;
    dependent->callback = callback;
    dependent->addr = NULL_GUID;
    dependent->slot = 0;
    COMPILER_DO_NOT_REORDER_WRITES_BETWEEN_THIS_POINT();
    dependent->doneWriting = true;

    unsigned int destroyEvent = (event->destroyOnFire != -1)
                                    ? artsAtomicSub(&event->destroyOnFire, 1U)
                                    : 1;
    if (event->fired) {
      while (event->pos == 0)
        ;
      if (event->pos - 1 <= position) {
        artsEdtDep_t arg;
        arg.guid = event->data;
        arg.ptr = artsRouteTableLookupItem(event->data);
        arg.mode = ARTS_NULL;
        callback(arg);
        if (!destroyEvent) {
          artsEventFree(event);
          artsRouteTableRemoveItem(source);
        }
      }
    }
  }
}

bool artsIsEventFired(artsGuid_t event) {
  bool fired = false;
  struct artsEvent *actualEvent =
      (struct artsEvent *)artsRouteTableLookupItem(event);
  if (actualEvent)
    fired = actualEvent->fired;
  return fired;
}

/// Persistent events
struct artsPersistentEventVersion *
artsPushPersistentEventVersion(struct artsPersistentEvent *event);

struct artsLinkList *artsGetEventVersions(struct artsPersistentEvent *event) {
  if (event->versions != NULL)
    return event->versions;
  event->versions = artsLinkListGroupNew(1);
  struct artsPersistentEventVersion *version =
      artsPushPersistentEventVersion(event);
  version->dependent.next = NULL;

  assert(version != NULL);
  return event->versions;
}

struct artsPersistentEventVersion *
artsPushPersistentEventVersion(struct artsPersistentEvent *event) {
  struct artsLinkList *versions = artsGetEventVersions(event);
  struct artsPersistentEventVersion *next = artsLinkListNewItem(
      (sizeof(struct artsPersistentEventVersion) +
       (sizeof(struct artsDependent) * INITIAL_DEPENDENT_SIZE)));
  next->latchCount = 0;
  next->dependentCount = 0;
  next->dependent.size = INITIAL_DEPENDENT_SIZE;
  struct artsPersistentEventVersion *last = NULL;
  if (versions && versions->tailPtr)
    last =
        (struct artsPersistentEventVersion *)artsLinkListGetTailData(versions);

  if (last)
    next->version = last->version + 1;
  else
    next->version = 0;
  artsLinkListPushBack(versions, next);
  return next;
}

struct artsPersistentEventVersion *
artsGetFrontPersistentEventVersion(struct artsPersistentEvent *event) {
  struct artsPersistentEventVersion *v =
      (struct artsPersistentEventVersion *)artsLinkListGetFrontData(
          artsGetEventVersions(event));
  return v;
}

struct artsPersistentEventVersion *
artsGetLastPersistentEventVersion(struct artsPersistentEvent *event) {
  struct artsPersistentEventVersion *v =
      (struct artsPersistentEventVersion *)artsLinkListGetTailData(
          artsGetEventVersions(event));
  return v;
}

bool artsPersistentEventCreateInternal(artsGuid_t *guid, unsigned int route,
                                       artsGuid_t eventData) {
  if (eventData == NULL_GUID) {
    printf("Event data is NULL_GUID for persistent event\n");
    artsDebugGenerateSegFault();
  }
  const unsigned int eventSize = sizeof(struct artsPersistentEvent);
  ARTSSETMEMSHOTTYPE(artsPersistentEventMemorySize);
  void *eventPacket = artsCalloc(eventSize);
  ARTSSETMEMSHOTTYPE(artsDefaultMemorySize);

  if (eventSize) {
    struct artsPersistentEvent *event = eventPacket;
    event->header.type = ARTS_PERSISTENT_EVENT;
    event->header.size = eventSize;
    event->versions = NULL;
    event->data = eventData;
    event->lock = 0;

    if (route == artsGlobalRankId) {
      if (*guid) {
        artsRouteTableAddItem(eventPacket, *guid, artsGlobalRankId, false);
        artsRouteTableFireOO(*guid, artsOutOfOrderHandler);
      } else {
        *guid = artsGuidCreateForRank(route, ARTS_PERSISTENT_EVENT);
        artsRouteTableAddItem(eventPacket, *guid, artsGlobalRankId, false);
      }
    } else {
      artsRemoteMemoryMove(route, *guid, eventPacket, eventSize,
                           ARTS_REMOTE_PERSISTENT_EVENT_MOVE_MSG, artsFree);
    }
    return true;
  }
  PRINTF(" - Failed to create persistent event\n");
  return false;
}

bool artsPersistentEventFreeVersion(struct artsPersistentEvent *event) {
  struct artsLinkList *versions = event->versions;
  assert(versions != NULL);
  bool last = true;
  artsLock(&versions->lock);

  if (versions->headPtr != versions->tailPtr)
    last = false;

  /// Get the top version
  struct artsPersistentEventVersion *version =
      (struct artsPersistentEventVersion *)(versions->headPtr + 1);
  assert(version != NULL);

  /// Free dependencies for this version
  PRINTF("   - Freeing dependencies for version %u\n", version->version);
  struct artsDependentList *trail, *current = version->dependent.next;
  while (current) {
    trail = current;
    current = current->next;
    artsFree(trail);
  }

  /// Free the version
  if (last) {
    version->latchCount = 0;
    version->dependentCount = 0;
    PRINTF("   - Resetting dependencies for version %u\n", version->version);
  } else {
    PRINTF("   - Popping version %u\n", version->version);
    versions->headPtr = versions->headPtr->next;
    struct artsLinkListItem *item = ((struct artsLinkListItem *)version) - 1;
    artsFree(item);
  }

  artsUnlock(&versions->lock);
  return last;
}

artsGuid_t artsPersistentEventCreate(unsigned int route,
                                     unsigned int latchCount,
                                     artsGuid_t dataGuid) {
  ARTSEDTCOUNTERTIMERSTART(persistentEventCreateCounter);
  artsGuid_t guid = NULL_GUID;
  artsPersistentEventCreateInternal(&guid, route, dataGuid);
  ARTSEDTCOUNTERTIMERENDINCREMENT(persistentEventCreateCounter);
  return guid;
}

void artsPersistentEventDestroy(artsGuid_t guid) {
  struct artsPersistentEvent *event =
      (struct artsPersistentEvent *)artsRouteTableLookupItem(guid);
  if (event != NULL) {
    artsLock(&event->lock);
    artsRouteTableRemoveItem(guid);
    while (!artsPersistentEventFreeVersion(event))
      ;
    artsUnlock(&event->lock);
    artsFree(event);
  }
}

void artsPersistentEventSatisfy(artsGuid_t eventGuid, uint32_t action,
                                bool lock) {
  ARTSEDTCOUNTERTIMERSTART(signalPersistentEventCounter);
  if (currentEdt && currentEdt->invalidateCount > 0) {
    artsOutOfOrderPersistentEventSatisfySlot(currentEdt->currentEdt, eventGuid,
                                             action, true);
    return;
  }
  struct artsPersistentEvent *event =
      (struct artsPersistentEvent *)artsRouteTableLookupItem(eventGuid);
  if (!event) {
    unsigned int rank = artsGuidGetRank(eventGuid);
    if (rank != artsGlobalRankId) {
      artsRemotePersistentEventSatisfySlot(eventGuid, action, lock);
    } else {
      artsOutOfOrderPersistentEventSatisfySlot(eventGuid, eventGuid, action,
                                               false);
    }
  } else {
    if (lock)
      artsLock(&event->lock);
    if (event->data == NULL_GUID) {
      DPRINTF("Data: NULL_GUID, avoiding signaling\n");
      artsDebugGenerateSegFault();
    }
    unsigned int res;
    struct artsPersistentEventVersion *version =
        artsGetFrontPersistentEventVersion(event);
    PRINTF("[PersistentEvent] Satisfying event %lu, version %u\n", eventGuid,
           version->version);
    assert(version != NULL);
    if (action == ARTS_EVENT_LATCH_INCR_SLOT) {
      res = artsAtomicFetchAdd(&version->latchCount, 0U);
      if (res == 1) {
        DPRINTF("Latch count is 1 for event %u, creating new version\n",
                eventGuid);
        version = artsPushPersistentEventVersion(event);
        PRINTF("    - Created version %u for event %lu\n", version->version,
               eventGuid);
      }
      res = artsAtomicAdd(&version->latchCount, 1U);
      DPRINTF("Increment latch count to %d for event %u\n", res, eventGuid);
    } else if (action == ARTS_EVENT_LATCH_DECR_SLOT) {
      res = artsAtomicFetchAdd(&version->latchCount, 0U);
      if (res == (unsigned int)-1) {
        DPRINTF("Latch count is -1 for event %u, creating new version\n",
                eventGuid);
        version = artsPushPersistentEventVersion(event);
        PRINTF("    - Created version %u for event %lu\n", version->version,
               eventGuid);
      }
      res = artsAtomicSub(&version->latchCount, 1U);
      DPRINTF("Decrement latch count to %d for event %u \n", res, eventGuid);
    } else if (action == ARTS_EVENT_UPDATE) {
      res = artsAtomicFetchAdd(&version->latchCount, 0U);
      DPRINTF("Update latch count to %d for event %u \n", res, eventGuid);
      PRINTF("    - Updated event %lu\n", eventGuid);
    } else {
      DPRINTF("Bad latch slot %u\n", action);
      artsDebugGenerateSegFault();
    }

    if (res == 0) {
      assert(version != NULL);
      struct artsDependentList *dependentList = &version->dependent;
      struct artsDependent *dependent = version->dependent.dependents;
      int i, j;
      unsigned int lastKnown = artsAtomicFetchAdd(&version->dependentCount, 0U);
      i = 0;
      int totalSize = 0;
      while (i < lastKnown) {
        j = i - totalSize;
        while (i < lastKnown && j < dependentList->size) {
          while (!dependent[j].doneWriting)
            ;
          if (dependent[j].type == ARTS_EDT) {
            if (event->data != NULL_GUID)
              artsSignalEdt(dependent[j].addr, dependent[j].slot, event->data);
            else
              DPRINTF("Event data is NULL_GUID for event %u\n", eventGuid);
          } else if (dependent[j].type == ARTS_EVENT) {
#ifdef COUNT
            // THIS IS A TEMP FIX... problem is recursion...
            artsCounterTimerEndIncrement(artsGetCounter(
                (artsThreadInfo.currentEdtGuid) ? signalEventCounterOn
                                                : signalEventCounter));
            uint64_t start = artsCounterGetStartTime(artsGetCounter(
                (artsThreadInfo.currentEdtGuid) ? signalEventCounterOn
                                                : signalEventCounter));
#endif
            artsPersistentEventSatisfy(dependent[j].addr, dependent[j].slot,
                                       false);
#ifdef COUNT
            // THIS IS A TEMP FIX... problem is recursion...
            artsCounterSetEndTime(artsGetCounter((artsThreadInfo.currentEdtGuid)
                                                     ? signalEventCounterOn
                                                     : signalEventCounter),
                                  start);
#endif
          } else if (dependent[j].type == ARTS_CALLBACK) {
            artsEdtDep_t arg;
            arg.guid = event->data;
            arg.ptr = artsRouteTableLookupItem(event->data);
            arg.mode = ARTS_NULL;
            dependent[j].callback(arg);
          }
          j++;
          i++;
        }
        totalSize += dependentList->size;
        while (i < lastKnown && dependentList->next == NULL)
          ;
        dependentList = dependentList->next;
        dependent = dependentList->dependents;
      }

      /// Free dependencies for this version
      PRINTF("[PersistentEvent] Freeing dependencies for event %lu\n",
             eventGuid);
      artsPersistentEventFreeVersion(event);
    }
    if (lock)
      artsUnlock(&event->lock);
  }
  artsUpdatePerformanceMetric(artsPersistentEventSignalThroughput, artsThread,
                              1, false);
  ARTSEDTCOUNTERTIMERENDINCREMENT(signalPersistentEventCounter);
}

void artsPersistentEventIncrementLatch(artsGuid_t eventGuid) {
  artsPersistentEventSatisfy(eventGuid, ARTS_EVENT_LATCH_INCR_SLOT, true);
}

void artsPersistentEventDecrementLatch(artsGuid_t eventGuid) {
  artsPersistentEventSatisfy(eventGuid, ARTS_EVENT_LATCH_DECR_SLOT, true);
}

void artsAddDependenceToPersistentEvent(artsGuid_t eventSource,
                                        artsGuid_t edtDest, uint32_t edtSlot) {
  /// Check that the eventSource is a persistent event
  if (artsGuidGetType(eventSource) != ARTS_PERSISTENT_EVENT) {
    PRINTF("Event source %u is not a persistent event\n", eventSource);
    artsDebugGenerateSegFault();
    return;
  }
  artsType_t mode = artsGuidGetType(edtDest);
  struct artsHeader *sourceHeader = artsRouteTableLookupItem(eventSource);
  if (sourceHeader == NULL) {
    unsigned int rank = artsGuidGetRank(eventSource);
    if (rank != artsGlobalRankId) {
      artsRemoteAddDependenceToPersistentEvent(eventSource, edtDest, edtSlot,
                                               mode, rank);
    } else {
      artsOutOfOrderAddDependenceToPersistentEvent(eventSource, edtDest,
                                                   edtSlot, mode, eventSource);
    }
    return;
  }

  DPRINTF("Add Dependence from persistent event %u to EDT %u at %u\n",
          eventSource, edtDest, edtSlot);
  struct artsPersistentEvent *event =
      (struct artsPersistentEvent *)sourceHeader;
  artsLock(&event->lock);
  struct artsPersistentEventVersion *version =
      artsGetLastPersistentEventVersion(event);
  assert(version != NULL);
  if (mode == ARTS_EDT) {
    struct artsDependentList *dependentList = &version->dependent;
    unsigned int position = artsAtomicFetchAdd(&version->dependentCount, 1U);
    struct artsDependent *dependent = artsDependentGet(dependentList, position);
    assert(dependent != NULL);
    dependent->type = ARTS_EDT;
    dependent->addr = edtDest;
    dependent->slot = edtSlot;
    COMPILER_DO_NOT_REORDER_WRITES_BETWEEN_THIS_POINT();
    dependent->doneWriting = true;

    unsigned int res = artsAtomicFetchAdd(&version->latchCount, 0U);
    if (res == 0) {
      artsPersistentEventSatisfy(eventSource, ARTS_EVENT_UPDATE, false);
    }
  } else if (mode == ARTS_EVENT) {
    struct artsDependentList *dependentList = &version->dependent;
    unsigned int position = artsAtomicFetchAdd(&version->dependentCount, 1U);
    struct artsDependent *dependent = artsDependentGet(dependentList, position);
    dependent->type = ARTS_EVENT;
    dependent->addr = edtDest;
    dependent->slot = edtSlot;
    COMPILER_DO_NOT_REORDER_WRITES_BETWEEN_THIS_POINT();
    dependent->doneWriting = true;

    if (artsAtomicFetchAdd(&version->latchCount, 0U) == 0) {
      artsPersistentEventSatisfy(eventSource, ARTS_EVENT_UPDATE, false);
    }
  }
  artsUnlock(&event->lock);
  return;
}