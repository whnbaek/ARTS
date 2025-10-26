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
#include "arts/runtime/compute/EdtFunctions.h"

#include "arts/gas/Guid.h"
#include "arts/gas/OutOfOrder.h"
#include "arts/gas/RouteTable.h"
#include "arts/introspection/Metrics.h"
#include "arts/runtime/Globals.h"
#include "arts/runtime/Runtime.h"
#include "arts/runtime/network/RemoteFunctions.h"
#include "arts/runtime/sync/TerminationDetection.h"
#include "arts/system/ArtsPrint.h"
#include "arts/system/Debug.h"
#include "arts/utils/ArrayList.h"
#include "arts/utils/Atomics.h"

#ifdef USE_GPU
#include "arts/gpu/GpuRuntime.cuh"
#endif

#define maxEpochArrayList 32

extern unsigned int numNumaDomains;

__thread artsArrayList *epochList = NULL;
__thread struct artsEdt *currentEdt = NULL;

bool artsSetCurrentEpochGuid(artsGuid_t epochGuid) {
  if (epochGuid) {
    if (!epochList)
      epochList = artsNewArrayList(sizeof(artsGuid_t), 8);
    artsPushToArrayList(epochList, &epochGuid);
    if (currentEdt) {
      currentEdt->epochGuid = epochGuid;
      return true;
    }
  }
  return false;
}

artsGuid_t artsGetCurrentEpochGuid() {
  if (epochList) {
    uint64_t length = artsLengthArrayList(epochList);
    if (length) {
      artsGuid_t *guid =
          (artsGuid_t *)artsGetFromArrayList(epochList, length - 1);
      return *guid;
    }
  }
  return NULL_GUID;
}

artsGuid_t *artsCheckEpochIsRoot(artsGuid_t toCheck) {
  if (epochList) {
    uint64_t length = artsLengthArrayList(epochList);
    for (uint64_t i = 0; i < length; i++) {
      artsGuid_t *guid = (artsGuid_t *)artsGetFromArrayList(epochList, i);
      if (*guid == toCheck)
        return guid;
    }
  }
  ARTS_INFO("ERROR %lu is not a valid epoch", toCheck);
  return NULL;
}

void artsSetThreadLocalEdtInfo(struct artsEdt *edt) {
  artsThreadInfo.currentEdtGuid = edt->currentEdt;
  currentEdt = edt;

  if (epochList)
    artsResetArrayList(epochList);

  artsSetCurrentEpochGuid(currentEdt->epochGuid);
}

void artsSaveThreadLocal(threadLocal_t *tl) {
  if (currentEdt) {
    EDT_COUNTER_STOP();
  }

  CONTEXT_SWITCH_START();
  tl->currentEdtGuid = artsThreadInfo.currentEdtGuid;
  tl->currentEdt = currentEdt;
  tl->epochList = (void *)epochList;

  artsThreadInfo.currentEdtGuid = NULL_GUID;
  currentEdt = NULL;
  epochList = NULL;
  CONTEXT_SWITCH_STOP();
  artsMetricsTriggerEvent(artsYieldBW, artsThread, 1);
}

void artsRestoreThreadLocal(threadLocal_t *tl) {
  CONTEXT_SWITCH_START();
  artsThreadInfo.currentEdtGuid = tl->currentEdtGuid;
  currentEdt = tl->currentEdt;
  if (epochList)
    artsDeleteArrayList(epochList);
  epochList = (artsArrayList *)tl->epochList;
  CONTEXT_SWITCH_STOP();

  EDT_COUNTER_START();
}

void artsIncrementFinishedEpochList() {
  if (epochList) {

    unsigned int epochArrayLength = artsLengthArrayList(epochList);
    for (unsigned int i = 0; i < epochArrayLength; i++) {
      artsGuid_t *guid = (artsGuid_t *)artsGetFromArrayList(epochList, i);
      ARTS_DEBUG("Current EDT [Guid: %lu] -  Unsetting Epoch [Guid: %lu]",
                 artsThreadInfo.currentEdtGuid, *guid);
      if (*guid)
        incrementFinishedEpoch(*guid);
    }

    if (epochArrayLength > maxEpochArrayList) {
      artsDeleteArrayList(epochList);
      epochList = NULL;
    } else
      artsResetArrayList(epochList);
  }
  globalShutdownGuidIncFinished();
}

void artsUnsetThreadLocalEdtInfo() {
  artsIncrementFinishedEpochList();
  artsThreadInfo.currentEdtGuid = NULL_GUID;
  currentEdt = NULL;
}

bool artsEdtCreateInternal(struct artsEdt *edt, artsType_t mode,
                           artsGuid_t *guid, unsigned int route,
                           unsigned int cluster, unsigned int edtSpace,
                           artsGuid_t outputBuffer, artsEdt_t funcPtr,
                           uint32_t paramc, uint64_t *paramv, uint32_t depc,
                           bool useEpoch, artsGuid_t epochGuid, bool hasDepv) {
  if (!edt)
    edt = (struct artsEdt *)artsCallocAlignWithType(1, edtSpace, 16,
                                                    artsEdtMemorySize);
  edt->header.type = mode;
  edt->header.size = edtSpace;
  if (edt) {
    bool createdGuid = false;
    if (*guid == NULL_GUID) {
      createdGuid = true;
      edt->currentEdt = *guid = artsGuidCreateForRank(route, mode);
    } else
      edt->currentEdt = *guid;

    edt->funcPtr = funcPtr;
    edt->depc = (hasDepv) ? depc : 0;
    edt->paramc = paramc;
    edt->outputBuffer = outputBuffer;
    edt->epochGuid = NULL_GUID;
    edt->cluster = cluster;
    edt->depcNeeded = depc;

    if (useEpoch) {
      artsGuid_t currentEpochGuid = NULL_GUID;
      if (epochGuid && artsCheckEpochIsRoot(epochGuid))
        currentEpochGuid = epochGuid;
      else
        currentEpochGuid = artsGetCurrentEpochGuid();

      if (currentEpochGuid) {
        edt->epochGuid = currentEpochGuid;
        incrementActiveEpoch(currentEpochGuid);
      }
    }
    globalShutdownGuidIncActive();

    if (paramc) {
      unsigned int offset =
          edtSpace - (depc * sizeof(artsEdtDep_t) + paramc * sizeof(uint64_t));
      char *tmp = (char *)edt + offset;
      memcpy(tmp, paramv, sizeof(uint64_t) * paramc);
    }

    /// DEBUG
    if (useEpoch) {
      ARTS_INFO("Creating EDT [Guid: %lu] [Epoch: %lu] [Deps: %u] [Route: %d]",
                *guid, edt->epochGuid, edt->depc, route);
    } else {
      ARTS_INFO("Created EDT [Guid: %lu] [Route: %d]", *guid, route);
    }

    if (route != artsGlobalRankId) {
      artsRemoteMemoryMove(route, *guid, (void *)edt,
                           (unsigned int)edt->header.size,
                           ARTS_REMOTE_EDT_MOVE_MSG, artsFree);
    } else {
      incOustandingEdts(1);
      if (createdGuid) {
        artsRouteTableAddItem(edt, *guid, artsGlobalRankId, false);
        if (edt->depcNeeded == 0)
          artsHandleReadyEdt(edt);
      } else {
        artsRouteTableAddItemRace(edt, *guid, artsGlobalRankId, false);
        if (edt->depcNeeded) {
          artsRouteTableFireOO(*guid, artsOutOfOrderHandler);
        } else
          artsHandleReadyEdt(edt);
      }
    }

    return true;
  }
  return false;
}

artsGuid_t artsEdtCreateDep(artsEdt_t funcPtr, unsigned int route,
                            uint32_t paramc, uint64_t *paramv, uint32_t depc,
                            bool hasDepv) {
  EDT_CREATE_COUNTER_START();
  if (route == -1)
    route = artsGlobalRankId;
  unsigned int depSpace = (hasDepv) ? depc * sizeof(artsEdtDep_t) : 0;
  unsigned int edtSpace =
      sizeof(struct artsEdt) + paramc * sizeof(uint64_t) + depSpace;
  artsGuid_t guid = NULL_GUID;
  artsGuid_t *guidPtr = &guid;
  bool created = artsEdtCreateInternal(
      NULL, ARTS_EDT, guidPtr, route, artsThreadInfo.clusterId, edtSpace,
      NULL_GUID, funcPtr, paramc, paramv, depc, true, NULL_GUID, hasDepv);
  EDT_CREATE_COUNTER_STOP();
  return guid;
}

artsGuid_t artsEdtCreateWithGuidDep(artsEdt_t funcPtr, artsGuid_t guid,
                                    uint32_t paramc, uint64_t *paramv,
                                    uint32_t depc, bool hasDepv) {
  EDT_CREATE_COUNTER_START();
  unsigned int route = artsGuidGetRank(guid);
  unsigned int depSpace = (hasDepv) ? depc * sizeof(artsEdtDep_t) : 0;
  unsigned int edtSpace =
      sizeof(struct artsEdt) + paramc * sizeof(uint64_t) + depSpace;
  bool ret = artsEdtCreateInternal(
      NULL, ARTS_EDT, &guid, route, artsThreadInfo.clusterId, edtSpace,
      NULL_GUID, funcPtr, paramc, paramv, depc, true, NULL_GUID, hasDepv);
  EDT_CREATE_COUNTER_STOP();
  return (ret) ? guid : NULL_GUID;
}

artsGuid_t artsEdtCreateWithEpochDep(artsEdt_t funcPtr, unsigned int route,
                                     uint32_t paramc, uint64_t *paramv,
                                     uint32_t depc, artsGuid_t epochGuid,
                                     bool hasDepv) {
  EDT_CREATE_COUNTER_START();
  if (route == -1)
    route = artsGlobalRankId;
  unsigned int depSpace = (hasDepv) ? depc * sizeof(artsEdtDep_t) : 0;
  unsigned int edtSpace =
      sizeof(struct artsEdt) + paramc * sizeof(uint64_t) + depSpace;
  artsGuid_t guid = NULL_GUID;
  bool created = artsEdtCreateInternal(
      NULL, ARTS_EDT, &guid, route, artsThreadInfo.clusterId, edtSpace,
      NULL_GUID, funcPtr, paramc, paramv, depc, true, epochGuid, hasDepv);
  EDT_CREATE_COUNTER_STOP();
  return guid;
}

artsGuid_t artsEdtCreate(artsEdt_t funcPtr, unsigned int route, uint32_t paramc,
                         uint64_t *paramv, uint32_t depc) {
  if (route == -1)
    route = artsGlobalRankId;
  return artsEdtCreateDep(funcPtr, route, paramc, paramv, depc, true);
}

artsGuid_t artsEdtCreateWithGuid(artsEdt_t funcPtr, artsGuid_t guid,
                                 uint32_t paramc, uint64_t *paramv,
                                 uint32_t depc) {
  return artsEdtCreateWithGuidDep(funcPtr, guid, paramc, paramv, depc, true);
}

artsGuid_t artsEdtCreateWithEpoch(artsEdt_t funcPtr, unsigned int route,
                                  uint32_t paramc, uint64_t *paramv,
                                  uint32_t depc, artsGuid_t epochGuid) {
  if (route == -1)
    route = artsGlobalRankId;
  return artsEdtCreateWithEpochDep(funcPtr, route, paramc, paramv, depc,
                                   epochGuid, true);
}

void artsEdtFree(struct artsEdt *edt) {
  artsThreadInfo.edtFree = 1;
  artsFree(edt);
  artsThreadInfo.edtFree = 0;
}

void artsEdtDelete(struct artsEdt *edt) {
  artsRouteTableRemoveItem(edt->currentEdt);
  artsEdtFree(edt);
}

void artsEdtDestroy(artsGuid_t guid) {
  struct artsEdt *edt = (struct artsEdt *)artsRouteTableLookupItem(guid);
  artsRouteTableRemoveItem(guid);
  artsEdtFree(edt);
}

void *artsGetDepv(void *edtPtr) {
  struct artsEdt *edt = (struct artsEdt *)edtPtr;
  unsigned int paramc = edt->paramc;
  if (edt->header.type == ARTS_EDT) {
    struct artsEdt *edt = (struct artsEdt *)edtPtr;
    return (void *)((uint64_t *)(edt + 1) + paramc);
  }
#ifdef USE_GPU
  if (edt->header.type == ARTS_GPU_EDT) {
    artsGpuEdt_t *edtGpu = (artsGpuEdt_t *)edtPtr;
    return (void *)((uint64_t *)(edtGpu + 1) + paramc);
  }
#endif
  return NULL;
}

void internalSignalEdt(artsGuid_t edtPacket, uint32_t slot, artsGuid_t dataGuid,
                       artsType_t mode, void *ptr, unsigned int size) {
  SIGNAL_EDT_COUNTER_START();
  // This is old CDAG code...
  if (currentEdt && currentEdt->invalidateCount > 0) {
    if (mode == ARTS_PTR)
      artsOutOfOrderSignalEdtWithPtr(edtPacket, dataGuid, ptr, size, slot);
    else
      artsOutOfOrderSignalEdt(currentEdt->currentEdt, edtPacket, dataGuid, slot,
                              mode, true);
  } else {
    unsigned int rank = artsGuidGetRank(edtPacket);
    if (rank == artsGlobalRankId) {
      struct artsEdt *edt =
          (struct artsEdt *)artsRouteTableLookupItem(edtPacket);
      if (edt) {
        artsEdtDep_t *edtDep = (artsEdtDep_t *)artsGetDepv(edt);
        if (slot < edt->depc) {
          edtDep[slot].guid = dataGuid;
          edtDep[slot].mode = mode;
          edtDep[slot].ptr = ptr;
        }
        unsigned int res = artsAtomicSub(&edt->depcNeeded, 1U);
        ARTS_INFO("Signal DB [Guid: %lu] to EDT [Guid: %lu, Slot: %u, "
                  "DepCount: %d]",
                  dataGuid, edt->currentEdt, slot, res);
        if (res == 0)
          artsHandleReadyEdt(edt);
      } else {
        if (mode == ARTS_PTR)
          artsOutOfOrderSignalEdtWithPtr(edtPacket, dataGuid, ptr, size, slot);
        else
          artsOutOfOrderSignalEdt(edtPacket, edtPacket, dataGuid, slot, mode,
                                  false);
      }
    } else {
      if (mode == ARTS_PTR)
        artsRemoteSignalEdtWithPtr(edtPacket, dataGuid, ptr, size, slot);
      else
        artsRemoteSignalEdt(edtPacket, dataGuid, slot, mode);
    }
  }
  artsMetricsTriggerEvent(artsEdtSignalThroughput, artsThread, 1);
  SIGNAL_EDT_COUNTER_STOP();
}

void artsSignalEdt(artsGuid_t edtGuid, uint32_t slot, artsGuid_t dataGuid) {
  artsGuid_t acqGuid = dataGuid;
  artsType_t mode = artsGuidGetType(dataGuid);
  internalSignalEdt(edtGuid, slot, acqGuid, mode, NULL, 0);
}

void artsSignalEdtValue(artsGuid_t edtGuid, uint32_t slot, uint64_t value) {
  internalSignalEdt(edtGuid, slot, value, ARTS_SINGLE_VALUE, NULL, 0);
}

void artsSignalEdtPtr(artsGuid_t edtGuid, uint32_t slot, void *ptr,
                      unsigned int size) {
  internalSignalEdt(edtGuid, slot, NULL_GUID, ARTS_PTR, ptr, size);
}

artsGuid_t artsActiveMessageWithDb(artsEdt_t funcPtr, uint32_t paramc,
                                   uint64_t *paramv, uint32_t depc,
                                   artsGuid_t dbGuid) {
  unsigned int rank = artsGuidGetRank(dbGuid);
  artsGuid_t guid = artsEdtCreate(funcPtr, rank, paramc, paramv, depc + 1);
  artsSignalEdt(guid, 0, dbGuid);
  return guid;
}

artsGuid_t artsActiveMessageWithDbAt(artsEdt_t funcPtr, uint32_t paramc,
                                     uint64_t *paramv, uint32_t depc,
                                     artsGuid_t dbGuid, unsigned int rank) {
  artsGuid_t guid = artsEdtCreate(funcPtr, rank, paramc, paramv, depc + 1);
  artsSignalEdt(guid, 0, dbGuid);
  return guid;
}

artsGuid_t artsActiveMessageWithBuffer(artsEdt_t funcPtr, unsigned int route,
                                       uint32_t paramc, uint64_t *paramv,
                                       uint32_t depc, void *data,
                                       unsigned int size) {
  if (route == -1)
    route = artsGlobalRankId;
  void *ptr = artsMalloc(size);
  memcpy(ptr, data, size);
  artsGuid_t guid = artsEdtCreate(funcPtr, route, paramc, paramv, depc + 1);
  artsSignalEdtPtr(guid, 0, ptr, size);
  return guid;
}

artsGuid_t artsAllocateLocalBuffer(void **buffer, unsigned int size,
                                   unsigned int uses, artsGuid_t epochGuid) {
  if (epochGuid)
    incrementActiveEpoch(epochGuid);
  globalShutdownGuidIncActive();

  // unsigned int alloc = 0;
  if (size) {
    if (*buffer == NULL) {
      *buffer = (char *)artsMalloc(sizeof(char) * size);
      // alloc = 1;
    }
  }

  artsBuffer_t *stub = (artsBuffer_t *)artsMalloc(sizeof(artsBuffer_t));
  stub->buffer = (buffer) ? *buffer : NULL;
  stub->sizeToWrite = NULL;
  stub->size = size;
  stub->uses = uses;
  stub->epochGuid = epochGuid;

  artsGuid_t guid = artsGuidCreateForRank(artsGlobalRankId, ARTS_BUFFER);
  artsRouteTableAddItem(stub, guid, artsGlobalRankId, false);
  return guid;
}

void *artsSetBuffer(artsGuid_t bufferGuid, void *buffer, unsigned int size) {
  void *ret = NULL;
  unsigned int rank = artsGuidGetRank(bufferGuid);
  if (rank == artsGlobalRankId) {
    artsBuffer_t *stub = (artsBuffer_t *)artsRouteTableLookupItem(bufferGuid);
    if (stub) {
      artsGuid_t epochGuid = stub->epochGuid;
      if (epochGuid)
        incrementQueueEpoch(epochGuid);
      globalShutdownGuidIncQueue();

      if (size > stub->size) {
        if (stub->size) {
          ARTS_INFO("Truncating buffer data buffer size: %u stub size: %u",
                    size, stub->size);
          artsDebugPrintStack();
        } else if (stub->buffer == NULL) {
          stub->buffer = (char *)artsMalloc(sizeof(char) * size);
          stub->size = size;
        } else
          stub->size = size;
      }

      if (stub->sizeToWrite)
        *stub->sizeToWrite = (uint32_t)size;

      if (stub->buffer) {
        memcpy(stub->buffer, buffer, stub->size);
        ARTS_DEBUG("Set buffer [Ptr: %p, Size: %u, Uses: %u]", stub->buffer,
                   *((unsigned int *)stub->buffer), stub->size);
        ret = stub->buffer;
      } else
        ret = NULL;

      if (!artsAtomicSub(&stub->uses, 1)) {
        artsRouteTableRemoveItem(bufferGuid);
        artsFree(stub);
      }

      if (epochGuid)
        incrementFinishedEpoch(epochGuid);
      globalShutdownGuidIncFinished();
    } else {
      ARTS_INFO("Out-of-order buffers not supported");
    }
  } else {
    artsRemoteMemoryMove(rank, bufferGuid, buffer, size,
                         ARTS_REMOTE_BUFFER_SEND_MSG, artsFree);
  }
  return ret;
}

void *artsGetBuffer(artsGuid_t bufferGuid) {
  void *buffer = NULL;
  if (artsIsGuidLocal(bufferGuid)) {
    artsBuffer_t *stub = (artsBuffer_t *)artsRouteTableLookupItem(bufferGuid);
    buffer = stub->buffer;
    if (!artsAtomicSub(&stub->uses, 1)) {
      artsRouteTableRemoveItem(bufferGuid);
      artsFree(stub);
    }
  }
  return buffer;
}

void *artsBlockForBuffer(artsGuid_t bufferGuid) {
  void *buffer = NULL;
  if (artsIsGuidLocal(bufferGuid)) {
    artsBuffer_t *stub = (artsBuffer_t *)artsRouteTableLookupItem(bufferGuid);
    while (stub->uses > 1) {
      ARTS_DEBUG("Yield: [Uses: %u]", stub->uses);
      artsYield();
    }
    buffer = stub->buffer;
    if (!artsAtomicSub(&stub->uses, 1)) {
      artsRouteTableRemoveItem(bufferGuid);
      artsFree(stub);
    }
  }
  return buffer;
}

volatile uint64_t outstandingEdts = 0;
void checkOutEdts(uint64_t threashold) {
  static uint64_t count = 0;
  if (artsAtomicFetchAddU64(&count, 1) + 1 == threashold) {
    artsAtomicFetchSubU64(&count, threashold);
  }
}

void artsLCSync(artsGuid_t edtGuid, uint32_t slot, artsGuid_t dataGuid) {
  artsType_t mode = artsGuidGetType(dataGuid);
  if (mode == ARTS_DB_LC)
    mode = ARTS_DB_LC_SYNC;
  internalSignalEdt(edtGuid, slot, dataGuid, ARTS_DB_LC_SYNC, NULL, 0);
}

void artsGpuSignalEdtMemset(artsGuid_t edtGuid, uint32_t slot,
                            artsGuid_t dataGuid) {
  artsType_t mode = artsGuidGetType(dataGuid);
  if (mode == ARTS_DB_GPU_WRITE)
    mode = ARTS_DB_GPU_MEMSET;
  else if (mode == ARTS_DB_LC)
    mode = ARTS_DB_LC_NO_COPY;
  internalSignalEdt(edtGuid, slot, dataGuid, mode, NULL, 0);
}
