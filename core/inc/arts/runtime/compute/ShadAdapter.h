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
#ifndef SHADADAPTER_H
#define SHADADAPTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "arts/utils/Queue.h"

artsGuid_t artsEdtCreateShad(artsEdt_t funcPtr, unsigned int route,
                             uint32_t paramc, uint64_t *paramv);
artsGuid_t artsActiveMessageShad(artsEdt_t funcPtr, unsigned int route,
                                 uint32_t paramc, uint64_t *paramv, void *data,
                                 unsigned int size, artsGuid_t epochGuid);
void artsSynchronousActiveMessageShad(artsEdt_t funcPtr, unsigned int route,
                                      uint32_t paramc, uint64_t *paramv,
                                      void *data, unsigned int size);

void artsIncLockShad();
void artsDecLockShad();
void artsCheckLockShad();
void artsStartIntroShad(unsigned int start);
void artsStopIntroShad();
unsigned int artsGetShadLoopStride();

typedef struct {
  volatile unsigned int lock;
  volatile unsigned int size;
  artsQueue *queue;
} artsShadLock_t;
artsShadLock_t *artsShadCreateLock();
void artsShadLock(artsShadLock_t *lock);
void artsShadUnlock(artsShadLock_t *lock);

artsGuid_t artsAllocateLocalBufferShad(void **buffer, uint32_t *sizeToWrite,
                                       artsGuid_t epochGuid);

bool artsShadAliasTryLock(volatile uint64_t *lock);
void artsShadAliasUnlock(volatile uint64_t *lock);

void artsShadTMTLock(volatile uint64_t *lock);
void artsShadTMTUnlock(volatile uint64_t *lock);

#ifdef __cplusplus
}
#endif

#endif /* SHADADAPTER_H */
