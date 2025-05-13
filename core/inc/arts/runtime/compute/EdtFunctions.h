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
#ifndef ARTSEDTFUNCTIONS_H
#define ARTSEDTFUNCTIONS_H
#ifdef __cplusplus
extern "C" {
#endif
#include "arts/arts.h"

bool artsEdtCreateInternal(struct artsEdt *edt, artsType_t mode,
                           artsGuid_t *guid, unsigned int route,
                           unsigned int cluster, unsigned int edtSpace,
                           artsGuid_t eventGuid, artsEdt_t funcPtr,
                           uint32_t paramc, uint64_t *paramv, uint32_t depc,
                           bool useEpoch, artsGuid_t epochGuid, bool hasDepv);
void artsEdtDelete(struct artsEdt *edt);
void internalSignalEdt(artsGuid_t edtPacket, uint32_t slot, artsGuid_t dataGuid,
                       artsType_t mode, void *ptr, unsigned int size);

typedef struct {
  artsGuid_t currentEdtGuid;
  struct artsEdt *currentEdt;
  void *epochList;
} threadLocal_t;

void artsSetThreadLocalEdtInfo(struct artsEdt *edt);
void artsUnsetThreadLocalEdtInfo();
void artsSaveThreadLocal(threadLocal_t *tl);
void artsRestoreThreadLocal(threadLocal_t *tl);

bool artsSetCurrentEpochGuid(artsGuid_t epochGuid);
artsGuid_t *artsCheckEpochIsRoot(artsGuid_t toCheck);
void artsIncrementFinishedEpochList();

void *artsGetDepv(void *edtPtr);
#ifdef __cplusplus
}
#endif

#endif
