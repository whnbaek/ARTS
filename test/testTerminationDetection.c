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
#include "arts/arts.h"
#include "arts/utils/Atomics.h"
#include <stdio.h>
#include <stdlib.h>

unsigned int counter = 0;
unsigned int numDummy = 0;
artsGuid_t exitGuid = NULL_GUID;

void dummytask(uint32_t paramc, uint64_t *paramv, uint32_t depc,
               artsEdtDep_t depv[]) {
  artsAtomicAdd(&counter, 1);
}

void exitProgram(uint32_t paramc, uint64_t *paramv, uint32_t depc,
                 artsEdtDep_t depv[]) {
  unsigned int numNodes = artsGetTotalNodes();
  for (unsigned int i = 0; i < depc; i++) {
    unsigned int numEdts = depv[i].guid;
    if (numEdts != numNodes * numDummy + 2)
      PRINTF("Error: %u vs %u\n", numEdts, numNodes * numDummy + 2);
  }
  PRINTF("Exit %u\n", counter);
  artsShutdown();
}

void rootTask(uint32_t paramc, uint64_t *paramv, uint32_t depc,
              artsEdtDep_t depv[]) {
  artsGuid_t guid = artsGetCurrentEpochGuid();
  PRINTF("Starting %lu %u\n", guid, artsGuidGetRank(guid));
  unsigned int numNodes = artsGetTotalNodes();
  for (unsigned int rank = 0; rank < numNodes * numDummy; rank++)
    artsEdtCreate(dummytask, rank % numNodes, 0, 0, 0);
}

void initPerNode(unsigned int nodeId, int argc, char **argv) {
  numDummy = (unsigned int)atoi(argv[1]);
  exitGuid = artsReserveGuidRoute(ARTS_EDT, 0);
}

void initPerWorker(unsigned int nodeId, unsigned int workerId, int argc,
                   char **argv) {
  if (!nodeId && !workerId)
    artsEdtCreateWithGuid(exitProgram, exitGuid, 0, NULL, artsGetTotalNodes());

  if (!workerId) {
    artsInitializeAndStartEpoch(exitGuid, nodeId);
    artsGuid_t startGuid = artsEdtCreate(rootTask, nodeId, 0, NULL, 0);
  }
}

int main(int argc, char **argv) {
  artsRT(argc, argv);
  return 0;
}
