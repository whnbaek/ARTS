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
#include "arts/Graph.h"
#include "arts/arts.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>

void initPerNode(unsigned int nodeId, int argc, char **argv) {
  // Simple Graph, vertices = 8, edges = 11
  /**
   0 6
  1 2
  2 5
  2 3
  2 4
  1 6
  1 3
  1 7
  1 4
  3 5
  1 5
  **/

  int edge_arr[] = {5, 6, 1, 2, 2, 5, 2, 3, 2, 4, 1,
                    6, 1, 3, 1, 7, 1, 4, 3, 5, 1, 5};

  // Create a block distribution
  arts_block_dist_t *dist = initBlockDistributionBlock(8,  // global vertices
                                                       11, // global edges
                                                       1,  // partitions
                                                       ARTS_DB_PIN);

  // Create a list of edges, use artsEdgeVector
  artsEdgeVector vec;
  initEdgeVector(&vec, 100);
  for (int i = 0; i < 11; ++i)
    pushBackEdge(&vec, edge_arr[i * 2], edge_arr[(i * 2) + 1], 0);
  sortBySourceAndTarget(&vec);

  // Create the CSR graph, graphGuid is used to allocate
  // row indices and column array
  csr_graph_t *graph = initCSR(0,
                               8,    // number of "local" vertices
                               11,   // number of "local" edges
                               dist, // distribution
                               &vec, // edges
                               true, /*are edges sorted ?*/
                               getGuidForPartitionDistr(dist, 0));

  // Edge list not needed after creating the CSR
  freeEdgeVector(&vec);

  printCSR(graph);

  vertex_t *neighbors = NULL;
  graph_sz_t nbrcnt = 0;
  getNeighbors(graph, (vertex_t)1, &neighbors, &nbrcnt);
  assert(nbrcnt == 6);

  PRINTF("Neighbors of 1 : {");
  for (graph_sz_t i = 0; i < nbrcnt; ++i) {
    printf("%" PRIu64 ", ", neighbors[i]);
  }
  printf("}\n");
  freeCSR(graph);

  // Testing -- reading from commandline
  // e.g., srun -N 1 ./testCSR --file /Users/kane972/Downloads/ca-HepTh.tsv
  // --num-vertices 9877 --num-edges 51946 --keep-self-loops

  // arts_block_dist_t * distCmd = initBlockDistributionWithCmdLineArgs(argc,
  // argv); loadGraphUsingCmdLineArgs(&graphCmd, &distCmd, argc, argv);
  // printLocalCSR(artsGetCurrentNode(), &graphCmd);
  // freeCSR(artsGetCurrentNode(), &graphCmd);
}

void initPerWorker(unsigned int nodeId, unsigned int workerId, int argc,
                   char **argv) {
  if (!nodeId && !workerId)
    artsShutdown();
}

int main(int argc, char **argv) {
  artsRT(argc, argv);
  return 0;
}
