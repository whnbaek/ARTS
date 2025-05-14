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
#include <stdio.h>
#include <stdlib.h>
#include "arts/arts.h"

artsGuid_t dbDestGuid = NULL_GUID;
artsGuid_t shutdownGuid = NULL_GUID;
unsigned int numElements = 0;
unsigned int blockSize = 0;

void setter(uint32_t paramc, uint64_t * paramv, uint32_t depc, artsEdtDep_t depv[])
{
    
    unsigned int id = paramv[0];
    unsigned int * buffer = depv[0].ptr;
    unsigned int * dest = depv[1].ptr;
    for(unsigned int i=0; i<blockSize; i++)
    {
        dest[id*blockSize + i] = buffer[i];
    }
    PRINTF("Setter: %u\n", id);
    artsSignalEdt(shutdownGuid, id, dbDestGuid);
}

void getter(uint32_t paramc, uint64_t * paramv, uint32_t depc, artsEdtDep_t depv[])
{
    unsigned int id = paramv[0];
    unsigned int * source = depv[0].ptr;
    unsigned int * buffer = &source[id*blockSize];
    PRINTF("Getter: %u\n", id);
    //This one actually sends to a remote node... yea for testing!
    artsGuid_t am = artsActiveMessageWithBuffer(setter, artsGetTotalNodes() - 1, paramc, paramv, 1, buffer, sizeof(unsigned int)*blockSize);
    artsSignalEdt(am, 1, dbDestGuid);
}

void shutDownEdt(uint32_t paramc, uint64_t * paramv, uint32_t depc, artsEdtDep_t depv[])
{
    bool pass = true;
    unsigned int * data = depv[0].ptr;
    for(unsigned int i=0; i<numElements; i++)
    {
        if(data[i]!=i)
        {
            PRINTF("I: %u vs %u\n", i, data[i]);
            pass = false;
        }
    }
    
    if(pass)
        PRINTF("CHECK\n");
    artsShutdown();
}

void initPerNode(unsigned int nodeId, int argc, char** argv)
{
    blockSize = atoi(argv[1]);
    numElements = blockSize * artsGetTotalNodes();
    dbDestGuid = artsReserveGuidRoute(ARTS_DB_PIN, artsGetTotalNodes() - 1);
    shutdownGuid = artsReserveGuidRoute(ARTS_EDT, artsGetTotalNodes() - 1);
}

void initPerWorker(unsigned int nodeId, unsigned int workerId, int argc, char** argv)
{
    if(!workerId)
    {   
        uint64_t id = nodeId;
        unsigned int * data = artsMalloc(sizeof(unsigned int)*numElements);
        for(unsigned int i=0; i<numElements; i++)
        {
            data[i] = i;
        }
        //This is kinda dumb since it is sending to itself, but hey lets check it...
        artsActiveMessageWithBuffer(getter, nodeId, 1, &id, 0, data, sizeof(unsigned int)*numElements);
        
        if(!nodeId)
            artsEdtCreateWithGuid(shutDownEdt, shutdownGuid, 0, NULL, artsGetTotalNodes());
        
        if(nodeId == artsGetTotalNodes() - 1)
            artsDbCreateWithGuid(dbDestGuid, sizeof(unsigned int) * numElements);
    }
}


int main(int argc, char** argv)
{
    artsRT(argc, argv);
    return 0;
}
