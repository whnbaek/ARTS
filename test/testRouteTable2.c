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

artsGuid_t shutdownGuid;
artsGuid_t edtGuid;
artsGuid_t dbGuid;

void shutdownEdt(uint32_t paramc, uint64_t * paramv, uint32_t depc, artsEdtDep_t depv[])
{
    artsShutdown();
}

void acquireTest(uint32_t paramc, uint64_t * paramv, uint32_t depc, artsEdtDep_t depv[])
{
    unsigned int * num = depv[0].ptr;
    PRINTF("%u %u i: %u %u\n", artsGetCurrentNode(), artsGetCurrentWorker(), 0, *num);
    artsSignalEdtValue(shutdownGuid, 0, 0);
}

void initPerNode(unsigned int nodeId, int argc, char** argv)
{
    edtGuid = artsReserveGuidRoute(ARTS_EDT, 0);
    shutdownGuid = artsReserveGuidRoute(ARTS_EDT, 0);
    dbGuid = artsReserveGuidRoute(ARTS_DB_READ, 1);
}

void initPerWorker(unsigned int nodeId, unsigned int workerId, int argc, char** argv)
{   
    if(!workerId)
    {
        if(nodeId)
        {
            unsigned int * ptr = artsDbCreateWithGuid(dbGuid, sizeof(unsigned int));
            *ptr = 999;
            artsSignalEdt(edtGuid, 0, dbGuid); 
        }
        else
        {
            artsEdtCreateWithGuid(shutdownEdt, shutdownGuid, 0, NULL, 1);
            artsEdtCreateWithGuid(acquireTest, edtGuid, 0, NULL, 1);    
        }
    }
}

int main(int argc, char** argv)
{
    artsRT(argc, argv);
    return 0;
}

