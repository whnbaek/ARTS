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
#include "arts/gas/Guid.h"
#include "arts/network/Remote.h"
#include "arts/network/RemoteProtocol.h"
#include "arts/network/Server.h"
#include "arts/runtime/Globals.h"
#include "arts/runtime/Runtime.h"
#include "arts/system/Config.h"
#include "arts/utils/Atomics.h"
#include "arts/utils/Deque.h"

#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "arpa/inet.h"
#include "arts/gas/RouteTable.h"
#include "arts/introspection/Counter.h"
#include "arts/introspection/Introspection.h"
#include "arts/network/Connection.h"
#include "arts/network/RemoteProtocol.h"
#include "arts/network/SocketServer.h"
#include "arts/runtime/compute/EdtFunctions.h"
#include "arts/runtime/network/RemoteFunctions.h"
#include "arts/system/Debug.h"
#include "errno.h"
#include "net/if.h"
#include "netdb.h"
#include "netinet/in.h"
#include "sys/ioctl.h"
#include "sys/types.h"
#include <ifaddrs.h>
#include <inttypes.h>
#include <unistd.h>
// #include <linux/if_packet.h>
// #include <linux/if_arp.h>


struct artsConfig *artsGlobalMessageTable;
unsigned int ports;
// SOCKETS!
int *remoteSocketSendList;
volatile unsigned int *volatile remoteSocketSendLockList;
struct sockaddr_in *remoteServerSendList;
bool *remoteConnectionAlive;

int *localSocketRecieve;
int *remoteSocketRecieveList;
fd_set readSet;
int maxFD;
struct sockaddr_in *remoteServerRecieveList;
struct pollfd *pollIncoming;

#define EDT_MUG_SIZE 32
#define PACKET_SIZE 4194304
#define INITIAL_OUT_SIZE 80000000

char *ipList;

void artsRemoteSetMessageTable(struct artsConfig *table) {
  artsGlobalMessageTable = table;
  ports = table->ports;
}

bool hostnameToIp(char *hostName, char *ip) {
  int j;
  struct hostent *he;
  struct in_addr **addr_list;
  struct addrinfo *result;
  int error = getaddrinfo(hostName, NULL, NULL, &result);
  if (error == 0) {

    if (result->ai_addr->sa_family == AF_INET) {
      struct sockaddr_in *res = (struct sockaddr_in *)result->ai_addr;
      inet_ntop(AF_INET, &res->sin_addr, ip, 100);
    } else if (result->ai_addr->sa_family == AF_INET6) {
      struct sockaddr_in6 *res = (struct sockaddr_in6 *)result->ai_addr;
      inet_ntop(AF_INET6, &res->sin6_addr, ip, 100);
    }
    freeaddrinfo(result);
    return true;
  }
  ARTS_INFO("%s", gai_strerror(error));

  return false;
}

void artsRemoteFixNames(char *fix, unsigned int fixLength, bool isItPost,
                        char **fixMe) {
  char *oldStr, *newStr;
  int oldStrLength;
  // for(int i=0; i<artsGlobalMessageTable->tableLength; i++)
  {
    oldStr = *fixMe; // artsGlobalMessageTable->table[i].ipAddress;
    oldStrLength = strlen(oldStr);

    newStr = artsMalloc(oldStrLength + fixLength + 1);

    if (isItPost) {
      strncpy(newStr, oldStr, oldStrLength);
      strncpy(newStr + oldStrLength, fix, fixLength);
      *(newStr + fixLength + oldStrLength) = '\0';
      *fixMe = newStr;
      artsFree(oldStr);
    } else {
      strncpy(newStr, fix, fixLength);
      strncpy(newStr + fixLength, oldStr, oldStrLength);
      *(newStr + fixLength + oldStrLength) = '\0';
      // artsGlobalMessageTable->table[i].ipAddress = newStr;
      *fixMe = newStr;
      artsFree(oldStr);
    }
  }
}

void artsServerFixIbNames(struct artsConfig *config) {
  char post[6][10] = {"-ib\0", "ib\0", ".ib\0", "-ib.ib\0", ".ibnet\0", "\0"};
  char pre[4][10] = {"ib-\0", "ib\0", "ib.\0", "\0"};

  int curLength;
  for (int j = 0; j < config->tableLength; j++) {
    char *testStr = artsGlobalMessageTable->table[j].ipAddress;
    int testStrLength = strlen(testStr);
    char *stringFixed = artsMalloc(testStrLength + 50);
    struct addrinfo *result;
    bool found = false;
    int i = 0, error;
    while (pre[i][0] != '\0' && !found) {
      curLength = strlen(pre[i]);
      strncpy(stringFixed, pre[i], curLength);
      strncpy(stringFixed + curLength, testStr, testStrLength);
      *(stringFixed + curLength + testStrLength) = '\0';
      error = getaddrinfo(stringFixed, NULL, NULL, &result);

      if (error == 0) {
        ARTS_DEBUG("%s", stringFixed);
        artsRemoteFixNames(pre[i], curLength, false,
                           &artsGlobalMessageTable->table[j].ipAddress);
        artsFree(stringFixed);
        freeaddrinfo(result);
        found = true;
      }
      i++;
    }

    i = 0;
    while (post[i][0] != '\0' && !found) {
      curLength = strlen(post[i]);
      strncpy(stringFixed, testStr, testStrLength);
      strncpy(stringFixed + testStrLength, post[i], curLength);
      *(stringFixed + curLength + testStrLength) = '\0';
      error = getaddrinfo(stringFixed, NULL, NULL, &result);
      if (error == 0) {
        ARTS_DEBUG("%s", stringFixed);
        artsRemoteFixNames(post[i], curLength, true,
                           &artsGlobalMessageTable->table[j].ipAddress);
        artsFree(stringFixed);
        freeaddrinfo(result);
        found = true;
      }
      i++;
    }
  }
}

bool artsServerSetIP(struct artsConfig *config) {
  ipList = artsMalloc(100 * sizeof(char) * config->tableLength);
  bool result;
  for (int i = 0; i < config->tableLength; i++) {
    result = hostnameToIp(config->table[i].ipAddress, ipList + 100 * i);
    // result = hostnameToIp("www.google.com", ipList+100*i);

    if (!result) {
      ARTS_INFO("Cannot get ip address for '%s'", config->table[i].ipAddress);
      exit(1);
    }
  }

  int fd;
  struct ifreq ifr;
  char *connection = NULL;
  ifr.ifr_addr.sa_family = AF_INET;

  bool found = false;
  // if(config->netInterface == NULL)
  {
    struct ifaddrs *ifap, *ifa;
    struct sockaddr_in *sa;
    struct sockaddr_in6 *sa6;
    char addr[100];

    getifaddrs(&ifap);
    for (ifa = ifap; ifa && !found; ifa = ifa->ifa_next) {
      if (ifa->ifa_addr->sa_family == AF_INET) {
        sa = (struct sockaddr_in *)ifa->ifa_addr;
        inet_ntop(AF_INET, &sa->sin_addr, addr, 100);
        ARTS_DEBUG("Interface: %s\tAddress: %s", ifa->ifa_name, addr);

        for (int i = 0; i < config->tableLength && !found; i++) {
          if (strcmp(addr, ipList + 100 * i) == 0) {
            found = true;
            config->myRank = i;
            artsGlobalRankId = i;
            artsGlobalRankCount = artsGlobalMessageTable->tableLength;
          }
        }
      } else if (ifa->ifa_addr->sa_family == AF_INET6) {
        sa6 = (struct sockaddr_in6 *)ifa->ifa_addr;
        inet_ntop(AF_INET6, &sa6->sin6_addr, addr, 100);
        ARTS_DEBUG("Interface: %s\tAddress: %s", ifa->ifa_name, addr);

        for (int i = 0; i < config->tableLength && !found; i++) {
          if (strcmp(addr, ipList + 100 * i) == 0) {
            found = true;
            config->myRank = i;
            artsGlobalRankId = i;
            artsGlobalRankCount = artsGlobalMessageTable->tableLength;
          }
        }
      }
    }
  }
  return found;
}

void artsLLServerSetup(struct artsConfig *config) {
  artsRemoteSetMessageTable(config);
#if defined(USE_TCP)
  if (config->table && config->ibNames)
    artsServerFixIbNames(config);
#else
  if (config->table)
    artsServerFixIbNames(config);
#endif

  if (!artsServerSetIP(config) && config->nodes > 1) {
    // ARTS_INFO("[%d]Could not connect to %s", artsGlobalRankId,
    // config->netInterface);
    ARTS_INFO("Could not resolve ip to any device");
    exit(1);
  }
}

void artsLLServerShutdown() {
  int count = artsGlobalMessageTable->tableLength;
  for (int i = 0; i < (count - 1) * ports; i++) {
    rshutdown(remoteSocketRecieveList[i], SHUT_RDWR);
    // rclose(remoteSocketRecieveList[i]);
  }

  for (int i = 0; i < count * ports; i++) {
    if (i / ports != artsGlobalRankId) {
      rshutdown(remoteSocketSendList[i], SHUT_RDWR);
      //            rclose(remoteSocketSendList[i]);
    }
  }
}

unsigned int artsRemoteGetMyRank() {
  ARTS_DEBUG("My rank %d", artsGlobalMessageTable->myRank);
  return artsGlobalMessageTable->myRank;
}

static inline bool artsRemoteConnect(int rank, unsigned int port) {

  ARTS_DEBUG("connecy try %d", rank);
  // sleep(10);
  if (!remoteConnectionAlive[rank * ports + port]) {
    ARTS_DEBUG("connecy %d %d", rank, remoteSocketSendList[rank * ports + port]);
    artsPrintSocketAddr(&remoteServerSendList[rank * ports + port]);
    int res = rconnect(
        remoteSocketSendList[rank * ports + port],
        (struct sockaddr *)(remoteServerSendList + rank * ports + port),
        sizeof(struct sockaddr_in));
    if (res < 0) {
      // if(artsGlobalRankId==0)
      //     artsDebugGenerateSegFault();
      void *ptrCrap;
      ARTS_DEBUG("%d error %s %d %p %d %s", rank, strerror(errno), errno,
              ptrCrap, remoteSocketSendList[rank],
              artsGlobalMessageTable->table[rank].ipAddress);
      ARTS_DEBUG("[%d]Connect Failed to rank %d %d", artsGlobalRankId, rank,
              res);

      remoteConnectionAlive[rank] = false;

      rclose(remoteSocketSendList[rank * ports + port]);
      remoteSocketSendList[rank * ports + port] = artsGetNewSocket();

      while (rconnect(remoteSocketSendList[rank * ports + port],
                      (struct sockaddr *)(remoteServerSendList + rank * ports +
                                          port),
                      sizeof(struct sockaddr_in)) < 0) {
        rclose(remoteSocketSendList[rank * ports + port]);
        remoteSocketSendList[rank * ports + port] = artsGetNewSocket();
      }

      ARTS_DEBUG("Connect now succedded to rank %d %d", rank, res);
      remoteConnectionAlive[rank * ports + port] = true;

      return true;
    }

    remoteConnectionAlive[rank * ports + port] = true;
  }

  return true;
}

// inline int artsActualSend(char * message, unsigned int length, int rank, int
// port)
int artsActualSend(char *message, unsigned int length, int rank, int port) {
  int res = 0;
  int total = 0;
  while (length != 0 && res >= 0) {
    res = rsend(remoteSocketSendList[rank * ports + port], message + total,
                length, MSG_DONTWAIT);
    if (res >= 0) {
      total += res;
      length -= res;
    }
  }

  if (res < 0) {
    if (errno != EAGAIN) {
      struct artsRemotePacket *pk = (void *)message;
      ARTS_INFO("artsRemoteSendRequest %u Socket appears to be closed to rank %d: "
             " %s",
             pk->messageType, rank, strerror(errno));
      artsRuntimeStop();
      return -1;
    }
  }
  return length;
}

unsigned int artsRemoteSendRequest(int rank, unsigned int queue, char *message,
                                   unsigned int length) {
  int port = queue % ports;
  if (artsRemoteConnect(rank, port)) {
#ifdef COUNT
    // struct artsRemotePacket * pk = (void *)message;
    // if(!pk->timeStamp)
    //     pk->timeStamp = artsExtGetTimeStamp();
#endif
    return artsActualSend(message, length, rank, port);
  }
  return length;
}

unsigned int artsRemoteSendPayloadRequest(int rank, unsigned int queue,
                                          char *message, unsigned int length,
                                          char *payload, int length2) {
  int port = queue % ports;
  if (artsRemoteConnect(rank, port)) {
#ifdef COUNT
    // struct artsRemotePacket * pk = (void *)message;
    // if(!pk->timeStamp)
    //     pk->timeStamp = artsExtGetTimeStamp();
#endif
    int tempLength = artsActualSend(message, length, rank, port);
    if (tempLength)
      return tempLength + length2;

    return artsActualSend(payload, length2, rank, port);
  }
  return length + length2;
}

bool artsRemoteSetupIncoming() {
  // ARTS_INFO("%d", FD_SETSIZE);
  int i, j, k, pos;
  int inPort = artsGlobalMessageTable->port;
  socklen_t sLength = sizeof(struct sockaddr);
  int count = (artsGlobalMessageTable->tableLength - 1);

  remoteSocketRecieveList = artsMalloc(sizeof(int) * (count + 1) * ports);
  remoteServerRecieveList =
      artsCalloc(sizeof(struct sockaddr_in) * (count + 1) * ports);
  pollIncoming = artsMalloc(sizeof(struct pollfd) * (count + 1) * ports);

  struct sockaddr_in test;

  struct sockaddr_in *localServerAddr =
      artsCalloc(ports * sizeof(struct sockaddr_in));
  localSocketRecieve = artsCalloc(ports * sizeof(int));

  int iSetOption;
  for (i = 0; i < artsGlobalMessageTable->ports; i++) {
    localSocketRecieve[i] =
        artsGetSocketListening(&localServerAddr[i], inPort + i);

    iSetOption = 1;
    setsockopt(localSocketRecieve[i], SOL_SOCKET, SO_REUSEADDR,
               (char *)&iSetOption, sizeof(iSetOption));

    int res =
        rbind(localSocketRecieve[i], (struct sockaddr *)&localServerAddr[i],
              sizeof(localServerAddr[i]));

    if (res < 0) {
      ARTS_INFO("Bind Failed");
      ARTS_INFO("error %s", strerror(errno));
      return false;
    }

    res = rlisten(localSocketRecieve[i], 2 * count);

    if (res < 0) {
      ARTS_INFO("Listening Failed");
      ARTS_INFO("error %s", strerror(errno));
      return false;
    }
  }

  FD_ZERO(&readSet);
  for (i = 0; i < artsGlobalMessageTable->tableLength; i++) {
    ARTS_DEBUG("%d %d", artsGlobalMessageTable->myRank,
            artsGlobalMessageTable->table[i].rank);
    if (artsGlobalMessageTable->myRank ==
        artsGlobalMessageTable->table[i].rank) {
      ARTS_DEBUG("Receive go %d", i);
      for (j = 0; j < count; j++) {
        for (int z = 0; z < ports; z++) {
          ARTS_DEBUG("%d", j);
          sLength = sizeof(struct sockaddr_in);
          // remoteSocketRecieveList[j] = raccept(localSocketRecieve, (struct
          // sockaddr *)&remoteServerRecieveList[j], &sLength );
          remoteSocketRecieveList[z + j * ports] = raccept(
              localSocketRecieve[z], (struct sockaddr *)&test, &sLength);

          if (remoteSocketRecieveList[z + j * ports] < 0) {
            int retry = 0;
            ARTS_INFO("Accept Failed");
            ARTS_INFO("error %s", strerror(errno));
            int retryLimit = 3;
            while (remoteSocketRecieveList[z + j * ports] < 0) {
              ARTS_INFO("Retrying %d more times", retryLimit - retry);
              if (retry == retryLimit) {
                exit(1);
              }
              remoteSocketRecieveList[z + j * ports] = raccept(
                  localSocketRecieve[z], (struct sockaddr *)&test, &sLength);
              retry++;
              if (remoteSocketRecieveList[z + j * ports] < 0) {
                ARTS_INFO("Accept Failed");
                ARTS_INFO("error %s", strerror(errno));
              }
            }
          }
          // FD_SET(remoteSocketRecieveList[j] , &readSet  );
          pollIncoming[z + j * ports].fd =
              remoteSocketRecieveList[z + j * ports];
          pollIncoming[z + j * ports].events = POLLIN;
        }
      }
    } else {
      ARTS_DEBUG("Connect go %d", i);
      for (int z = 0; z < ports; z++) {
        if (!artsRemoteConnect(i, z)) {
          ARTS_INFO("Could not create initial connection");
          return false;
        }
      }
    }
  }

  return true;
}

void artsRemoteSetupOutgoing() {
  int i, j, k, outPort = artsGlobalMessageTable->port;
  struct sockaddr_in serverAddress, clientAddress;
  int count = artsGlobalMessageTable->tableLength;
  struct hostent *he;
  struct in_addr **addr_list;
  char ip[100];
  int pos;

  remoteSocketSendList = artsMalloc(sizeof(int) * count * ports);
  remoteSocketSendLockList = artsCalloc(sizeof(int) * count * ports);
  remoteServerSendList = artsCalloc(sizeof(struct sockaddr_in) * count * ports);
  remoteConnectionAlive = artsCalloc(sizeof(bool) * count * ports);

  for (i = 0; i < count; i++) {
    for (j = 0; j < ports; j++)
      remoteSocketSendList[i * ports + j] =
          artsGetSocketOutgoing(remoteServerSendList + i * ports + j,
                                outPort + j, inet_addr(ipList + 100 * i));
  }
}

static __thread unsigned int threadStart;
static __thread unsigned int threadStop;
static __thread char **bypassBuf;
static __thread unsigned int *bypassPacketSize;
static __thread unsigned int *reRecieveRes;
static __thread void **reRecievePacket;
static __thread bool *maxIncoming;
static __thread bool maxOutWorking;

void artsRemotSetThreadInboundQueues(unsigned int start, unsigned int stop) {
  threadStart = start;
  threadStop = stop;
  // ARTS_INFO_MASTER("%d %d", start, stop);
  unsigned int size = stop - start;
  bypassBuf = artsMalloc(sizeof(char *) * size);
  bypassPacketSize = artsMalloc(sizeof(unsigned int) * size);
  reRecieveRes = artsCalloc(sizeof(int) * size);
  reRecievePacket = artsCalloc(sizeof(void *) * size);
  maxIncoming = artsCalloc(sizeof(bool) * size);
  for (int i = 0; i < size; i++) {
    bypassBuf[i] = artsMalloc(PACKET_SIZE);
    bypassPacketSize[i] = PACKET_SIZE;
  }
}

bool maxOutBuffs(unsigned int ignore) {
  int timeOut = 1, res, res2;
  struct artsRemotePacket *packet;
  // ARTS_INFO("MAX");
  res = rpoll(pollIncoming + threadStart, threadStop - threadStart, timeOut);
  unsigned int pos;

  if (res == -1) {
    artsShutdown();
    artsRuntimeStop();
  }
  if (res > 0) {
    // ARTS_INFO("MAX LOOP");
    timeOut = 1;
    for (int i = threadStart; i < threadStop; i++) {
      pos = i - threadStart;
      if (i != ignore && pollIncoming[i].revents & POLLIN) {
        maxOutWorking = true;
        if (reRecieveRes[pos] == 0) {
          packet = (struct artsRemotePacket *)bypassBuf[pos];
          res = rrecv(remoteSocketRecieveList[i], bypassBuf[pos],
                      bypassPacketSize[pos], 0);
        } else {
          // packet = reRecievePacket[pos];
          packet = (struct artsRemotePacket *)bypassBuf[pos];
          res = reRecieveRes[pos];
          reRecieveRes[pos] = 0;
          // if(packet->size > 5000000)
          //     artsDebugGenerateSegFault();
          // ARTS_INFO("Here res %p %d %d", packet, res, pos);
        }
        // spaceLeft = bypassPacketSize[pos];
        if (res > 0) {
          ARTS_DEBUG("gg %d %d", res, packet->rank);
          // spaceLeft-=res;
          while (res < bypassPacketSize[pos]) {
            ARTS_DEBUG("POS Buffffff %d %d", res, packet->size);
            if (bypassBuf[pos] != (char *)packet) {
              ARTS_DEBUG("memmove");
              memmove(bypassBuf[pos], packet, res);
              packet = (struct artsRemotePacket *)bypassBuf[pos];
              // spaceLeft = bypassPacketSize[pos];
            }
            res2 = rrecv(remoteSocketRecieveList[i], bypassBuf[pos] + res,
                         bypassPacketSize[pos] - res, MSG_DONTWAIT);

            ARTS_DEBUG("res %d %d", res, res2);
            if (res2 < 0) {
              if (errno != EAGAIN) {
                ARTS_INFO("Error on recv return 0 %d %d", errno, EAGAIN);
                ARTS_INFO("error %s", strerror(errno));
                artsShutdown();
                artsRuntimeStop();
              }

              reRecieveRes[pos] = res;
              maxIncoming[pos] = true;
              // ARTS_INFO("Here");
              // reRecievePacket[pos] = packet;
              break;
            }
            // spaceLeft-=res2;
            res += res2;
          }
          maxIncoming[pos] = true;
          reRecieveRes[pos] = res;
        } else if (res == -1) {
          ARTS_INFO("Error on recv socket return 0");
          ARTS_INFO("error %s", strerror(errno));
          artsShutdown();
          artsRuntimeStop();
          return false;
        } else if (res == 0) {
          // ARTS_INFO("Hmm socket close?");
          artsShutdown();
          artsRuntimeStop();
          return false;
        }
      }
    }
  }
  return true;
}

bool artsServerTryToRecieve(char **inBuffer, int *inPacketSize,
                            volatile unsigned int *remoteStealLock) {
  int i, res, res2, stealHandlerThread = 0;
  struct artsRemotePacket *packet;
  int count = artsGlobalMessageTable->tableLength - 1;
  fd_set tempSet;
  int timeOut = 300000;
  // int timeOut=300000;
  struct timeval selTimeout;
  unsigned int pos;
  res = rpoll(pollIncoming + threadStart, threadStop - threadStart, timeOut);

  if (res == -1) {
    artsShutdown();
    artsRuntimeStop();
  }

  unsigned int spaceLeft;
  bool packetIncomingOnASocket = false;
  bool gotoNext = false;
  if (res > 0) {
    // ARTS_INFO("POLL");
    timeOut = 1;
    maxOutWorking = true;
    while (maxOutWorking) {
      maxOutWorking = false;
      for (i = threadStart; i < threadStop; i++) {
        pos = i - threadStart;
        gotoNext = false;
        // if( pollIncoming[i].revents & POLLIN )
        // if(!maxOutBuffs(-1))
        //     return false;
        // if( maxIncoming[pos] )
        if (pollIncoming[i].revents & POLLIN) {
          // ARTS_INFO("Here2");
          maxIncoming[pos] = false;
          if (reRecieveRes[pos] == 0) {
            // ARTS_INFO("Here3a");
            packet = (struct artsRemotePacket *)bypassBuf[pos];
            res = rrecv(remoteSocketRecieveList[i], bypassBuf[pos],
                        bypassPacketSize[pos], 0);
          } else {
            // packet = reRecievePacket[pos];
            packet = (struct artsRemotePacket *)bypassBuf[pos];
            res = reRecieveRes[pos];
            reRecieveRes[pos] = 0;
            // ARTS_INFO("Here3");
            // if(packet->size > 5000000)
            //     artsDebugGenerateSegFault();
            // ARTS_INFO("Here res %p %d %d", packet, res, pos);
          }
          // spaceLeft = bypassPacketSize[pos];
          if (res > 0) {
            packetIncomingOnASocket = true;
            ARTS_DEBUG("gg %d %d", res, packet->rank);
            // spaceLeft-=res;
            while (res > 0) {
              // if(!maxOutBuffs(i))
              //     return false;
              while (res < sizeof(struct artsRemotePacket)) {
                // ARTS_INFO("Here4");
                ARTS_DEBUG("POS Buffffff %d %d", res, packet->size);
                if (bypassBuf[pos] != (char *)packet) {
                  ARTS_DEBUG("memmove");
                  memmove(bypassBuf[pos], packet, res);
                  packet = (struct artsRemotePacket *)bypassBuf[pos];
                  // spaceLeft = bypassPacketSize[pos];
                }
                res2 = rrecv(remoteSocketRecieveList[i], bypassBuf[pos] + res,
                             bypassPacketSize[pos] - res, 0);

                ARTS_DEBUG("res %d %d", res, res2);
                if (res2 < 0) {
                  if (errno != EAGAIN) {
                    ARTS_INFO("Error on recv return 0 %d %d", errno, EAGAIN);
                    ARTS_INFO("error %s", strerror(errno));
                    artsShutdown();
                    artsRuntimeStop();
                  }

                  reRecieveRes[pos] = res;
                  // reRecievePacket[pos] = packet;
                  gotoNext = true;
                  break;
                  // return false;
                }
                // spaceLeft-=res2;
                res += res2;
              }
              if (gotoNext)
                break;

              ARTS_DEBUG("gg2 %d %d %d %d", res, packet->rank, packet->size,
                      packet->messageType);

              if (bypassPacketSize[pos] < packet->size) {
                // ARTS_INFO("Here5");
                char *nextBuf = artsMalloc(packet->size * 4);

                memcpy(nextBuf, bypassBuf[pos], bypassPacketSize[pos]);

                artsFree(bypassBuf[pos]);

                packet =
                    (struct artsRemotePacket *)(nextBuf +
                                                (((char *)packet) -
                                                 ((char *)bypassBuf[pos])));

                //*inBuffer = buf = nextBuf;
                bypassBuf[pos] = nextBuf;

                //(*inPacketSize) = packetSize = packet->size;
                bypassPacketSize[pos] = packet->size * 4;
                // spaceLeft = bypassPacketSize[pos];
              }

              while (res < packet->size) {
                // ARTS_INFO("Here6");
                ARTS_DEBUG("POS Buffffff a %d %d", res, packet->size);
                // spaceLeft = (bypassPacketSize[pos] - (((char *)packet) -
                // bypassBuf[pos])) - res; ARTS_INFO("%d %d %d %d", spaceLeft,
                // ((char *)packet) - bypassBuf[pos], res, packet->size);
                // if(bypassBuf[pos]!=(char*)packet && (packet->size-res) >
                // spaceLeft )
                if (bypassBuf[pos] != (char *)packet) {
                  ARTS_DEBUG("memmove fix");
                  memmove(bypassBuf[pos], packet, res);
                  packet = (struct artsRemotePacket *)bypassBuf[pos];
                  // spaceLeft = bypassPacketSize[pos];
                }
                res2 = rrecv(remoteSocketRecieveList[i], bypassBuf[pos] + res,
                             bypassPacketSize[pos] - res, 0);
                // res2 = rrecv( remoteSocketRecieveList[i], bypassBuf[pos]+res,
                // bypassPacketSize[pos]-res, MSG_WAITALL ); res2 = rrecv(
                // remoteSocketRecieveList[i], bypassBuf[pos]+res,
                // packet->size-res, MSG_WAITALL ); res2 = rrecv(
                // remoteSocketRecieveList[i], ((char *)packet)+res, spaceLeft,
                // 0 );
                if (res2 < 0) {
                  if (errno != EAGAIN) {
                    ARTS_INFO("Error on recv return 0 %d %d", errno, EAGAIN);
                    ARTS_INFO("error %s", strerror(errno));
                    artsShutdown();
                    artsRuntimeStop();
                  }

                  // ARTS_INFO("Here %p %d %d", packet, res, pos);

                  reRecieveRes[pos] = res;
                  // reRecievePacket[pos] = packet;
                  gotoNext = true;
                  break;
                  // return false;
                }
                // spaceLeft-=res2;
                res += res2;
                ARTS_DEBUG("res %d %d", res, res2);
              }
              if (gotoNext)
                break;

              artsServerProcessPacket(packet);

              res -= packet->size;
              // ARTS_INFO("Here 8 %d", res);
              ARTS_DEBUG("PACKET move %d %d", res, packet->size);

              packet =
                  (struct artsRemotePacket *)(((char *)packet) + packet->size);
              // memmove(bypassBuf[pos], packet, res);
              // packet = (struct artsRemotePacket *)bypassBuf[pos];
              // reRecieveRes[pos] = res;
              // break;
            }
          } else if (res == -1) {
            ARTS_INFO("Error on recv socket return 0");
            ARTS_INFO("error %s", strerror(errno));
            artsShutdown();
            artsRuntimeStop();
            return false;
          } else if (res == 0) {
            // ARTS_INFO("Hmm socket close?");
            artsShutdown();
            artsRuntimeStop();
            return false;
          }
        }
      }
    }
    return packetIncomingOnASocket;
  }
  return false;
}

void artsServerPingPongTestRecieve(char *inBuffer, int inPacketSize) {
  int packetSize = inPacketSize;
  char *buf = inBuffer;
  int i, res, res2, stealHandlerThread = 0, pos;
  struct artsRemotePacket *packet = (struct artsRemotePacket *)buf;
  int count = artsGlobalMessageTable->tableLength - 1;
  fd_set tempSet;
  int timeOut = 100;
  struct timeval selTimeout;
  tempSet = readSet;
  selTimeout.tv_sec = 10;
  selTimeout.tv_usec = timeOut;
  bool recieved = false;

  while (!recieved) {
    res = rpoll(pollIncoming, count, timeOut);
    timeOut = 1;
    // if(res)
    for (i = 0; i < count; i++) {
      if (pollIncoming[i].revents & POLLIN) {
        packet = (struct artsRemotePacket *)buf;
        res = rrecv(remoteSocketRecieveList[i], buf, packetSize, 0);
        if (res > 0) {
          while (res > 0) {
            while (res < sizeof(struct artsRemotePacket)) {
              if (buf != (char *)packet) {
                memmove(buf, packet, res);
                packet = (struct artsRemotePacket *)buf;
              }
              res2 = rrecv(remoteSocketRecieveList[i], buf + res,
                           packetSize - res, 0);
              res += res2;
              if (res2 == -1) {
                ARTS_INFO("Error on recv return 0");
                ARTS_INFO("error %s", strerror(errno));
                artsShutdown();
                return;
              }
            }

            // ARTS_INFO("Here");
            while (res < packet->size) {
              // ARTS_INFO("Here %d %d", res, packet->size);

              if (buf != (char *)packet) {
                memmove(buf, packet, res);
                packet = (struct artsRemotePacket *)buf;
              }
              res2 = rrecv(remoteSocketRecieveList[i], buf + res,
                           packetSize - res, 0);
              res += res2;
              if (res2 == -1) {
                ARTS_INFO("Error on recv return 0");
                ARTS_INFO("error %s", strerror(errno));
                artsShutdown();
                return;
              }
            }
            if (packet->messageType == ARTS_REMOTE_PINGPONG_TEST_MSG) {
              recieved = true;
              artsUpdatePerformanceMetric(artsNetworkRecieveBW, artsThread,
                                          packet->size, false);
              artsUpdatePerformanceMetric(artsFreeBW + packet->messageType,
                                          artsThread, 1, false);
              artsUpdatePacketInfo(packet->size);
              // ARTS_INFO("Recv Packet %d %d", res, packet->size);
            } else {
              ARTS_INFO("Shit Packet %d %d %d", packet->messageType,
                     packet->size, packet->rank);
            }
            res -= packet->size;
            packet =
                (struct artsRemotePacket *)(((char *)packet) + packet->size);
          }
        } else if (res == -1) {
          ARTS_INFO("Error on recv socket return 0");
          ARTS_INFO("error %s", strerror(errno));
          artsShutdown();
          return;
        } else if (res == 0) {
          ARTS_INFO("Hmm socket close?");
          artsShutdown();
          return;
        }
      }
    }
  }
}
