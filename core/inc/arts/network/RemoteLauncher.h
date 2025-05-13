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
#ifndef ARTSREMOTELAUNCHER_H
#define ARTSREMOTELAUNCHER_H
#ifdef __cplusplus
extern "C" {
#endif
#include "arts/system/Config.h"

struct artsRemoteLauncher {
  unsigned int argc;
  char **argv;
  struct artsConfig *config;
  unsigned int killStuckProcesses;
  void (*launchProcesses)(struct artsRemoteLauncher *);
  void (*cleanupProcesses)(struct artsRemoteLauncher *);
  void *launcherMemory;
};

// Add your launcher prototypes here
void artsRemoteLauncherSSHStartupProcesses(struct artsRemoteLauncher *launcher);
void artsRemoteLauncherSSHCleanupProcesses(struct artsRemoteLauncher *launcher);

static inline struct artsRemoteLauncher *artsRemoteLauncherCreate(
    unsigned int argc, char **argv, struct artsConfig *config,
    unsigned int killMode, void (*launchProcesses)(struct artsRemoteLauncher *),
    void (*cleanupProcesses)(struct artsRemoteLauncher *)) {
  struct artsRemoteLauncher *launcher = (struct artsRemoteLauncher *)artsMalloc(
      sizeof(struct artsRemoteLauncher));

  launcher->argc = argc;
  launcher->argv = argv;
  launcher->config = config;
  launcher->killStuckProcesses = killMode;
  launcher->launcherMemory = NULL;
  launcher->launchProcesses = launchProcesses;
  launcher->cleanupProcesses = cleanupProcesses;

  return launcher;
}
#ifdef __cplusplus
}
#endif

#endif
