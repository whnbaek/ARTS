###############################################################################
## This material was prepared as an account of work sponsored by an agency 
## of the United States Government.  Neither the United States Government 
## nor the United States Department of Energy, nor Battelle, nor any of 
## their employees, nor any jurisdiction or organization that has cooperated 
## in the development of these materials, makes any warranty, express or 
## implied, or assumes any legal liability or responsibility for the accuracy, 
## completeness, or usefulness or any information, apparatus, product, 
## software, or process disclosed, or represents that its use would not 
## infringe privately owned rights.
##
## Reference herein to any specific commercial product, process, or service 
## by trade name, trademark, manufacturer, or otherwise does not necessarily 
## constitute or imply its endorsement, recommendation, or favoring by the 
## United States Government or any agency thereof, or Battelle Memorial 
## Institute. The views and opinions of authors expressed herein do not 
## necessarily state or reflect those of the United States Government or 
## any agency thereof.
##
##                      PACIFIC NORTHWEST NATIONAL LABORATORY
##                                  operated by
##                                    BATTELLE
##                                    for the
##                      UNITED STATES DEPARTMENT OF ENERGY
##                         under Contract DE-AC05-76RL01830
##
## Copyright 2019 Battelle Memorial Institute
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
##
##    https://www.apache.org/licenses/LICENSE-2.0 
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
## License for the specific language governing permissions and limitations
###############################################################################
add_subdirectory(system)
add_subdirectory(utils)
add_subdirectory(network)
add_subdirectory(runtime)
add_subdirectory(gas)
add_subdirectory(gpu)
add_subdirectory(introspection)

file(GLOB main_src "*.c")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../inc/arts)

if(CUDA_FOUND)
    add_library(arts STATIC ${main_src}
        $<TARGET_OBJECTS:gas>
        $<TARGET_OBJECTS:network>
        $<TARGET_OBJECTS:introspection>
        $<TARGET_OBJECTS:runtime_main>
        $<TARGET_OBJECTS:runtime_compute>
        $<TARGET_OBJECTS:runtime_memory>
        $<TARGET_OBJECTS:runtime_sync>
        $<TARGET_OBJECTS:runtime_network>
        $<TARGET_OBJECTS:system>
        $<TARGET_OBJECTS:utils>
        $<TARGET_OBJECTS:gpu>
    )
else()
    add_library(arts SHARED ${main_src}
        $<TARGET_OBJECTS:gas>
        $<TARGET_OBJECTS:network>
        $<TARGET_OBJECTS:introspection>
        $<TARGET_OBJECTS:runtime_main>
        $<TARGET_OBJECTS:runtime_compute>
        $<TARGET_OBJECTS:runtime_memory>
        $<TARGET_OBJECTS:runtime_sync>
        $<TARGET_OBJECTS:runtime_network>
        $<TARGET_OBJECTS:system>
        $<TARGET_OBJECTS:utils>
    )
    message("PATHS INSTALLED: " ${M_LIB} ${RT_LIB} ${PTHREAD_LIB} ${RDMACM_LIB} ${HWLOC_LIB})
endif()

# Install all headers from inc/arts and its subdirectories, preserving structure
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../inc/arts/ DESTINATION include/arts FILES_MATCHING PATTERN "*.h")

install(TARGETS arts ARCHIVE DESTINATION lib)