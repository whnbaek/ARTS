set(sync_src
    artsTerminationDetection.c
    artsEventFunctions.c
)

add_library(runtime_sync OBJECT ${sync_src}) 