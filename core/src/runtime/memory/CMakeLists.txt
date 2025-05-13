set(memory_src
    artsDbFunctions.c
    artsDbList.c
    artsArrayDb.c
    # artsSmartDbFunctions.c
)

add_library(runtime_memory OBJECT ${memory_src}) 