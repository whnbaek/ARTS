set(compute_src
    artsEdtFunctions.c
    shadAdapter.c
)

add_library(runtime_compute OBJECT ${compute_src}) 