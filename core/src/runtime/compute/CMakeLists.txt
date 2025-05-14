file(GLOB compute_src "*.c")
add_library(runtime_compute OBJECT ${compute_src}) 