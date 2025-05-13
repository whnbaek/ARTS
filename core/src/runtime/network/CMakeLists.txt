set(server_src
    artsServer.c
    artsRemoteFunctions.c
    artsRuntime.c
)

add_library(runtime_server OBJECT ${server_src}) 