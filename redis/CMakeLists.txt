cmake_minimum_required(VERSION 3.14)
project(redis)

add_library(hiredis "")
target_sources(hiredis PUBLIC
        "hiredis/async.c"
        "hiredis/async.h"
        "hiredis/dict.c"
        "hiredis/dict.h"
        "hiredis/fmacros.h"
        "hiredis/hiredis.c"
        "hiredis/hiredis.h"
        "hiredis/net.c"
        "hiredis/net.h"
        "hiredis/read.c"
        "hiredis/read.h"
        "hiredis/sds.c"
        "hiredis/sds.h"
        #"hiredis/test.c"
        "hiredis/win32.h"
        "redis_client.h"
        )
target_include_directories(hiredis PUBLIC hiredis/adapters .)
