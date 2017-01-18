cmake_minimum_required(VERSION 3.6)
project(mp1)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        testcases/msgdropsinglefailure.conf
        testcases/multifailure.conf
        testcases/singlefailure.conf
        Application.cpp
        Application.h
        EmulNet.cpp
        EmulNet.h
        Log.cpp
        Log.h
        Member.cpp
        Member.h
        MP1Node.cpp
        MP1Node.h
        Params.cpp
        Params.h
        Queue.h
        stdincludes.h)

include_directories(.)
include_directories(testcases)

add_executable(mp1 ${SOURCE_FILES})