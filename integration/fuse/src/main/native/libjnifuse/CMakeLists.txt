cmake_minimum_required(VERSION 3.17)
project(jnifuse)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-Wall -D_FILE_OFFSET_BITS=64")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

find_package(JNI REQUIRED)
find_package(FUSE REQUIRED)

include_directories(
        #${CMAKE_CURRENT_SOURCE_DIR}
        ${JNI_INCLUDE_DIRS}
        ${FUSE_INCLUDE_DIR})

# set(SOURCE_FILES jnifuse_helper.cc jnifuse_fs.cc jnifuse_impls.cc operation.cc)
file(GLOB SRCS *.c *.cpp *.cc *.h *.hpp)

add_library(jnifuse SHARED ${SRCS})

TARGET_LINK_LIBRARIES(jnifuse ${JNI_LIBRARIES} ${FUSE_LIBRARIES})

install(TARGETS jnifuse DESTINATION lib)