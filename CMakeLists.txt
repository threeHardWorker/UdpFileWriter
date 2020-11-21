cmake_minimum_required(VERSION 3.0)
project(RE VERSION 0.5 DESCRIPTION "the re project")

set(CMAKE_CXX_STANDARD 17)


#configuration types
SET(CMAKE_CONFIGURATION_TYPES "Debug;Release" CACHE STRING "Configs" FORCE)
IF(DEFINED CMAKE_BUILD_TYPE AND CMAKE_VERSION VERSION_GREATER "2.8")
  SET_PROPERTY(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS ${CMAKE_CONFIGURATION_TYPES})
ENDIF()

IF (CMAKE_BUILD_TYPE MATCHES Debug)
  message("Debug build")
  set(CMAKE_C_FLAGS_DEBUG "-g -Wall -O2 -DDEBUG")
  set(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -O2 -DDEBUG")
ENDIF ()

IF (CMAKE_BUILD_TYPE MATCHES Release)
  message("Release build")
  set(CMAKE_C_FLAGS "-Wall -O2")
  set(CMAKE_CXX_FLAGS "-Wall -O2")
ENDIF ()

add_executable(udpwriter main.cpp)
target_link_libraries(udpwriter pthread)

