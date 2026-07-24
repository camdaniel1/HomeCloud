cmake_minimum_required(VERSION 3.13)
project(iot_sensor_pipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(USE_ODBC "Build ingestion-service/dashboard-api against SQL Server via unixODBC" OFF)

find_package(Threads REQUIRED)

add_executable(sensor-agent sensor-agent/src/main.cpp)
target_link_libraries(sensor-agent PRIVATE Threads::Threads)

add_executable(ingestion-service ingestion-service/src/main.cpp)
target_link_libraries(ingestion-service PRIVATE Threads::Threads)

add_executable(dashboard-api dashboard-api/src/main.cpp)
target_link_libraries(dashboard-api PRIVATE Threads::Threads)

if(USE_ODBC)
  find_library(ODBC_LIB odbc REQUIRED)
  target_compile_definitions(ingestion-service PRIVATE USE_ODBC)
  target_link_libraries(ingestion-service PRIVATE ${ODBC_LIB})
endif()

install(TARGETS sensor-agent ingestion-service dashboard-api RUNTIME DESTINATION bin)
