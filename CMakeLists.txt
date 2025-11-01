cmake_minimum_required(VERSION 3.12)
project(rank_run VERSION 1.0 LANGUAGES C)

# Require MPI
find_package(MPI REQUIRED)

# Create executable
add_executable(rank_run.x rank_run.c)

# Link MPI libraries
target_link_libraries(rank_run.x PRIVATE MPI::MPI_C)

# Enable strict warnings for safer builds
target_compile_options(rank_run.x PRIVATE -Wall -Wextra -Wpedantic)

# Add optional build type (default = Release)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Installation target (optional)
install(TARGETS rank_run.x DESTINATION bin)

# Print build summary
message(STATUS "Project: ${PROJECT_NAME}")
message(STATUS "Version: ${PROJECT_VERSION}")
message(STATUS "MPI Include Path: ${MPI_C_INCLUDE_DIRS}")
message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
