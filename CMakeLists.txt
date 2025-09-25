cmake_minimum_required(VERSION 3.22)
project(HashThesis CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler warnings
if (MSVC)
  add_compile_options(/W4 /permissive-)
else()
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Always read seeds from the repo's seed/ directory
set(REPO_SEED_DIR "${CMAKE_SOURCE_DIR}/seed")
file(TO_CMAKE_PATH "${REPO_SEED_DIR}" REPO_SEED_DIR_FWD)
add_compile_definitions(RNG_DEFAULT_SEED_DIR="${REPO_SEED_DIR_FWD}")

# Always read seeds from the repo's seed/ directory
set(REPO_ROOT_DIR "${CMAKE_SOURCE_DIR}")
file(TO_CMAKE_PATH "${REPO_ROOT_DIR}" REPO_ROOT_DIR_FWD)
add_compile_definitions(ROOT_DEFAULT_DIR="${REPO_ROOT_DIR_FWD}")

# Executables (add sources as you implement things)
add_executable(bench_timing  src/main_bench_timing.cpp)
add_executable(accuracy      src/main_accuracy.cpp)
add_executable(verify        src/main_verify.cpp)

target_include_directories(bench_timing PRIVATE include)
target_include_directories(accuracy     PRIVATE include)
target_include_directories(verify       PRIVATE include)
