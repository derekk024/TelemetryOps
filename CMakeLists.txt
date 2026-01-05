# CMakeLists.txt (repo root)
cmake_minimum_required(VERSION 3.20)
project(leo_telemetry LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.14.1
)
FetchContent_MakeAvailable(spdlog)

FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.15.3
)
FetchContent_MakeAvailable(httplib)

find_package(SQLite3 REQUIRED)

add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/common/include)
target_link_libraries(common INTERFACE
  nlohmann_json::nlohmann_json
  spdlog::spdlog_header_only
  httplib::httplib
  SQLite::SQLite3
)

add_subdirectory(services/ingest)
add_subdirectory(services/aggregator)
add_subdirectory(services/controlplane)