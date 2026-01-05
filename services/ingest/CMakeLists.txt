# services/ingest/CMakeLists.txt
add_executable(ingest main.cpp)
target_link_libraries(ingest PRIVATE common)