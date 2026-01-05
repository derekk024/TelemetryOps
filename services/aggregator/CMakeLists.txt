# services/aggregator/CMakeLists.txt
add_executable(aggregator main.cpp)
target_link_libraries(aggregator PRIVATE common)