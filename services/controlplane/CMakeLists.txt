# services/controlplane/CMakeLists.txt
add_executable(controlplane main.cpp)
target_link_libraries(controlplane PRIVATE common)