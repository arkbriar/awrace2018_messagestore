add_executable(bm main.cc)

target_link_libraries(bm queue_race)

set_target_properties(bm PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
