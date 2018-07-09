add_executable(tbm main.cc)

target_link_libraries(tbm queue_race boost_system boost_timer tcmalloc)

set_target_properties(tbm PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
