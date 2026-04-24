cmake_minimum_required(VERSION 3.20)
project(rithmic_engine VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── Dependencies ───────────────────────────────────────────────────
find_package(Boost 1.82 REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)

# PostgreSQL (libpq)
find_package(PostgreSQL REQUIRED)
find_package(Curses REQUIRED)

# ── Protobuf codegen ───────────────────────────────────────────────
set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/rithmic.proto")
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/proto")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILE})

# ── Shared sources (used by main binary and tests) ─────────────────
set(LIB_SOURCES
    src/db.cpp
    src/audit.cpp
    src/client.cpp
    src/collector.cpp
    ${PROTO_SRCS}
)

add_library(rithmic_lib STATIC ${LIB_SOURCES})

target_include_directories(rithmic_lib PUBLIC
    src/
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(rithmic_lib PUBLIC
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    ${Protobuf_LIBRARIES}
    ${PostgreSQL_LIBRARIES}
)

target_compile_definitions(rithmic_lib PUBLIC
    BOOST_ASIO_HAS_CO_AWAIT
)

# ── Main binary ────────────────────────────────────────────────────
add_executable(rithmic_engine src/main.cpp)
target_link_libraries(rithmic_engine PRIVATE rithmic_lib)

# ── Test binaries ──────────────────────────────────────────────────
add_executable(test_db tests/test_db.cpp)
target_link_libraries(test_db PRIVATE rithmic_lib)
target_include_directories(test_db PRIVATE src/)

add_executable(test_connection tests/test_connection.cpp)
target_link_libraries(test_connection PRIVATE rithmic_lib)
target_include_directories(test_connection PRIVATE src/)

add_executable(test_validator tests/test_validator.cpp)
target_link_libraries(test_validator PRIVATE rithmic_lib)
target_include_directories(test_validator PRIVATE src/)

enable_testing()
add_test(NAME db_tests        COMMAND test_db)
add_test(NAME validator_tests COMMAND test_validator)

# ── Dashboard (ncurses TUI) ────────────────────────────────────────
add_executable(dashboard src/dashboard.cpp)
target_link_libraries(dashboard PRIVATE rithmic_lib ${CURSES_LIBRARIES})
target_include_directories(dashboard PRIVATE src/ ${CURSES_INCLUDE_DIRS})

# ── orb_strategy (standalone ORB signal binary for CI parity tests) ──
# Header-only nlohmann/json; no link deps beyond stdlib.
add_executable(orb_strategy src/orb_strategy_main.cpp)
target_compile_features(orb_strategy PRIVATE cxx_std_17)

# ── NQ ORB Executor ────────────────────────────────────────────────
set(NQ_EXECUTOR_SOURCES
    src/execution/executor_main.cpp
    src/execution/orb_strategy.cpp
    src/execution/order_manager.cpp
    src/execution/risk_manager.cpp
    src/execution/latency_logger.cpp
)

add_executable(nq_executor ${NQ_EXECUTOR_SOURCES})

target_include_directories(nq_executor PRIVATE
    src/execution/
    src/
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${Protobuf_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(nq_executor PRIVATE rithmic_lib)

target_compile_definitions(nq_executor PRIVATE
    BOOST_ASIO_HAS_CO_AWAIT
)

# ── Install ────────────────────────────────────────────────────────
install(TARGETS rithmic_engine DESTINATION bin)
install(DIRECTORY certs/ DESTINATION share/rithmic_engine/certs)
