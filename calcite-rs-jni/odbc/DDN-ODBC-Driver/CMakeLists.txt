cmake_minimum_required(VERSION 3.20)
project(odbc_driver)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Platform configurations
if (WIN32)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif ()

# Vcpkg integration
set(CMAKE_TOOLCHAIN_FILE "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file" FORCE)

# Determine the processor architecture
if (NOT DEFINED CMAKE_GENERATOR_PLATFORM OR CMAKE_GENERATOR_PLATFORM STREQUAL "")
    message(STATUS "[DEBUG] CMAKE_GENERATOR_PLATFORM not set, detecting automatically.")
    message(STATUS "[DEBUG] CMAKE_HOST_SYSTEM_PROCESSOR: ${CMAKE_HOST_SYSTEM_PROCESSOR}")
    message(STATUS "[DEBUG] CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "ARM64" OR ${CMAKE_SYSTEM_PROCESSOR} MATCHES "ARM64")
        set(TARGET_ARCH "ARM64")
    else()
        set(TARGET_ARCH "X64")
    endif()
else()
    set(TARGET_ARCH ${CMAKE_GENERATOR_PLATFORM})
endif()

message(STATUS "[DEBUG] Target Architecture: ${TARGET_ARCH}")

if (TARGET_ARCH STREQUAL "ARM64")
    if (NOT DEFINED ENV{JAVA_HOME_ARM64})
        message(FATAL_ERROR "JAVA_HOME_ARM64 environment variable not set")
    endif()
    set(JDK_PATH $ENV{JAVA_HOME_ARM64})
    message(STATUS "[DEBUG] Setting ARM64 JDK path: ${JDK_PATH}")
elseif (TARGET_ARCH STREQUAL "X64")
    if (NOT DEFINED ENV{JAVA_HOME_X64})
        message(FATAL_ERROR "JAVA_HOME_X64 environment variable not set")
    endif()
    set(JDK_PATH $ENV{JAVA_HOME_X64})
    message(STATUS "[DEBUG] Setting X64 JDK path: ${JDK_PATH}")
else()
    message(FATAL_ERROR "Unsupported platform: ${TARGET_ARCH}")
endif()

# Convert path to CMake format
file(TO_CMAKE_PATH "${JDK_PATH}" JDK_PATH)
message(STATUS "[DEBUG] Converted JDK path: ${JDK_PATH}")

# Set JNI paths
set(JNI_INCLUDE_DIRS
    "${JDK_PATH}/include"
    "${JDK_PATH}/include/win32"
)

# Debug Messages
message(STATUS "[DEBUG] JNI include directories: ${JNI_INCLUDE_DIRS}")

# Set JNI library path
set(JNI_LIBRARIES "${JDK_PATH}/lib/jvm.lib")
message(STATUS "[DEBUG] JNI library path: ${JNI_LIBRARIES}")

# Verify JNI paths exist
foreach(dir ${JNI_INCLUDE_DIRS})
    if (NOT EXISTS "${dir}")
        message(FATAL_ERROR "JNI include directory not found: ${dir}")
    endif()
    message(STATUS "[DEBUG] Include dir exists: ${dir}")
endforeach()

if (NOT EXISTS "${JNI_LIBRARIES}")
    message(FATAL_ERROR "JNI library not found: ${JNI_LIBRARIES}")
else()
    message(STATUS "[DEBUG] JNI library exists at: ${JNI_LIBRARIES}")
endif()

# Source files
set(SOURCE_FILES
    src/connection.cpp
    src/logging.cpp
    src/main.cpp
    src/statement.cpp
    src/SQLColAttribute.cpp
    src/SQLColumns.cpp
    src/SQLConnect.cpp
    src/SQLDisconnect.cpp
    src/SQLDriverConnect.cpp
    src/SQLFetch.cpp
    src/SQLFreeStmt.cpp
    src/SQLGetData.cpp
    src/SQLInfo.cpp
    src/SQLMoreResults.cpp
    src/SQLRowCount.cpp
    src/SQLTables.cpp
    src/SQLAllocHandle.cpp
    src/SQLFreeHandle.cpp
    src/SQLExecDirect.cpp
    src/SQLSetEnvAttr.cpp
        src/SQLGetStmtAttr.cpp
        src/SQLSetDescField.cpp
        src/SQLGetTypeInfo.cpp
        src/SQLBindParameter.cpp
)

# Header files
set(HEADER_FILES
    include/connection.hpp
    include/environment.hpp
    include/globals.hpp
    include/logging.hpp
    include/statement.hpp
        include/JniParam.hpp
        src/JniParam.cpp
)

# Function to filter files starting with `._`
function(filter_files file_list)
    set(filtered_files "")
    foreach(file ${${file_list}})
        if (NOT file MATCHES "^._")
            list(APPEND filtered_files ${file})
        endif()
    endforeach()
    set(${file_list} "${filtered_files}" PARENT_SCOPE)
endfunction()
filter_files(SOURCE_FILES)
filter_files(HEADER_FILES)

add_definitions(-D_SILENCE_CXX17_C_HEADER_DEPRECATION_WARNING)

# Create the library
add_library(${PROJECT_NAME} SHARED
    ${SOURCE_FILES}
    ${HEADER_FILES}
    odbc_driver.def
)

# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${JNI_INCLUDE_DIRS}
)

# Link libraries
target_link_libraries(${PROJECT_NAME} PRIVATE
    ${JNI_LIBRARIES}
    odbc32
    odbccp32
)

# Compile definitions
target_compile_definitions(${PROJECT_NAME} PRIVATE
    WIN32
    _WINDOWS
    _USRDLL
)

# Set output directories for all configurations
set_target_properties(${PROJECT_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        RUNTIME_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}/bin"
        RUNTIME_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/bin"
        LIBRARY_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}/bin"
        LIBRARY_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/bin"
        ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${CMAKE_BINARY_DIR}/bin"
        ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${CMAKE_BINARY_DIR}/bin"
        OUTPUT_NAME "DDN-ODBC-Driver"
)

# Copy JAR files to output directory
add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/bin"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
    "${CMAKE_CURRENT_SOURCE_DIR}/../../jni-arrow/target/jni-arrow-1.0.0-jar-with-dependencies.jar"
    "${CMAKE_BINARY_DIR}/bin/jni-arrow-1.0.0-jar-with-dependencies.jar"
)

# Print configuration summary
message(STATUS "")
message(STATUS "Configuration summary:")
message(STATUS "--------------------")
message(STATUS "Target Architecture: ${TARGET_ARCH}")
message(STATUS "Output directory: ${CMAKE_BINARY_DIR}/bin")
message(STATUS "JDK Path: ${JDK_PATH}")
message(STATUS "JNI Include Dirs: ${JNI_INCLUDE_DIRS}")
message(STATUS "JNI Libraries: ${JNI_LIBRARIES}")
message(STATUS "--------------------")