# Pre-built OpenSSL overlay port
# Copies pre-built OpenSSL libraries from sindarin-pkg-libs instead of compiling from source.

# Map vcpkg triplet to sindarin platform name
if(VCPKG_TARGET_IS_WINDOWS OR VCPKG_TARGET_IS_MINGW)
    set(SN_PLATFORM "windows")
elseif(VCPKG_TARGET_IS_OSX)
    set(SN_PLATFORM "darwin")
else()
    set(SN_PLATFORM "linux")
endif()

set(PREBUILT "${CURRENT_PORT_DIR}/../../.sn/sindarin-pkg-libs/libs/${SN_PLATFORM}")

if(NOT EXISTS "${PREBUILT}")
    message(FATAL_ERROR
        "Pre-built OpenSSL not found at: ${PREBUILT}\n"
        "Run 'sn --install' first to populate .sn/sindarin-pkg-libs/."
    )
endif()

# Install headers
file(INSTALL "${PREBUILT}/include/openssl" DESTINATION "${CURRENT_PACKAGES_DIR}/include")

# Install static libraries (release)
file(INSTALL "${PREBUILT}/lib/libssl.a" DESTINATION "${CURRENT_PACKAGES_DIR}/lib")
file(INSTALL "${PREBUILT}/lib/libcrypto.a" DESTINATION "${CURRENT_PACKAGES_DIR}/lib")

# Install static libraries (debug — same binaries, vcpkg requires both)
file(INSTALL "${PREBUILT}/lib/libssl.a" DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib")
file(INSTALL "${PREBUILT}/lib/libcrypto.a" DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib")

# Install only OpenSSL pkg-config files (not the entire pkgconfig directory)
foreach(_pc libssl.pc libcrypto.pc openssl.pc)
    if(EXISTS "${PREBUILT}/lib/pkgconfig/${_pc}")
        file(INSTALL "${PREBUILT}/lib/pkgconfig/${_pc}" DESTINATION "${CURRENT_PACKAGES_DIR}/lib/pkgconfig")
        file(INSTALL "${PREBUILT}/lib/pkgconfig/${_pc}" DESTINATION "${CURRENT_PACKAGES_DIR}/debug/lib/pkgconfig")
    endif()
endforeach()

# CMake wrapper so find_package(OpenSSL) works for CMake-based consumers.
# Modeled on the standard vcpkg openssl port wrapper to ensure correct discovery.
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/vcpkg-cmake-wrapper.cmake" [[
cmake_policy(PUSH)
cmake_policy(SET CMP0012 NEW)
cmake_policy(SET CMP0054 NEW)
cmake_policy(SET CMP0057 NEW)

# vcpkg handles linkage via the triplet — temporarily disable OPENSSL_USE_STATIC_LIBS
# so FindOpenSSL doesn't reject our libs based on naming conventions.
if(OPENSSL_USE_STATIC_LIBS)
    set(OPENSSL_USE_STATIC_LIBS_BAK "${OPENSSL_USE_STATIC_LIBS}")
    set(OPENSSL_USE_STATIC_LIBS FALSE)
endif()

# Point FindOpenSSL at the vcpkg installed tree
if(DEFINED OPENSSL_ROOT_DIR)
    set(OPENSSL_ROOT_DIR_BAK "${OPENSSL_ROOT_DIR}")
endif()
get_filename_component(OPENSSL_ROOT_DIR "${CMAKE_CURRENT_LIST_DIR}" DIRECTORY)
get_filename_component(OPENSSL_ROOT_DIR "${OPENSSL_ROOT_DIR}" DIRECTORY)
find_path(OPENSSL_INCLUDE_DIR NAMES openssl/ssl.h PATHS "${OPENSSL_ROOT_DIR}/include" NO_DEFAULT_PATH)

# Pre-find libraries so _find_package picks them up from cache
find_library(OPENSSL_CRYPTO_LIBRARY NAMES crypto PATHS "${OPENSSL_ROOT_DIR}/lib" NO_DEFAULT_PATH)
find_library(OPENSSL_SSL_LIBRARY NAMES ssl PATHS "${OPENSSL_ROOT_DIR}/lib" NO_DEFAULT_PATH)

_find_package(${ARGS})

unset(OPENSSL_ROOT_DIR)
if(DEFINED OPENSSL_ROOT_DIR_BAK)
    set(OPENSSL_ROOT_DIR "${OPENSSL_ROOT_DIR_BAK}")
    unset(OPENSSL_ROOT_DIR_BAK)
endif()

if(DEFINED OPENSSL_USE_STATIC_LIBS_BAK)
    set(OPENSSL_USE_STATIC_LIBS "${OPENSSL_USE_STATIC_LIBS_BAK}")
    unset(OPENSSL_USE_STATIC_LIBS_BAK)
endif()

# Static builds need -ldl and -lpthread on Unix
if(OPENSSL_FOUND)
    if(NOT WIN32)
        find_library(OPENSSL_DL_LIBRARY NAMES dl)
        if(OPENSSL_DL_LIBRARY)
            list(APPEND OPENSSL_LIBRARIES "dl")
            if(TARGET OpenSSL::Crypto)
                set_property(TARGET OpenSSL::Crypto APPEND PROPERTY INTERFACE_LINK_LIBRARIES "dl")
            endif()
        endif()

        if("REQUIRED" IN_LIST ARGS)
            find_package(Threads REQUIRED)
        else()
            find_package(Threads)
        endif()
        list(APPEND OPENSSL_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
        if(TARGET OpenSSL::Crypto)
            set_property(TARGET OpenSSL::Crypto APPEND PROPERTY INTERFACE_LINK_LIBRARIES "Threads::Threads")
        endif()
        if(TARGET OpenSSL::SSL)
            set_property(TARGET OpenSSL::SSL APPEND PROPERTY INTERFACE_LINK_LIBRARIES "Threads::Threads")
        endif()
    endif()
endif()
cmake_policy(POP)
]])

# Copyright
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright"
    "OpenSSL is licensed under the Apache License 2.0.\nPre-built binaries provided by sindarin-pkg-libs.\n")

vcpkg_fixup_pkgconfig()
