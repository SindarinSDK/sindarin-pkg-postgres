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

# Copyright
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright"
    "OpenSSL is licensed under the Apache License 2.0.\nPre-built binaries provided by sindarin-pkg-libs.\n")

vcpkg_fixup_pkgconfig()
