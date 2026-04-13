vcpkg_download_distfile(ARCHIVE
    URLS "https://ftp.postgresql.org/pub/source/v${VERSION}/postgresql-${VERSION}.tar.bz2"
         "https://www.mirrorservice.org/sites/ftp.postgresql.org/source/v${VERSION}/postgresql-${VERSION}.tar.bz2"
    FILENAME "postgresql-${VERSION}.tar.bz2"
    SHA512 fdbe6d726f46738cf14acab96e5c05f7d65aefe78563281b416bb14a27c7c42e4df921e26b32816a5030ddbe506b95767e2c74a35afc589916504df38d1cb11c
)

vcpkg_extract_source_archive(
    SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES
        unix/installdirs.patch
        unix/fix-configure.patch
        unix/single-linkage.patch
        unix/no-server-tools.patch
        unix/mingw-install.patch
        unix/mingw-static-importlib-fix.patch
        unix/python.patch
        windows/macro-def.patch
        windows/spin_delay.patch
        windows/tcl-9.0-alpha.patch
        windows/meson-vcpkg.patch
        windows/getopt.patch
)

file(GLOB _py3_include_path "${CURRENT_HOST_INSTALLED_DIR}/include/python3*")
string(REGEX MATCH "python3\\.([0-9]+)" _python_version_tmp "${_py3_include_path}")
set(PYTHON_VERSION_MINOR "${CMAKE_MATCH_1}")

if("client" IN_LIST FEATURES)
    set(HAS_TOOLS TRUE)
else()
    set(HAS_TOOLS FALSE)
endif()

vcpkg_cmake_get_vars(cmake_vars_file)
include("${cmake_vars_file}")

set(required_programs BISON FLEX)
if(VCPKG_DETECTED_MSVC OR NOT VCPKG_HOST_IS_WINDOWS)
    list(APPEND required_programs PERL)
endif()
foreach(program_name IN LISTS required_programs)
    vcpkg_find_acquire_program(${program_name})
    get_filename_component(program_dir ${${program_name}} DIRECTORY)
    vcpkg_add_to_path(PREPEND "${program_dir}")
endforeach()

if(VCPKG_DETECTED_MSVC)
    include("${CMAKE_CURRENT_LIST_DIR}/build-msvc.cmake")
    build_msvc("${SOURCE_PATH}")
else()
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/Makefile" DESTINATION "${SOURCE_PATH}")

    # vcpkg downloads bison/flex as win_bison.exe / win_flex.exe, but the
    # autoconf configure script searches for "bison" / "flex" by name.
    # Create copies so configure can find them.
    if(VCPKG_HOST_IS_WINDOWS)
        get_filename_component(_bison_dir "${BISON}" DIRECTORY)
        if(NOT EXISTS "${_bison_dir}/bison.exe")
            file(COPY_FILE "${BISON}" "${_bison_dir}/bison.exe")
        endif()
        get_filename_component(_flex_dir "${FLEX}" DIRECTORY)
        if(NOT EXISTS "${_flex_dir}/flex.exe")
            file(COPY_FILE "${FLEX}" "${_flex_dir}/flex.exe")
        endif()
    endif()

    vcpkg_list(SET BUILD_OPTS)
    foreach(option IN ITEMS bonjour icu lz4 readline zlib zstd)
        if(option IN_LIST FEATURES)
            list(APPEND BUILD_OPTS --with-${option})
        else()
            list(APPEND BUILD_OPTS --without-${option})
        endif()
    endforeach()
    if("openssl" IN_LIST FEATURES)
        list(APPEND BUILD_OPTS --with-openssl)
    else()
        list(APPEND BUILD_OPTS --without-openssl)
    endif()
    if("xml" IN_LIST FEATURES)
        list(APPEND BUILD_OPTS --with-libxml)
    else()
        list(APPEND BUILD_OPTS --without-libxml)
    endif()
    if("xslt" IN_LIST FEATURES)
        list(APPEND BUILD_OPTS --with-libxslt)
    else()
        list(APPEND BUILD_OPTS --without-libxslt)
    endif()
    if("nls" IN_LIST FEATURES)
        list(APPEND BUILD_OPTS --enable-nls)
    else()
        list(APPEND BUILD_OPTS --disable-nls)
    endif()
    if("nls" IN_LIST FEATURES)
        set(ENV{MSGFMT} "${CURRENT_HOST_INSTALLED_DIR}/tools/gettext/bin/msgfmt${VCPKG_HOST_EXECUTABLE_SUFFIX}")
    endif()
    if("python" IN_LIST FEATURES)
        list(APPEND BUILD_OPTS --with-python=3.${PYTHON_VERSION_MINOR})
        vcpkg_find_acquire_program(PYTHON3)
        list(APPEND BUILD_OPTS "PYTHON=${PYTHON3}")
    endif()
    if(VCPKG_TARGET_IS_ANDROID AND (NOT VCPKG_CMAKE_SYSTEM_VERSION OR VCPKG_CMAKE_SYSTEM_VERSION LESS "26"))
        list(APPEND BUILD_OPTS ac_cv_header_langinfo_h=no)
    endif()
    if(VCPKG_DETECTED_CMAKE_OSX_SYSROOT)
        list(APPEND BUILD_OPTS "PG_SYSROOT=${VCPKG_DETECTED_CMAKE_OSX_SYSROOT}")
    endif()
    if(VCPKG_TARGET_IS_OSX)
          set(ENV{LDFLAGS} "$ENV{LDFLAGS} -headerpad_max_install_names")
      endif()
    vcpkg_configure_make(
        SOURCE_PATH "${SOURCE_PATH}"
        COPY_SOURCE
        AUTOCONFIG
        ADDITIONAL_MSYS_PACKAGES autoconf-archive
            DIRECT_PACKAGES
                "https://mirror.msys2.org/msys/x86_64/tzcode-2025b-1-x86_64.pkg.tar.zst"
                824779e3ac4857bb21cbdc92fa881fa24bf89dfa8bc2f9ca816e9a9837a6d963805e8e0991499c43337a134552215fdee50010e643ddc8bd699170433a4c83de
        OPTIONS
            ${BUILD_OPTS}
        OPTIONS_DEBUG
            --enable-debug
    )

    # Fix: On MinGW/Windows, ./configure records Windows backslash paths
    # (e.g. C:\Users) in the CONFIGURE_ARGS macro in pg_config.h. Clang treats
    # \U as a universal character name escape, causing a compile error in
    # config_info.c. Replace with an empty string before compilation.
    if(VCPKG_TARGET_IS_MINGW)
        foreach(_buildtype IN ITEMS "rel" "dbg")
            set(_pg_config_h "${CURRENT_BUILDTREES_DIR}/${TARGET_TRIPLET}-${_buildtype}/src/include/pg_config.h")
            if(EXISTS "${_pg_config_h}")
                file(READ "${_pg_config_h}" _pg_config_content)
                string(REGEX REPLACE "#define CONFIGURE_ARGS \"[^\"]*\"" "#define CONFIGURE_ARGS \"\"" _pg_config_content "${_pg_config_content}")
                file(WRITE "${_pg_config_h}" "${_pg_config_content}")
            endif()
        endforeach()
    endif()

    if(VCPKG_LIBRARY_LINKAGE STREQUAL "dynamic")
        set(ENV{LIBPQ_LIBRARY_TYPE} shared)
    else()
        set(ENV{LIBPQ_LIBRARY_TYPE} static)
    endif()
    if(VCPKG_TARGET_IS_MINGW)
        set(ENV{LIBPQ_USING_MINGW} yes)
    endif()
    if(HAS_TOOLS)
        set(ENV{LIBPQ_ENABLE_TOOLS} yes)
    endif()
    vcpkg_install_make(DISABLE_PARALLEL)

    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/include/postgresql/server/pg_config.h" "#define CONFIGURE_ARGS" "// #define CONFIGURE_ARGS")
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/include/pg_config.h" "#define CONFIGURE_ARGS" "// #define CONFIGURE_ARGS")
endif()

vcpkg_fixup_pkgconfig()
configure_file("${CMAKE_CURRENT_LIST_DIR}/vcpkg-cmake-wrapper.cmake" "${CURRENT_PACKAGES_DIR}/share/postgresql/vcpkg-cmake-wrapper.cmake" @ONLY)

file(REMOVE_RECURSE
    "${CURRENT_PACKAGES_DIR}/debug/doc"
    "${CURRENT_PACKAGES_DIR}/debug/include"
    "${CURRENT_PACKAGES_DIR}/debug/share"
    "${CURRENT_PACKAGES_DIR}/debug/symbols"
    "${CURRENT_PACKAGES_DIR}/debug/tools"
    "${CURRENT_PACKAGES_DIR}/symbols"
    "${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug"
)

file(INSTALL "${CURRENT_PORT_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/COPYRIGHT")
