/*
 * cpplog.h – Qt-free stub for WarnBridge / welle.io
 * Replaces the Qt-based logging from DABstar with std::cerr.
 */
#pragma once
#include <iostream>
#include <sstream>

// Minimal stream-based logger that flushes to stderr
struct CppLogStream {
    std::ostringstream oss;
    ~CppLogStream() { std::cerr << oss.str() << "\n"; }
    template<typename T>
    CppLogStream& operator<<(const T& v) { oss << v; return *this; }
};

struct EndMsg {};
static const EndMsg endmsg;

struct CppLogger {
    std::ostringstream oss;
    CppLogger& operator<<(const EndMsg&) {
        std::cerr << oss.str() << "\n";
        oss.str("");
        return *this;
    }
    template<typename T>
    CppLogger& operator<<(const T& v) { oss << v; return *this; }
};

// Global logger instances (used as log_err << "msg" << endmsg)
static CppLogger log_err;
static CppLogger log_info;
