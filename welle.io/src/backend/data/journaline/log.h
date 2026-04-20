/*
 * log.h – Qt-free stub for WarnBridge / welle.io
 */
#pragma once
#include <stdio.h>
#include <stdarg.h>

#define LOG_ERR   3
#define LOG_INFO  6
#define LOG_ERR_DUMP 4

static inline void logit(int level, const char* fmt, ...) {
    (void)level;
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
    va_end(args);
}
