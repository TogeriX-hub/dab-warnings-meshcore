/*
 * Splitter.h – Qt-free stub for WarnBridge / welle.io
 */
#pragma once
#include <string>
#include <vector>

class Splitter {
public:
    void Split(std::vector<std::string>& lines, const std::string& s) {
        std::string line;
        for (char c : s) {
            if (c == '\n') { lines.push_back(line); line.clear(); }
            else line += c;
        }
        if (!line.empty()) lines.push_back(line);
    }
};
