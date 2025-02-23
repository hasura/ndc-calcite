﻿#ifndef GLOBALS_HPP
#define GLOBALS_HPP

#include <string>
#include "DiagnosticManager.hpp"

extern std::string dllPath;
extern DiagnosticManager *diagMgr;

#define SQL_NTS    (-3)
#define SQL_NTS_W  (-3)  // Some ODBC implementations use this for wide strings

#endif // GLOBALS_HPP