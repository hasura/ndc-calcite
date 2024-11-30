#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Environment.hpp"
#include "../include/Statement.hpp"

extern "C" {
SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV environmentHandle, SQLINTEGER attribute, SQLPOINTER value,
                                SQLINTEGER stringLength) {
    LOG("SQLSetEnvAttr called, environmentHandle: " + std::to_string(reinterpret_cast<uint64_t>(environmentHandle)) +
        ", attribute: " + std::to_string(attribute));

    if (!environmentHandle) {
        LOG("Invalid environment handle");
        return SQL_INVALID_HANDLE;
    }

    auto *env = static_cast<Environment *>(environmentHandle);

    switch (attribute) {
        case SQL_ATTR_ODBC_VERSION:
            // Ensure the driver supports the requested ODBC version
            if (reinterpret_cast<unsigned long>(value) == SQL_OV_ODBC3) {
                env->envVersion = SQL_OV_ODBC3;
                LOG("SQLSetEnvAttr: Set ODBC version to " + std::to_string(env->envVersion));
                return SQL_SUCCESS;
            } else {
                LOG("SQLSetEnvAttr: Unsupported ODBC version");
                return SQL_ERROR;
            }

        default:
            LOG("SQLSetEnvAttr: Unknown attribute requested");
            return SQL_ERROR;
    }
}
} // extern "C"
