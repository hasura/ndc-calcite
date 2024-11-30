#include <winsock2.h>
#include <windows.h>
#define WIN32_LEAN_AND_MEAN
#include "../include/Connection.hpp"
#include "../include/Logging.hpp"
#include "../include/Globals.hpp"
#include "../include/Environment.hpp"
#include "../include/DiagnosticManager.hpp"
#include "../include/JVMSingleton.hpp"

std::string dllPath;
DiagnosticManager* diagMgr;

extern "C" {

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call, LPVOID lpReserved)
{
    switch (ul_reason_for_call) {
        case DLL_PROCESS_ATTACH:
            dllPath = GetModuleDirectory();
            diagMgr = new DiagnosticManager();
            break;
        case DLL_THREAD_ATTACH:
        case DLL_THREAD_DETACH:
        case DLL_PROCESS_DETACH:
            break;
    }
    return TRUE;
}
}
