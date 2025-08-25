#pragma once
#if defined(_WIN32)
#  include <windows.h>
inline void pin_current_thread_to_core(unsigned i){
  DWORD_PTR mask = (1ULL << i);
  SetThreadAffinityMask(GetCurrentThread(), mask);
  SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
}
#else
inline void pin_current_thread_to_core(unsigned) {}
#endif
