#include <ctrl-c.h>
#include <spdlog/spdlog.h>

#ifdef USE_BACKWARD
#include <backward.hpp>
#endif

#include "core.h"

int main() {
#ifdef USE_BACKWARD
  backward::SignalHandling sh;
#endif
  spdlog::set_pattern("[%H:%M:%S %z] [thread %t] %v");
  nanokv::Core core;

  CtrlCLibrary::SetCtrlCHandler([&](auto) {
    spdlog::warn("CtrlC triggered");
    core.stop();
    return true;
  });

  core.run();
  spdlog::warn("Exiting");
  return 0;
}