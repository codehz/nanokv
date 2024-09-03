#include <ctrl-c.h>
#include <spdlog/spdlog.h>

#include <backward.hpp>

#include "core.h"

int main() {
  backward::SignalHandling sh;
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