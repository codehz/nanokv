#include <ctrl-c.h>
#include <spdlog/spdlog.h>

#include <argz/argz.hpp>
#include <optional>

#include "core.h"

#ifdef USE_BACKWARD
#include <backward.hpp>
#endif

constexpr std::string_view version = "0.1.0";

uint16_t check_port(uint32_t value);

int main(int argc, char *argv[]) {
#ifdef USE_BACKWARD
  backward::SignalHandling sh;
#endif
  nanokv::CoreOptions copt;

  argz::about about{"nanokv - simple key-value store with expiration and queue", version};

  argz::options options{
    {{"port", 'p'}, copt.port, "port (default 2256)"},
    {{"db", 'd'}, copt.db_path, "database path"},
    {{"thread", 't'}, copt.threads, "threads count (default to current cpu count)"},
    {{"cert", 'C'}, copt.cert, "path to ssl cert file"},
    {{"key", 'K'}, copt.key, "path to ssl key file"},
    {{"passphrase", 'P'}, copt.passphrase, "ssl key passphrase"},
    {{"ssl-ciphers", 'S'},
     copt.ssl_ciphers,
     "ssl ciphers names (default 'TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256')"},
  };

  spdlog::set_pattern("[%H:%M:%S %z] [thread %t] %v");

  try {
    argz::parse(about, options, argc, argv);
    if (copt.db_path.empty()) {
      throw std::runtime_error("Database path is required");
    }
    nanokv::Core core{copt};

    CtrlCLibrary::SetCtrlCHandler([&](auto) {
      spdlog::warn("CtrlC triggered");
      core.stop();
      return true;
    });

    core.run();
    spdlog::warn("Exiting");
    return 0;
  } catch (std::exception const &e) {
    spdlog::error(e.what());
  }
}

uint16_t check_port(uint32_t value) {
  if (value < 1 || value > 65535) {
    throw std::runtime_error("Port must be between 1 and 65535");
  }
  return (uint16_t)value;
}