add_requires("uwebsockets", "leveldb", "spdlog", "flatbuffers", "ctrl-c")
add_requires("backward-cpp", {configs = {stack_details = "backtrace_symbol"}, optional = true})
add_requires("atomic", {system = true, optional = true})

add_rules("mode.release", "mode.debug")
set_languages("c++23")

rule("flatc")
  add_deps("c++")
  set_extensions(".fbs")
  on_config(function (target)
    local headersdir = path.join(target:autogendir(), "rules", "flatc")
    target:add("includedirs", headersdir)
  end)
  before_buildcmd_file(function (target, batchcmds, sourcefile, opt)
    local headersdir = path.join(target:autogendir(), "rules", "flatc")
    local headersfile = path.join(headersdir, path.basename(sourcefile) .. "_generated.h")

    import("lib.detect.find_tool")
    local flatc = assert(find_tool("flatc"))
    batchcmds:show_progress(opt.progress, "${color.build.object}flatc %s", sourcefile)
    batchcmds:vrunv(flatc.program, {"-o", headersdir, "-c", sourcefile})
    batchcmds:vrunv(flatc.program, {"-o", "npm", "-T", sourcefile})

    batchcmds:add_depfiles(sourcefile)
    batchcmds:set_depmtime(os.mtime(headersfile))
    batchcmds:set_depcache(target:dependfile(headersfile))
  end)

option("backward")
  set_default(false)

target("nanokv")
  add_options("backward")
  add_rules("flatc")
  set_kind("binary")
  add_files("src/*.cpp")
  add_files("src/*.fbs")
  add_packages("uwebsockets", "leveldb", "spdlog", "flatbuffers", "ctrl-c", "atomic")
  if has_package("backward-cpp") and has_config("backward") then
    add_packages("backward-cpp")
    add_defines("USE_BACKWARD")
  end
