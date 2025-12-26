const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // redis-server
    {
        const mod = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        buildLzf(b, mod);
        buildRax(b, mod);
        buildLua(b, mod);

        const exe = b.addExecutable(.{
            .name = "redis-server",
            .root_module = mod,
        });

        b.installArtifact(exe);

        const run_cmd = b.addRunArtifact(exe);
        run_cmd.step.dependOn(b.getInstallStep());
        if (b.args) |args| {
            run_cmd.addArgs(args);
        }

        const run_step = b.step(
            "redis-server",
            "Run redis server",
        );
        run_step.dependOn(&run_cmd.step);
    }

    // all test
    {
        const mod = b.addModule("main", .{
            .root_source_file = b.path("src/Server.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });

        buildLzf(b, mod);
        buildRax(b, mod);
        buildLua(b, mod);
        const all_tests = b.addTest(.{
            .root_module = mod,
        });

        const run_tests = b.addRunArtifact(all_tests);
        b.step("test", "Run all tests").dependOn(&run_tests.step);
    }

    // quicklist test
    {
        const mod = b.addModule("quicklist", .{
            .root_source_file = b.path("src/QuickList.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        buildLzf(b, mod);

        const quicklist_test = b.addTest(.{
            .root_module = mod,
        });

        const run_tests = b.addRunArtifact(quicklist_test);
        b.step("test-quicklist", "Run quicklist tests").dependOn(&run_tests.step);
    }
}

fn buildLzf(b: *std.Build, root: *std.Build.Module) void {
    root.addCSourceFiles(.{
        .files = &.{
            "deps/lzf/lzf_c.c",
            "deps/lzf/lzf_d.c",
        },
        .language = .c,
        .flags = &.{"-DSTRICT_ALIGN=1"},
    });
    root.addIncludePath(b.path("deps/lzf"));
}

fn buildRax(b: *std.Build, root: *std.Build.Module) void {
    // TODO: use Allocator(-Dmalloc=my_malloc -Dfree=my_free)
    root.addCSourceFiles(.{
        .files = &.{
            "deps/rax/rax.c",
        },
        .language = .c,
    });
    root.addIncludePath(b.path("deps/rax"));
}

fn buildLua(b: *std.Build, mod: *std.Build.Module) void {
    mod.addIncludePath(b.path("deps/lua/src/"));
    mod.addCSourceFiles(.{
        .files = &.{
            "deps/lua/src/lapi.c",       "deps/lua/src/lcode.c",
            "deps/lua/src/ldebug.c",     "deps/lua/src/ldump.c",
            "deps/lua/src/lfunc.c",      "deps/lua/src/lgc.c",
            "deps/lua/src/llex.c",       "deps/lua/src/lmem.c",
            "deps/lua/src/lobject.c",    "deps/lua/src/lopcodes.c",
            "deps/lua/src/lparser.c",    "deps/lua/src/lstate.c",
            "deps/lua/src/lstring.c",    "deps/lua/src/ltable.c",
            "deps/lua/src/ltm.c",        "deps/lua/src/lundump.c",
            "deps/lua/src/lvm.c",        "deps/lua/src/lzio.c",
            "deps/lua/src/lauxlib.c",    "deps/lua/src/lbaselib.c",
            "deps/lua/src/ldblib.c",     "deps/lua/src/liolib.c",
            "deps/lua/src/lmathlib.c",   "deps/lua/src/loslib.c",
            "deps/lua/src/ltablib.c",    "deps/lua/src/lstrlib.c",
            "deps/lua/src/loadlib.c",    "deps/lua/src/linit.c",
            "deps/lua/src/ldo.c",        "deps/lua/src/fpconv.c",
            "deps/lua/src/strbuf.c",     "deps/lua/src/lua_bit.c",
            "deps/lua/src/lua_cjson.c",  "deps/lua/src/lua_cmsgpack.c",
            "deps/lua/src/lua_struct.c",
        },
        .language = .c,
    });
}
