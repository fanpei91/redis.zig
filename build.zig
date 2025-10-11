const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const redis_mod = b.addModule("redis", .{
        .root_source_file = b.path("src/server.zig"),
        .target = target,
        .optimize = optimize,
    });

    {
        redis_mod.addCSourceFiles(.{
            .files = &.{
                "src/lzf/lzf_c.c",
                "src/lzf/lzf_d.c",
            },
            .flags = &.{"-DSTRICT_ALIGN=1"},
        });
        redis_mod.addIncludePath(b.path("src/lzf"));
    }

    {
        const all_tests = b.addTest(.{
            .root_module = redis_mod,
        });

        const run_tests = b.addRunArtifact(all_tests);
        b.step("test", "Run all tests").dependOn(&run_tests.step);
    }

    {
        const mod = b.addModule("quicklist", .{
            .root_source_file = b.path("src/QuickList.zig"),
            .target = target,
            .optimize = optimize,
        });

        mod.addCSourceFiles(.{
            .files = &.{
                "src/lzf/lzf_c.c",
                "src/lzf/lzf_d.c",
            },
            .flags = &.{"-DSTRICT_ALIGN=1"},
        });
        mod.addIncludePath(b.path("src/lzf"));

        const quicklist_test = b.addTest(.{
            .root_module = mod,
        });

        const run_tests = b.addRunArtifact(quicklist_test);
        b.step("test-quicklist", "Run quicklist tests").dependOn(&run_tests.step);
    }
}
