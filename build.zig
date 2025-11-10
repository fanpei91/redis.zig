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

fn buildLzf(b: *std.Build, mod: *std.Build.Module) void {
    mod.addCSourceFiles(.{
        .files = &.{
            "src/lzf/lzf_c.c",
            "src/lzf/lzf_d.c",
        },
        .flags = &.{"-DSTRICT_ALIGN=1"},
    });
    mod.addIncludePath(b.path("src/lzf"));
}

fn buildRax(b: *std.Build, mod: *std.Build.Module) void {
    // TODO: use Allocator(-Dmalloc=my_malloc -Dfree=my_free)
    mod.addCSourceFiles(.{
        .files = &.{
            "src/rax/art.c",
        },
    });
    mod.addIncludePath(b.path("src/rax"));
}
