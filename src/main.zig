pub fn main() !void {
    random.seed(null);
    hasher.seed(null);

    const argv = try std.process.argsAlloc(allocator.impl);
    defer std.process.argsFree(allocator.impl, argv);

    var options: sds.String = sds.empty(allocator.impl);
    defer sds.free(allocator.impl, options);

    var configfile: ?sds.String = null;
    defer if (configfile) |f| sds.free(allocator.impl, f);

    if (argv.len >= 2) {
        // First option to parse in argv[]
        var j: usize = 1;

        if (std.mem.eql(u8, argv[1], "--help") or std.mem.eql(u8, argv[1], "-h")) {
            printUsage();
        }

        // First argument is the config file name?
        if (argv[j][0] != '-' or argv[j][1] != '-') {
            configfile = try util.getAbsolutePath(argv[j]);
            j += 1;
        }

        // All the other options are parsed and conceptually appended to the
        // configuration file. For instance --port 6380 will generate the
        // string "port 6380\n" to be parsed after the actual file name
        // is parsed, if any.
        while (j != argv.len) : (j += 1) {
            if (argv[j][0] == '-' and argv[j][1] == '-') {
                // option name
                if (sds.getLen(options) != 0) {
                    options = sds.cat(allocator.impl, options, "\n");
                }
                options = sds.cat(allocator.impl, options, argv[j][2..]);
                options = sds.cat(allocator.impl, options, " ");
            } else {
                // option argument
                options = sds.catRepr(allocator.impl, options, argv[j]);
                options = sds.cat(allocator.impl, options, " ");
            }
        }
    }

    if (argv.len == 1) {
        logging.warn(
            "no config file specified, " ++
                "using the default config. " ++
                "In order to specify a config file use {s} /path/to/{s}.conf,",
            .{
                argv[0],
                "redis",
            },
        );
    } else {
        logging.warn("Configuration loaded", .{});
    }

    try Server.create(configfile, options);
    var server = &Server.instance;

    logging.notice("Running mode={s}, port={}", .{ "standalone", server.port });

    try checkTcpBacklogSettings(server);
    checkMaxMemorySettings(server);
    loadDataFromDisk(server);
    try server.up();
}

fn checkTcpBacklogSettings(server: *const Server) !void {
    if (builtin.os.tag != .linux) return;

    const f = try std.fs.openFileAbsolute("/proc/sys/net/core/somaxconn", .{});
    defer f.close();

    var buf: [1024]u8 = undefined;
    const read = try f.read(&buf);
    const val = std.mem.trim(u8, buf[0..read], "\n");
    const somaxconn = std.fmt.parseInt(i32, val, 10) catch 0;
    if (somaxconn > 0 and somaxconn < server.tcp_backlog) {
        logging.warn(
            "The TCP backlog setting of {} cannot be enforced " ++
                "because /proc/sys/net/core/somaxconn is set to the lower value of {}.",
            .{
                server.tcp_backlog,
                somaxconn,
            },
        );
    }
}

fn checkMaxMemorySettings(server: *const Server) void {
    // Warning the user about suspicious maxmemory setting.
    if (server.maxmemory > 0 and server.maxmemory < 1024 * 1024) {
        logging.warn(
            "You specified a maxmemory value that is less than 1MB " ++
                "(current value is {} bytes). Are you sure this is what you really want?",
            .{
                server.maxmemory,
            },
        );
    }
}

/// Function called at startup to load RDB or AOF file in memory.
fn loadDataFromDisk(server: *const Server) void {
    const start = std.time.microTimestamp();
    if (server.aof_state == Server.AOF_ON) {
        aof.loadAppendOnlyFile(server.aof_filename);
        const elapsed: f32 = @floatFromInt(std.time.microTimestamp() - start);
        logging.notice(
            "DB loaded from append only file: {:.3} seconds",
            .{elapsed / std.time.us_per_s},
        );
    } else {
        rdb.load(server.rdb_filename) catch |err| {
            if (err != error.FileNotFound) {
                logging.warn(
                    "Fatal error loading the DB: {}. Existing.",
                    .{err},
                );
                std.process.exit(1);
            }
            return;
        };
        const elapsed: f32 = @floatFromInt(std.time.microTimestamp() - start);
        logging.notice(
            "DB loaded from disk: {:.3} seconds",
            .{elapsed / std.time.us_per_s},
        );
    }
}

fn printUsage() void {
    std.debug.print("Usage: ./redis-server [/path/to/redis.conf] [options]\n", .{});
    std.debug.print("       ./redis-server -h or --help\n", .{});
    std.debug.print("Examples: \n", .{});
    std.debug.print("       ./redis-server (run the server with default conf)\n", .{});
    std.debug.print("       ./redis-server /etc/redis/6399.conf\n", .{});
    std.debug.print("       ./redis-server --port 7777\n", .{});
    std.debug.print("       ./redis-server /etc/myredis.conf --loglevel verbose\n", .{});
    std.process.exit(0);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const hasher = @import("hasher.zig");
const random = @import("random.zig");
const util = @import("util.zig");
const config = @import("config.zig");
const Server = @import("Server.zig");
const builtin = @import("builtin");
const rdb = @import("rdb.zig");
const aof = @import("aof.zig");
const logging = @import("logging.zig");
