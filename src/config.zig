const YES_NO_ARG_ERR = "argument must be 'yes' or 'no'";

const configEnum = struct {
    name: []const u8,
    val: i32,
};

const maxmemory_policy_enum = [_]configEnum{
    .{ .name = "volatile-lru", .val = Server.MAXMEMORY_VOLATILE_LRU },
    .{ .name = "volatile-lfu", .val = Server.MAXMEMORY_VOLATILE_LFU },
    .{ .name = "volatile-random", .val = Server.MAXMEMORY_VOLATILE_RANDOM },
    .{ .name = "volatile-ttl", .val = Server.MAXMEMORY_VOLATILE_TTL },
    .{ .name = "allkeys-lru", .val = Server.MAXMEMORY_ALLKEYS_LRU },
    .{ .name = "allkeys-lfu", .val = Server.MAXMEMORY_ALLKEYS_LFU },
    .{ .name = "allkeys-random", .val = Server.MAXMEMORY_ALLKEYS_RANDOM },
    .{ .name = "noeviction", .val = Server.MAXMEMORY_NO_EVICTION },
};

const aof_fsync_enum = [_]configEnum{
    .{ .name = "everysec", .val = Server.AOF_FSYNC_EVERYSEC },
    .{ .name = "always", .val = Server.AOF_FSYNC_ALWAYS },
    .{ .name = "no", .val = Server.AOF_FSYNC_NO },
};

/// Get enum value from name. If there is no match std.math.minInt(i32) is
/// returned.
fn configEnumGetValue(ce: []const configEnum, name: []const u8) i32 {
    for (ce) |e| {
        if (std.ascii.eqlIgnoreCase(e.name, name)) {
            return e.val;
        }
    }
    return std.math.minInt(i32);
}

/// Load the server configuration from the specified filename.
/// The function appends the additional configuration directives stored
/// in the 'options' string to the config file before loading.
///
/// Both filename and options can be null, in such a case are considered
/// empty. This way load() can be used to just load a file or
/// just load a string.
pub fn load(
    server: *Server,
    filename: ?sds.String,
    options: ?sds.String,
) !void {
    var config = sds.empty(allocator.impl);
    defer sds.free(allocator.impl, config);
    var buf: [Server.CONFIG_MAX_LINE]u8 = undefined;

    if (filename) |f| {
        const filepath = sds.asBytes(f);
        var fp: std.fs.File = undefined;
        if (filepath.len == 1 and filepath[0] == '-') {
            fp = std.fs.File.stdin();
        } else {
            fp = std.fs.openFileAbsolute(filepath, .{}) catch |err| {
                log.warn("Can't open config file '{s}'", .{filepath});
                return err;
            };
        }
        defer fp.close();
        while (true) {
            const nread = try fp.read(&buf);
            if (nread == 0) break;
            config = sds.cat(allocator.impl, config, buf[0..nread]);
        }
    }

    if (options) |o| {
        config = sds.cat(allocator.impl, config, "\n");
        config = sds.cat(allocator.impl, config, sds.asBytes(o));
    }

    return try loadFromString(server, config);
}

fn loadFromString(
    server: *Server,
    config: sds.String,
) (error{InvalidConfig})!void {
    const lines = sds.split(allocator.impl, sds.asBytes(config), "\n");
    defer sds.freeSplitRes(allocator.impl, lines);
    var err: ?[]const u8 = null;
    var i: usize = 0;
    var linenum: usize = 0;
    biz: {
        for (lines, 0..) |line, x| {
            i = x;
            linenum = i + 1;

            sds.trim(line, "\t\r\n");
            const bytes = sds.asSentinelBytes(line);

            // Skip comments and blank lines
            if (bytes.len == 0 or bytes[0] == '#') continue;

            // Split into arguments
            const argv = sds.splitArgs(allocator.impl, bytes) orelse {
                err = "Unbalanced quotes in configuration line";
                break :biz;
            };
            defer sds.freeSplitRes(allocator.impl, argv);
            const argc = argv.len;

            // Skip this line if the resulting command vector is empty.
            if (argc == 0) continue;

            // Execute config directives
            sds.toLower(argv[0]);
            const option = sds.asBytes(argv[0]);

            if (eql(option, "hz") and argc == 2) {
                server.config_hz = std.fmt.parseInt(
                    u32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.config_hz < Server.CONFIG_MIN_HZ) {
                    server.config_hz = Server.CONFIG_MIN_HZ;
                }
                if (server.config_hz > Server.CONFIG_MAX_HZ) {
                    server.config_hz = Server.CONFIG_MAX_HZ;
                }
                continue;
            }

            if (eql(option, "dynamic-hz") and argc == 2) {
                server.dynamic_hz = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "tcp-backlog") and argc == 2) {
                server.tcp_backlog = std.fmt.parseInt(
                    u32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch {
                    err = "Invalid backlog value";
                    break :biz;
                };
                continue;
            }

            if (eql(option, "protected-mode") and argc == 2) {
                server.protected_mode = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "requirepass") and argc == 2) {
                const pass = sds.asBytes(argv[1]);
                if (pass.len > Server.CONFIG_AUTHPASS_MAX_LEN) {
                    err = "Password is longer than CONFIG_AUTHPASS_MAX_LEN";
                    break :biz;
                }
                server.requirepass = pwd: {
                    if (pass.len == 0) {
                        break :pwd null;
                    } else {
                        break :pwd allocator.dupe(u8, pass);
                    }
                };
                continue;
            }

            if (eql(option, "bind") and argc >= 2) {
                const addresses = argc - 1;
                if (addresses > Server.CONFIG_BINDADDR_MAX) {
                    err = "Too many bind addresses specified";
                    break :biz;
                }
                for (0..addresses) |j| {
                    server.bindaddr[j] = allocator.dupe(
                        u8,
                        sds.asBytes(argv[j + 1]),
                    );
                }
                server.bindaddr_count = addresses;
                continue;
            }

            if (eql(option, "unixsocket") and argc == 2) {
                server.unixsocket = allocator.dupe(
                    u8,
                    sds.asBytes(argv[1]),
                );
                continue;
            }

            if (eql(option, "unixsocketperm") and argc == 2) {
                err = "Invalid socket file permissions";
                const v = std.fmt.parseInt(
                    std.posix.mode_t,
                    sds.asBytes(argv[1]),
                    8,
                ) catch {
                    break :biz;
                };
                if (v > 0o777) {
                    break :biz;
                }
                server.unixsocketperm = v;
                continue;
            }

            if (eql(option, "port") and argc == 2) {
                server.port = std.fmt.parseInt(
                    u16,
                    sds.asBytes(argv[1]),
                    10,
                ) catch {
                    err = "Invalid port";
                    break :biz;
                };
                continue;
            }

            if (eql(option, "databases") and argc == 2) {
                server.dbnum = std.fmt.parseInt(
                    u32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.dbnum < 1) {
                    err = "Invalid number of databases";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "tcp-keepalive") and argc == 2) {
                server.tcpkeepalive = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch -1;
                if (server.tcpkeepalive < 0) {
                    err = "Invalid tcp-keepalive value";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "client-query-buffer-limit") and argc == 2) {
                server.client_max_querybuf_len = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }

            if (eql(option, "proto-max-bulk-len") and argc == 2) {
                server.proto_max_bulk_len = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }

            if (eql(option, "maxclients") and argc == 2) {
                server.maxclients = std.fmt.parseInt(
                    u32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.maxclients < 1) {
                    err = "Invalid max clients limit";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "maxmemory") and argc == 2) {
                server.maxmemory = memtosize(sds.asBytes(argv[1])) catch 0;
                continue;
            }

            if (eql(option, "maxmemory-policy") and argc == 2) {
                server.maxmemory_policy = configEnumGetValue(
                    &maxmemory_policy_enum,
                    sds.asBytes(argv[1]),
                );
                if (server.maxmemory_policy == std.math.minInt(i32)) {
                    err = "Invalid maxmemory policy";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "maxmemory-samples") and argc == 2) {
                server.maxmemory_samples = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.maxmemory_samples <= 0) {
                    err = "maxmemory-samples must be 1 or greater";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "lfu-log-factor") and argc == 2) {
                server.lfu_log_factor = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.lfu_log_factor < 0) {
                    err = "lfu-log-factor must be 0 or greater";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "lfu-decay-time") and argc == 2) {
                server.lfu_decay_time = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                if (server.lfu_decay_time < 0) {
                    err = "lfu-decay-time must be 0 or greater";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "timeout") and argc == 2) {
                server.maxidletime = std.fmt.parseInt(
                    u32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch {
                    err = "Invalid timeout value";
                    break :biz;
                };
                continue;
            }

            if (eql(option, "lazyfree-lazy-expire") and argc == 2) {
                server.lazyfree_lazy_expire = parseYesNo(
                    sds.asBytes(argv[1]),
                ) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "list-max-ziplist-size") and argc == 2) {
                server.list_max_ziplist_size = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch continue;
                continue;
            }

            if (eql(option, "list-compress-depth") and argc == 2) {
                server.list_compress_depth = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch continue;
                continue;
            }

            if (eql(option, "hash-max-ziplist-value") and argc == 2) {
                server.hash_max_ziplist_value = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }
            if (eql(option, "hash-max-ziplist-entries") and argc == 2) {
                server.hash_max_ziplist_entries = std.fmt.parseInt(
                    usize,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                continue;
            }

            if (eql(option, "zset-max-ziplist-value") and argc == 2) {
                server.zset_max_ziplist_value = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }
            if (eql(option, "zset-max-ziplist-entries") and argc == 2) {
                server.zset_max_ziplist_entries = std.fmt.parseInt(
                    usize,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                continue;
            }

            if (eql(option, "set-max-intset-entries") and argc == 2) {
                server.set_max_intset_entries = std.fmt.parseInt(
                    usize,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                continue;
            }

            if (eql(option, "stream-node-max-bytes") and argc == 2) {
                server.stream_node_max_bytes = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }

            if (eql(option, "stream-node-max-entries") and argc == 2) {
                server.stream_node_max_entries = std.fmt.parseInt(
                    i64,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                continue;
            }

            if (eql(option, "lua-time-limit") and argc == 2) {
                server.lua.time_limit = std.fmt.parseInt(
                    i64,
                    sds.asBytes(argv[1]),
                    10,
                ) catch 0;
                server.lua.time_limit = @max(server.lua.time_limit, 0);
                continue;
            }

            if (eql(option, "rdb-save-incremental-fsync") and argc == 2) {
                server.rdb_save_incremental_fsync = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "dbfilename") and argc == 2) {
                const path = sds.asSentinelBytes(argv[1]);
                if (!util.pathIsBaseName(path)) {
                    err = "dbfilename can't be a path, just a filename";
                    break :biz;
                }
                allocator.free(server.rdb_filename);
                server.rdb_filename = allocator.dupe(u8, path);
                continue;
            }

            if (eql(option, "rdbchecksum") and argc == 2) {
                server.rdb_checksum = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "rdbcompression") and argc == 2) {
                server.rdb_compression = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                continue;
            }

            if (eql(option, "save")) {
                if (argc == 3) {
                    const seconds = std.fmt.parseInt(
                        i32,
                        sds.asBytes(argv[1]),
                        10,
                    ) catch 0;
                    const changes = std.fmt.parseInt(
                        i32,
                        sds.asBytes(argv[2]),
                        10,
                    ) catch -1;
                    if (seconds < 1 or changes < 0) {
                        err = "Invalid save parameters";
                        break :biz;
                    }
                    appendServerSaveParams(server, seconds, changes);
                } else if (argc == 2 and eql(sds.asBytes(argv[1]), "")) {
                    resetServerSaveParams(server);
                }
                continue;
            }

            if (eql(option, "appendonly") and argc == 2) {
                const yes = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                server.aof_state = if (yes) Server.AOF_ON else Server.AOF_OFF;
                continue;
            }

            if (eql(option, "appendfilename") and argc == 2) {
                const path = sds.asSentinelBytes(argv[1]);
                if (!util.pathIsBaseName(path)) {
                    err = "appendfilename can't be a path, just a filename";
                    break :biz;
                }
                allocator.free(server.aof_filename);
                server.aof_filename = allocator.dupe(u8, path);
                continue;
            }

            if (eql(option, "appendfsync") and argc == 2) {
                server.aof_fsync = configEnumGetValue(
                    &aof_fsync_enum,
                    sds.castBytes(argv[1]),
                );
                if (server.aof_fsync == std.math.minInt(i32)) {
                    err = "argument must be 'no', 'always' or 'everysec'";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "no-appendfsync-on-rewrite") and argc == 2) {
                const yes = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                server.aof_no_fsync_on_rewrite = yes;
                continue;
            }

            if (eql(option, "aof-load-truncated") and argc == 2) {
                const yes = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                server.aof_load_truncated = yes;
                continue;
            }

            if (eql(option, "aof-rewrite-incremental-fsync") and argc == 2) {
                const yes = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                server.aof_rewrite_incremental_fsync = yes;
                continue;
            }

            if (eql(option, "aof-use-rdb-preamble") and argc == 2) {
                const yes = parseYesNo(sds.asBytes(argv[1])) catch {
                    err = YES_NO_ARG_ERR;
                    break :biz;
                };
                server.aof_use_rdb_preamble = yes;
                continue;
            }

            if (eql(option, "auto-aof-rewrite-percentage") and argc == 2) {
                server.aof_rewrite_perc = std.fmt.parseInt(
                    i32,
                    sds.asBytes(argv[1]),
                    10,
                ) catch -1;
                if (server.aof_rewrite_perc < 0) {
                    err = "Invalid negative percentage for AOF auto rewrite";
                    break :biz;
                }
                continue;
            }

            if (eql(option, "auto-aof-rewrite-min-size") and argc == 2) {
                server.aof_rewrite_min_size = memtosize(
                    sds.asBytes(argv[1]),
                ) catch 0;
                continue;
            }

            err = "Bad directive or wrong number of arguments";
            break :biz;
        }
        return;
    }
    log.err("*** CONFIG FILE ERROR ***", .{});
    log.err("Reading the configuration file, at line {}", .{linenum});
    log.err(">>> '{s}'", .{sds.asBytes(lines[i])});
    if (err) |e| {
        log.err("{s}", .{e});
    }
    return error.InvalidConfig;
}

inline fn eql(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

fn parseYesNo(s: []const u8) error{InvalidArgument}!bool {
    if (std.ascii.eqlIgnoreCase(s, "yes")) {
        return true;
    }
    if (std.ascii.eqlIgnoreCase(s, "no")) {
        return false;
    }
    return error.InvalidArgument;
}

/// Convert a string representing an amount of memory into the number of
/// bytes, so for instance memtoll("1Gb") will return 1073741824 that is
/// (1024*1024*1024).
fn memtosize(
    s: []const u8,
) (error{InvalidUnit} || std.fmt.ParseIntError)!usize {
    if (s.len == 0) return error.InvalidUnit;
    var mem = s;
    if (mem[0] == '-') mem = mem[1..];
    if (mem.len == 0) return error.InvalidUnit;

    var i: usize = 0;
    for (mem, 0..) |c, j| {
        if (!std.ascii.isDigit(c)) {
            i = j;
            break;
        }
    }
    const caseEql = std.ascii.eqlIgnoreCase;
    var mul: usize = 0;
    const unit = mem[i..];
    if (i == 0 or caseEql(unit, "b")) {
        mul = 1;
    } else if (caseEql(unit, "k")) {
        mul = 1000;
    } else if (caseEql(unit, "kb")) {
        mul = 1024;
    } else if (caseEql(unit, "m")) {
        mul = 1000 * 1000;
    } else if (caseEql(unit, "mb")) {
        mul = 1024 * 1024;
    } else if (caseEql(unit, "g")) {
        mul = 1000 * 1000 * 1000;
    } else if (caseEql(unit, "gb")) {
        mul = 1024 * 1024 * 1024;
    } else if (caseEql(unit, "t")) {
        mul = 1000 * 1000 * 1000 * 1000;
    } else if (caseEql(unit, "tb")) {
        mul = 1024 * 1024 * 1024 * 1024;
    } else {
        return error.InvalidUnit;
    }
    const digits = if (i == 0) mem[0..] else mem[0..i];
    const val = try std.fmt.parseInt(usize, digits, 10);
    return val * mul;
}

test memtosize {
    try expectEqual(
        2 * 1024 * 1024,
        try memtosize("2mb"),
    );
    try expectEqual(
        2,
        try memtosize("2"),
    );
    try expectEqual(
        2,
        try memtosize("-2"),
    );
    try std.testing.expectError(
        std.fmt.ParseIntError.InvalidCharacter,
        memtosize("mb"),
    );
    try std.testing.expectError(
        error.InvalidUnit,
        memtosize(""),
    );
    try std.testing.expectError(
        error.InvalidUnit,
        memtosize("-"),
    );
}

pub fn appendServerSaveParams(server: *Server, seconds: i32, changes: i32) void {
    server.saveparams.append(allocator.impl, .{
        .seconds = seconds,
        .changes = changes,
    }) catch allocator.oom();
}

pub fn resetServerSaveParams(server: *Server) void {
    server.saveparams.deinit(allocator.impl);
    server.saveparams = .empty;
}

// pub fn fsync(fd: posix.fd_t) posix.SyncError!void {
//     if (builtin.os.tag == .linux) {
//         return posix.fdatasync(fd);
//     }
//     return posix.fsync(fd);
// }

pub const fsync = if (builtin.os.tag == .linux)
    posix.fdatasync
else
    posix.fsync;

const posix = std.posix;
const builtin = @import("builtin");
const std = @import("std");
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const Server = @import("Server.zig");
const log = std.log.scoped(.config);
const expectEqual = std.testing.expectEqual;
const util = @import("util.zig");
