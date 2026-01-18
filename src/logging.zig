pub fn debug(comptime format: []const u8, args: anytype) void {
    raw(.debug, format, args);
}

pub fn verbose(comptime format: []const u8, args: anytype) void {
    raw(.verbose, format, args);
}

pub fn notice(comptime format: []const u8, args: anytype) void {
    raw(.notice, format, args);
}

pub fn warn(comptime format: []const u8, args: anytype) void {
    raw(.warning, format, args);
}

pub fn raw(level: Level, comptime format: []const u8, args: anytype) void {
    if (@intFromEnum(level) < @intFromEnum(server.verbosity)) {
        return;
    }
    //  TODO: sys log
    switch (level) {
        .debug => log.debug(format, args),
        .verbose, .notice, .raw => log.info(format, args),
        .warning => log.warn(format, args),
    }
}

pub const Level = enum(i32) {
    debug = 0,
    verbose = 1,
    notice = 2,
    warning = 3,
    raw = 1 << 10, // Modifier to log without timestamp
};

const std = @import("std");
const Server = @import("Server.zig");
const server = &Server.instance;

const log = std.log.scoped(.logging);
