pub fn debug(comptime format: []const u8, args: anytype) void {
    raw(DEBUG, format, args);
}

pub fn verbose(comptime format: []const u8, args: anytype) void {
    raw(VERBOSE, format, args);
}

pub fn notice(comptime format: []const u8, args: anytype) void {
    raw(NOTICE, format, args);
}

pub fn warn(comptime format: []const u8, args: anytype) void {
    raw(WARNING, format, args);
}

pub fn raw(level: Level, comptime format: []const u8, args: anytype) void {
    _ = level;
    // TODO:
    log.debug(format, args);
}

pub const Level = i32;
pub const DEBUG: Level = 0;
pub const VERBOSE: Level = 1;
pub const NOTICE: Level = 2;
pub const WARNING: Level = 3;
pub const RAW: Level = (1 << 10); // Modifier to log without timestamp

const std = @import("std");
const log = std.log.scoped(.logging);
