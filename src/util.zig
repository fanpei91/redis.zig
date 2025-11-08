/// The maximum number of characters needed to represent a long double
/// as a string (long double has a huge range).
/// This should be the size of the buffer given to `ld2string()`
pub const MAX_LONG_DOUBLE_CHARS = 5 * 1024;

pub fn ld2string(buf: []u8, value: f80, humanfriendly: bool) []u8 {
    assert(buf.len >= MAX_LONG_DOUBLE_CHARS);
    var writer: std.Io.Writer = .fixed(buf);
    writer.printFloat(value, .{
        .mode = if (humanfriendly) .decimal else .scientific,
        .case = .lower,
    }) catch unreachable;
    return writer.buffered();
}

pub fn ll2string(buf: []u8, value: i64) []u8 {
    assert(buf.len >= 20);
    return std.fmt.bufPrint(buf, "{d}", .{value}) catch unreachable;
}

pub fn string2l(str: []const u8, lval: *i64) bool {
    lval.* = std.fmt.parseInt(i64, str, 0) catch return false;
    return true;
}

pub fn string2ll(str: []const u8, llval: *i64) bool {
    return string2l(str, llval);
}

pub fn sdigits10(v: i64) u32 {
    if (v < 0) {
        const uv: u64 = @abs(v);
        return digits10(uv) + 1; // +1 for the minus.
    }
    return digits10(@intCast(v));
}

/// Return the number of digits of 'v' when converted to string in radix 10.
// See ll2string() for more information.
pub fn digits10(v: u64) u32 {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000) {
        if (v < 100000000) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return @as(u8, 5) + @intFromBool(v >= 100000);
            }
            return @as(u8, 7) + @intFromBool(v >= 10000000);
        }
        if (v < 10000000000) {
            return @as(u8, 9) + @intFromBool(v >= 1000000000);
        }
        return @as(u8, 11) + @intFromBool(v >= 100000000000);
    }
    return 12 + digits10(v / 1000000000000);
}

/// Given the filename, return the absolute path as an SDS string, or NULL
/// if it fails for some reason. Note that "filename" may be an absolute path
/// already, this will be detected and handled correctly.
pub fn getAbsolutePath(filename: []const u8) !sds.String {
    var buffer: [std.fs.max_path_bytes]u8 = undefined;
    const realpath = try std.fs.realpath(filename, &buffer);
    return sds.new(realpath);
}

/// Return true if strings are the same, false if they are not.
/// The comparison is performed in a way that prevents an attacker to obtain
/// information about the nature of the strings just monitoring the execution
/// time of the function.
///
/// Note that limiting the comparison length to strings up to max_len bytes we
/// can avoid leaking any information about the password length and any
/// possible branch misprediction related leak.
pub fn timeIndependentEql(
    comptime max_len: usize,
    a: []const u8,
    b: []const u8,
) bool {
    if (a.len > max_len or b.len > max_len) {
        return false;
    }

    var bufa: [max_len]u8 = @splat(0);
    var bufb: [max_len]u8 = @splat(0);

    @memcpy(bufa[0..a.len], a);
    @memcpy(bufb[0..b.len], b);

    var diff: usize = 0;
    for (0..max_len) |i| {
        diff |= (bufa[i] ^ bufb[i]);
    }

    diff |= (a.len ^ b.len);
    return diff == 0;
}

test sdigits10 {
    try expectEqual(20, sdigits10(std.math.minInt(i64)));
    try expectEqual(19, sdigits10(std.math.maxInt(i64)));
}

test ld2string {
    var buf: [MAX_LONG_DOUBLE_CHARS]u8 = undefined;

    var str = ld2string(&buf, 0.00011222000, true);
    try expectEqualStrings("0.00011222", str);

    str = ld2string(&buf, 0.0001122000, false);
    try expectEqualStrings("1.122e-4", str);
}

const std = @import("std");
const expectEqualStrings = std.testing.expectEqualStrings;
const assert = std.debug.assert;
const expectEqual = std.testing.expectEqual;
const sds = @import("sds.zig");
const allocator = @import("allocator.zig");
