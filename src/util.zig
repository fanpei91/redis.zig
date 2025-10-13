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
