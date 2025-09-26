/// The maximum number of characters needed to represent a long double
/// as a string (long double has a huge range).
/// This should be the size of the buffer given to `ld2string()`
pub const MAX_LONG_DOUBLE_CHARS = 5 * 1024;

pub fn ld2string(buf: []u8, value: longdouble, humanfriendly: bool) []u8 {
    var writer: std.Io.Writer = .fixed(buf);
    writer.printFloat(value, .{
        .mode = if (humanfriendly) .decimal else .scientific,
        .case = .lower,
    }) catch unreachable;
    return writer.buffered();
}

test ld2string {
    var buf: [MAX_LONG_DOUBLE_CHARS]u8 = undefined;

    var str = ld2string(&buf, 0.00011222000, true);
    try expectEqualStrings("0.00011222", str);

    str = ld2string(&buf, 0.0001122000, false);
    try expectEqualStrings("1.122e-4", str);
}

const std = @import("std");
const ctypes = @import("ctypes.zig");
const longdouble = ctypes.longdouble;
const expectEqualStrings = std.testing.expectEqualStrings;
