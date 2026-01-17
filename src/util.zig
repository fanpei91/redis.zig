/// Glob-style pattern matching.
pub fn stringmatch(pattern: [:0]const u8, string: [:0]const u8, nocase: bool) bool {
    var patternlen = pattern.len;
    var stringlen = string.len;

    var pat = pattern.ptr;
    var str = string.ptr;
    while (patternlen > 0 and stringlen > 0) {
        switch (pat[0]) {
            '*' => {
                while (pat[1] == '*') {
                    pat += 1;
                    patternlen -= 1;
                }
                if (patternlen == 1) {
                    return true;
                }
                while (stringlen > 0) {
                    if (stringmatch(
                        pattern[@intFromPtr(pat + 1) - @intFromPtr(pattern.ptr) ..],
                        string[@intFromPtr(str) - @intFromPtr(string.ptr) ..],
                        nocase,
                    )) {
                        return true;
                    }
                    str += 1;
                    stringlen -= 1;
                }
                return false;
            },
            '?' => {
                if (stringlen == 0) {
                    return false;
                }
                str += 1;
                stringlen -= 1;
            },
            '[' => {
                pat += 1;
                patternlen -= 1;
                const not = pat[0] == '^';
                if (not) {
                    pat += 1;
                    patternlen -= 1;
                }
                var match = false;
                while (true) {
                    if (pat[0] == '\\' and patternlen >= 2) {
                        pat += 1;
                        patternlen -= 1;
                        if (pat[0] == str[0]) {
                            match = true;
                        }
                    } else if (pat[0] == ']') {
                        break;
                    } else if (patternlen == 0) {
                        pat -= 1;
                        patternlen += 1;
                        break;
                    } else if (pat[1] == '-' and patternlen >= 3) {
                        var start = pat[0];
                        var end = pat[2];
                        var c = str[0];
                        if (start > end) {
                            std.mem.swap(u8, &start, &end);
                        }
                        if (nocase) {
                            start = toLower(start);
                            end = toLower(end);
                            c = toLower(c);
                        }
                        pat += 2;
                        patternlen -= 2;
                        if (c >= start and c <= end) {
                            match = true;
                        }
                    } else {
                        if (nocase) {
                            if (toLower(pat[0]) == toLower(str[0])) {
                                match = true;
                            }
                        } else {
                            if (pat[0] == str[0]) {
                                match = true;
                            }
                        }
                    }
                    pat += 1;
                    patternlen -= 1;
                }
                if (not) {
                    match = !match;
                }
                if (!match) {
                    return false;
                }
                str += 1;
                stringlen -= 1;
            },
            else => {
                if (pat[0] == '\\') {
                    if (patternlen >= 2) {
                        pat += 1;
                        patternlen -= 1;
                    }
                }
                if (nocase) {
                    if (toLower(pat[0]) != toLower(str[0])) {
                        return false;
                    }
                } else {
                    if (pat[0] != str[0]) {
                        return false;
                    }
                }
                str += 1;
                stringlen -= 1;
            },
        }

        pat += 1;
        patternlen -= 1;
        if (stringlen == 0) {
            while (pat[0] == '*') {
                pat += 1;
                patternlen -= 1;
            }
            break;
        }
    }
    if (patternlen == 0 and stringlen == 0) {
        return true;
    }
    return false;
}

/// The maximum number of characters needed to represent a long double
/// as a string (long double has a huge range).
/// This should be the size of the buffer given to `ld2string()`
pub const MAX_LONG_DOUBLE_CHARS = 5 * 1024;

pub fn ld2string(buf: []u8, value: f80, humanfriendly: bool) []u8 {
    assert(buf.len >= MAX_LONG_DOUBLE_CHARS);
    if (std.math.isInf(value)) {
        var output: []u8 = undefined;
        if (value > 0) {
            output = buf[0..3];
            @memcpy(output, "inf");
        } else {
            output = buf[0..4];
            @memcpy(output, "-inf");
        }
        return output;
    }

    var writer: std.Io.Writer = .fixed(buf);
    writer.printFloat(value, .{
        .mode = if (humanfriendly) .decimal else .scientific,
        .case = .lower,
        .precision = 17,
    }) catch unreachable;
    var output = writer.buffered();
    if (humanfriendly and std.mem.indexOfScalar(u8, output, '.') != null) {
        output = @constCast(std.mem.trimEnd(u8, output, "0"));
        if (output[output.len - 1] == '.') {
            output = output[0 .. output.len - 1];
        }
    }
    return output;
}

/// Convert a double to a string representation. The representation should
/// always be parsable by std.fmt.parseFloat().
/// This function does not support human-friendly formatting like ld2string
/// does. It is intended mainly to be used inside t_zset.zig when writing
/// scores into a ziplist representing a sorted set.
pub fn d2string(buf: []u8, value: f64) []const u8 {
    var len: c_int = undefined;
    if (isNan(value)) {
        len = libc.snprintf(buf.ptr, buf.len, "nan");
    } else if (isInf(value)) {
        if (value < 0) {
            len = libc.snprintf(buf.ptr, buf.len, "-inf");
        } else {
            len = libc.snprintf(buf.ptr, buf.len, "inf");
        }
    } else if (value == 0) {
        // See: http://en.wikipedia.org/wiki/Signed_zero#Comparisons
        if (1.0 / value < 0) {
            len = libc.snprintf(buf.ptr, buf.len, "-0");
        } else {
            len = libc.snprintf(buf.ptr, buf.len, "0");
        }
    } else {
        len = libc.snprintf(buf.ptr, buf.len, "%.17g", value);
    }
    return buf[0..@intCast(len)];
}

pub fn ll2string(buf: []u8, value: i64) []u8 {
    assert(buf.len >= 20);
    return std.fmt.bufPrint(buf, "{d}", .{value}) catch {
        unreachable;
    };
}

pub fn string2ll(str: []const u8) ?i64 {
    const v = std.fmt.parseInt(i64, str, 10) catch {
        return null;
    };
    return v;
}

pub fn string2ull(str: []const u8) ?u64 {
    const v = std.fmt.parseInt(u64, str, 10) catch {
        return null;
    };
    return v;
}

/// Like digits10() but for signed values.
pub fn sdigits10(v: i64) u32 {
    if (v < 0) {
        const uv: u64 = @abs(v);
        return digits10(uv) + 1; // +1 for the minus.
    }
    return digits10(@intCast(v));
}

/// Return the number of digits of 'v' when converted to string in radix 10.
/// See ll2string() for more information.
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
    return sds.new(allocator.impl, realpath);
}

/// Return true if the specified path is just a file basename without any
/// relative or absolute path. This function just checks that no / or \
/// character exists inside the specified path, that's enough in the
/// environments where Redis runs.
pub fn pathIsBaseName(path: []const u8) bool {
    return std.mem.indexOfScalar(u8, path, '/') == null and
        std.mem.indexOfScalar(u8, path, '\\') == null;
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

const std = @import("std");
const expectEqualStrings = std.testing.expectEqualStrings;
const assert = std.debug.assert;
const expectEqual = std.testing.expectEqual;
const sds = @import("sds.zig");
const allocator = @import("allocator.zig");
const toLower = std.ascii.toLower;
const libc = @cImport({
    @cInclude("stdio.h");
});
const isNan = std.math.isNan;
const isInf = std.math.isInf;
