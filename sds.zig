const MemSizedHdr5 = extern struct {
    alloc: u8 align(1),
    hdr: Hdr5 align(1),
};

const Hdr5 = extern struct {
    flags: u8 align(1), // 3 lsb of type, and 5 msb of string length
    buf: [0]u8,

    fn alloc(s: Sds) *u8 {
        const hdr: *Hdr5 = @ptrCast(s - @sizeOf(Hdr5));
        const parent: *MemSizedHdr5 = @fieldParentPtr("hdr", hdr);
        return &parent.alloc;
    }
};

const Hdr8 = extern struct {
    len: u8 align(1),
    alloc: u8 align(1), // excluding the header
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,
};

const Hdr16 = extern struct {
    len: u16 align(1),
    alloc: u16 align(1), // excluding the header
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,
};

const Hdr32 = extern struct {
    len: u32 align(1),
    alloc: u32 align(1), // excluding the header
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,
};

const Hdr64 = extern struct {
    len: u64 align(1),
    alloc: u64 align(1),
    flags: u8 align(1), // 3 lsb of type, 5 unused bits
    buf: [0]u8,
};

const MAX_PREALLOC = 1024 * 1024;

const TYPE_MASK = 0b00000111;
const TYPE_BITS = 3;
const TYPE_5 = 0;
const TYPE_8 = 1;
const TYPE_16 = 2;
const TYPE_32 = 3;
const TYPE_64 = 4;

pub const Sds = [*]u8;

pub fn new(allocator: Allocator, init: []const u8) Allocator.Error!Sds {
    // Empty strings are usually created in order to append. Use type 8
    // since type 5 is not good at this.
    var typ = reqType(init.len);
    if (typ == TYPE_5 and init.len == 0) {
        typ = TYPE_8;
    }

    const hdr_len = hdrSize(typ);
    const mem_size = hdr_len + init.len;
    const mem = try allocator.alloc(u8, mem_size);

    const s = mem.ptr + hdr_len;
    setType(s, typ);
    setLength(s, init.len);
    setAlloc(s, init.len);
    setBuf(s, init);
    return s;
}

pub fn empty(allocator: Allocator) Allocator.Error!Sds {
    return new(allocator, "");
}

pub fn fromLonglong(
    allocator: Allocator,
    num: longlong,
) Allocator.Error!Sds {
    var buf: [20]u8 = undefined;
    const digits = std.fmt.bufPrint(&buf, "{d}", .{num}) catch unreachable;
    return new(allocator, digits);
}

pub fn dupe(allocator: Allocator, s: Sds) Allocator.Error!Sds {
    return new(allocator, s[0..getLen(s)]);
}

pub fn clear(s: Sds) void {
    setLength(s, 0);
}

pub fn makeRoomFor(
    allocator: Allocator,
    s: Sds,
    add_len: usize,
) Allocator.Error!Sds {
    if (getAvail(s) >= add_len) return s;

    const old_len = getLen(s);
    var new_alloc = old_len + add_len;
    if (new_alloc < MAX_PREALLOC) {
        new_alloc *= 2;
    } else {
        new_alloc += MAX_PREALLOC;
    }
    var new_type = reqType(new_alloc);
    // Don't use type 5: the user is appending to the string and type 5 is
    // not able to remember empty space, so makeRoomFor() must be called
    // at every appending operation.
    if (new_type == TYPE_5) {
        new_type = TYPE_8;
    }
    const new_hdr_len = hdrSize(new_type);
    const new_mem_size = new_hdr_len + new_alloc;

    const old_mem = memSlice(s);
    const old_type = getType(s);
    if (old_type == new_type) {
        const new_mem = try allocator.realloc(
            old_mem,
            new_mem_size,
        );
        const ns: [*]u8 = new_mem.ptr + new_hdr_len;
        setAlloc(ns, new_alloc);
        return ns;
    }

    // Since the header size changes, need to move the string forward,
    // and can't use realloc.
    const new_mem = try allocator.alloc(u8, new_mem_size);
    const ns: [*]u8 = new_mem.ptr + new_hdr_len;
    setType(ns, new_type);
    setLength(ns, old_len);
    setAlloc(ns, new_alloc);
    setBuf(ns, s[0..old_len]);
    allocator.free(old_mem);
    return ns;
}

pub fn cat(allocator: Allocator, s: Sds, src: []const u8) Allocator.Error!Sds {
    const cur_len = getLen(s);
    const ns = try makeRoomFor(allocator, s, src.len);
    @memcpy(ns[cur_len .. cur_len + src.len], src);
    setLength(ns, cur_len + src.len);
    return ns;
}

pub fn catPrintf(
    allocator: Allocator,
    s: Sds,
    comptime fmt: []const u8,
    args: anytype,
) (Allocator.Error)!Sds {
    var static_buf: [1024]u8 = undefined;
    const buf = std.fmt.bufPrint(&static_buf, fmt, args) catch {
        @branchHint(.unlikely);
        const alloc_buf = try std.fmt.allocPrint(allocator, fmt, args);
        defer allocator.free(alloc_buf);
        return cat(allocator, s, alloc_buf);
    };
    return cat(allocator, s, buf);
}

pub fn catRepr(
    allocator: Allocator,
    s: Sds,
    raw: []const u8,
) Allocator.Error!Sds {
    var ns = try cat(allocator, s, "\"");
    errdefer free(allocator, ns);

    for (raw) |b| {
        switch (b) {
            '\\', '"' => ns = try catPrintf(allocator, ns, "\\{c}", .{b}),
            '\n' => ns = try cat(allocator, ns, "\\n"),
            '\r' => ns = try cat(allocator, ns, "\\r"),
            '\t' => ns = try cat(allocator, ns, "\\t"),
            // in c: '\a'
            0x07 => ns = try cat(allocator, ns, "\\a"),
            // in c: '\b'
            0x08 => ns = try cat(allocator, ns, "\\b"),
            else => {
                if (std.ascii.isPrint(b)) {
                    ns = try catPrintf(allocator, ns, "{c}", .{b});
                } else {
                    ns = try catPrintf(allocator, ns, "\\x{x:0>2}", .{b});
                }
            },
        }
    }
    ns = try cat(allocator, ns, "\"");
    return ns;
}

pub fn removeAvailSpace(allocator: Allocator, s: Sds) Allocator.Error!Sds {
    if (getAvail(s) == 0) return s;

    const len = getLen(s);
    const new_type = reqType(len);
    const old_mem = memSlice(s);

    // If the type is the same, or at least a large enough type is still
    // required, we just realloc(), letting the allocator to do the copy
    // only if really needed. Otherwise if the change is huge, we manually
    // reallocate the string to use the different header type.
    const old_type = getType(s);
    if (old_type == new_type or new_type > TYPE_8) {
        const old_hdr_len = hdrSize(old_type);
        const new_mem = try allocator.realloc(
            old_mem,
            old_hdr_len + len,
        );
        const ns = new_mem.ptr + old_hdr_len;
        setAlloc(ns, len);
        return ns;
    }

    const new_hdr_len = hdrSize(new_type);
    const new_mem = try allocator.alloc(u8, new_hdr_len + len);
    const ns = new_mem.ptr + new_hdr_len;
    setType(ns, new_type);
    setLength(ns, len);
    setAlloc(ns, len);
    setBuf(ns, s[0..len]);
    allocator.free(old_mem);
    return ns;
}

pub fn incrLen(s: Sds, incr: isize) void {
    const len = getLen(s);
    const alloc = getAlloc(s);
    const abs_incr: usize = @abs(incr);

    assert((incr > 0 and alloc - len >= abs_incr) or (incr < 0 and len >= abs_incr));
    const new_len = if (incr > 0) len + abs_incr else len - abs_incr;
    setLength(s, new_len);
}

pub fn growZero(
    allocator: Allocator,
    s: Sds,
    new_len: usize,
) Allocator.Error!Sds {
    const curr_len = getLen(s);
    if (new_len <= curr_len) return s;

    const add_len = new_len - curr_len;
    const ns = try makeRoomFor(allocator, s, add_len);
    @memset(ns[curr_len..new_len], 0);
    setLength(ns, new_len);
    return ns;
}

pub fn copy(allocator: Allocator, s: Sds, src: []const u8) Allocator.Error!Sds {
    var ns = s;
    if (getAlloc(s) < src.len) {
        ns = try makeRoomFor(allocator, s, src.len - getLen(s));
    }
    setLength(ns, src.len);
    setBuf(ns, src);
    return ns;
}

pub fn join(
    allocator: Allocator,
    slices: []const []const u8,
    sep: []const u8,
) Allocator.Error!Sds {
    var joined = try empty(allocator);
    errdefer free(allocator, joined);
    for (slices, 0..) |slice, i| {
        joined = try cat(allocator, joined, slice);
        if (i != slices.len - 1) joined = try cat(allocator, joined, sep);
    }
    return joined;
}

pub fn split(
    allocator: Allocator,
    str: []const u8,
    sep: []const u8,
) Allocator.Error![]Sds {
    var tokens = std.ArrayList(Sds).empty;
    errdefer {
        for (tokens.items) |token| {
            free(allocator, token);
        }
        tokens.deinit(allocator);
    }

    var it = std.mem.splitSequence(u8, str, sep);
    while (it.next()) |token| {
        try tokens.append(allocator, try new(allocator, token));
    }
    return tokens.toOwnedSlice(allocator);
}

pub fn freeSplitRes(allocator: Allocator, tokens: []Sds) void {
    for (tokens) |token| {
        free(allocator, token);
    }
    allocator.free(tokens);
}

pub fn trim(s: Sds, values_to_strip: []const u8) void {
    const trimed = std.mem.trim(u8, bufSlice(s), values_to_strip);
    @memmove(s[0..trimed.len], trimed);
    setLength(s, trimed.len);
}

pub fn range(s: Sds, start: isize, endinc: isize) void {
    const len = getLen(s);
    if (len == 0) return;

    var from: usize = @abs(start);
    var to: usize = @abs(endinc);

    if (start < 0) {
        if (@abs(start) > len) {
            from = 0;
        } else {
            from = len - @abs(start);
        }
    }
    if (endinc < 0) {
        if (@abs(endinc) > len) {
            to = 0;
        } else {
            to = len - @abs(endinc);
        }
    }

    var new_len = if (from > to) 0 else to - from + 1;
    if (new_len != 0) {
        if (from >= len) {
            new_len = 0;
        } else if (to >= len) {
            to = len - 1;
            new_len = if (from > to) 0 else to - from + 1;
        }
    } else {
        from = 0;
    }

    if (from != 0 and new_len != 0) {
        @memmove(s[0..new_len], s[from .. from + new_len]);
    }
    setLength(s, new_len);
}

pub fn toLower(s: Sds) void {
    const slice = bufSlice(s);
    for (slice, 0..) |c, i| {
        slice[i] = std.ascii.toLower(c);
    }
}

pub fn toUpper(s: Sds) void {
    const slice = bufSlice(s);
    for (slice, 0..) |c, i| {
        slice[i] = std.ascii.toUpper(c);
    }
}

pub fn cmp(s1: Sds, s2: Sds) std.math.Order {
    const lhs = bufSlice(s1);
    const rhs = bufSlice(s2);
    const n = @min(lhs.len, rhs.len);
    var i: usize = 0;
    while (i < n) : (i += 1) {
        switch (std.math.order(lhs[i], rhs[i])) {
            .eq => continue,
            .lt => return .lt,
            .gt => return .gt,
        }
    }
    return std.math.order(lhs.len, rhs.len);
}

pub fn getLen(s: Sds) usize {
    const flags = (s - 1)[0];
    return switch (flags & TYPE_MASK) {
        TYPE_5 => flags >> TYPE_BITS,
        TYPE_8 => @as(*Hdr8, @ptrCast(s - @sizeOf(Hdr8))).len,
        TYPE_16 => @as(*Hdr16, @ptrCast(s - @sizeOf(Hdr16))).len,
        TYPE_32 => @as(*Hdr32, @ptrCast(s - @sizeOf(Hdr32))).len,
        TYPE_64 => @as(*Hdr64, @ptrCast(s - @sizeOf(Hdr64))).len,
        else => 0,
    };
}

pub fn getAvail(s: Sds) usize {
    return getAlloc(s) - getLen(s);
}

pub fn getAlloc(s: Sds) usize {
    const flags = (s - 1)[0];
    return switch (flags & TYPE_MASK) {
        TYPE_5 => Hdr5.alloc(s).*,
        TYPE_8 => @as(*Hdr8, @ptrCast(s - @sizeOf(Hdr8))).alloc,
        TYPE_16 => @as(*Hdr16, @ptrCast(s - @sizeOf(Hdr16))).alloc,
        TYPE_32 => @as(*Hdr32, @ptrCast(s - @sizeOf(Hdr32))).alloc,
        TYPE_64 => @as(*Hdr64, @ptrCast(s - @sizeOf(Hdr64))).alloc,
        else => 0,
    };
}

/// Return the total size of the allocation of the specified sds string,
/// including:
/// 1) The sds header before the pointer.
/// 2) The string.
/// 3) The free buffer at the end if any.
pub fn getAllocMemSize(s: Sds) usize {
    return hdrSize(getType(s)) + getAlloc(s);
}

pub fn setLength(s: Sds, new_len: usize) void {
    switch (getType(s)) {
        TYPE_5 => {
            setType(s, TYPE_5 | (@as(u8, @intCast(new_len)) << TYPE_BITS));
        },
        TYPE_8 => {
            const hdr: *Hdr8 = @ptrCast(s - @sizeOf(Hdr8));
            hdr.len = @intCast(new_len);
        },
        TYPE_16 => {
            const hdr: *Hdr16 = @ptrCast(s - @sizeOf(Hdr16));
            hdr.len = @intCast(new_len);
        },
        TYPE_32 => {
            const hdr: *Hdr32 = @ptrCast(s - @sizeOf(Hdr32));
            hdr.len = @intCast(new_len);
        },
        TYPE_64 => {
            const hdr: *Hdr64 = @ptrCast(s - @sizeOf(Hdr64));
            hdr.len = @intCast(new_len);
        },
        else => {},
    }
}

pub fn free(allocator: Allocator, s: Sds) void {
    const hdr_len = hdrSize(getType(s));
    const mem: [*]u8 = s - hdr_len;
    const mem_size = getAllocMemSize(s);
    allocator.free(mem[0..mem_size]);
}

inline fn getType(s: Sds) u8 {
    return (s - 1)[0] & TYPE_MASK;
}

inline fn setType(s: Sds, typ: u8) void {
    (s - 1)[0] = typ;
}

inline fn setBuf(s: Sds, buf: []const u8) void {
    @memcpy(s[0..buf.len], buf);
}

inline fn bufSlice(s: Sds) []u8 {
    return s[0..getLen(s)];
}

inline fn memSlice(s: Sds) []u8 {
    const mem: [*]u8 = s - hdrSize(getType(s));
    return mem[0..getAllocMemSize(s)];
}

fn setAlloc(s: Sds, new_alloc: usize) void {
    switch (getType(s)) {
        TYPE_5 => {
            Hdr5.alloc(s).* = @intCast(new_alloc);
        },
        TYPE_8 => {
            const hdr: *Hdr8 = @ptrCast(s - @sizeOf(Hdr8));
            hdr.alloc = @intCast(new_alloc);
        },
        TYPE_16 => {
            const hdr: *Hdr16 = @ptrCast(s - @sizeOf(Hdr16));
            hdr.alloc = @intCast(new_alloc);
        },
        TYPE_32 => {
            const hdr: *Hdr32 = @ptrCast(s - @sizeOf(Hdr32));
            hdr.alloc = @intCast(new_alloc);
        },
        TYPE_64 => {
            const hdr: *Hdr64 = @ptrCast(s - @sizeOf(Hdr64));
            hdr.alloc = @intCast(new_alloc);
        },
        else => {},
    }
}

fn reqType(size: usize) u8 {
    if (size < 1 << 5) return TYPE_5;
    if (size < 1 << 8) return TYPE_8;
    if (size < 1 << 16) return TYPE_16;
    if (builtin.target.ptrBitWidth() == 64) {
        if (size < 1 << 32) {
            return TYPE_32;
        }
        return TYPE_64;
    }
    return TYPE_32;
}

fn hdrSize(typ: u8) usize {
    return switch (typ & TYPE_MASK) {
        TYPE_5 => @sizeOf(MemSizedHdr5),
        TYPE_8 => @sizeOf(Hdr8),
        TYPE_16 => @sizeOf(Hdr16),
        TYPE_32 => @sizeOf(Hdr32),
        TYPE_64 => @sizeOf(Hdr64),
        else => 0,
    };
}

test new {
    const allocator = testing.allocator;
    const init = "hello";
    const s = try new(allocator, init);
    defer free(allocator, s);

    try expectEqual(init.len, getLen(s));
    try expectEqualStrings(init, bufSlice(s));

    const long_str = "hello" ** 100;
    const long_s = try new(allocator, long_str);
    defer free(allocator, long_s);
    try expectEqual(long_str.len, getLen(long_s));
    try expectEqualStrings(long_str, bufSlice(long_s));
}

test empty {
    const allocator = testing.allocator;
    const s = try empty(allocator);
    defer free(allocator, s);

    try expectEqual(0, getLen(s));
    try expectStringEndsWith("", bufSlice(s));
}

test fromLonglong {
    const allocator = testing.allocator;

    const min = try fromLonglong(allocator, std.math.minInt(longlong));
    defer free(allocator, min);
    try expectEqualStrings("-9223372036854775808", bufSlice(min));

    const max = try fromLonglong(allocator, std.math.maxInt(longlong));
    defer free(allocator, max);
    try expectEqualStrings("9223372036854775807", bufSlice(max));
}

test dupe {
    const allocator = testing.allocator;

    const s = try new(allocator, "hello");
    defer free(allocator, s);

    const dup = try dupe(allocator, s);
    defer free(allocator, dup);

    try expectEqualStrings(bufSlice(s), bufSlice(dup));
}

test clear {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer free(allocator, s);

    clear(s);
    try expectEqual(0, getLen(s));
    try expectEqualStrings("", bufSlice(s));
}

test makeRoomFor {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const s1 = try makeRoomFor(allocator, s, 500);
    defer free(allocator, s1);

    try expectEqual(5, getLen(s1));
    try expectEqualStrings("hello", bufSlice(s1));
}

test removeAvailSpace {
    const allocator = testing.allocator;
    var s = try new(allocator, "hello");
    defer free(allocator, s);

    s = try makeRoomFor(allocator, s, 5);
    s = try removeAvailSpace(allocator, s);
    try expect(getLen(s) == 5);
    try expect(getAlloc(s) == 5);
    try expect(getAvail(s) == 0);
}

test cat {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const ns = try cat(allocator, s, "world");
    defer free(allocator, ns);

    try expectEqual(10, getLen(ns));
    try expectEqualStrings("helloworld", bufSlice(ns));
}

test catPrintf {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const ns = try catPrintf(allocator, s, " {s} {d}", .{ "world", 2025 });
    defer free(allocator, ns);
    try expectEqualStrings("hello world 2025", bufSlice(ns));
}

test catRepr {
    const allocator = testing.allocator;
    const s = try empty(allocator);
    const input = "\x07\n\x00foo\r\""; // in c: \a\n\0foo\r\"
    const ns = try catRepr(allocator, s, input);
    defer free(allocator, ns);

    try expectEqualStrings("\"\\a\\n\\x00foo\\r\\\"\"", bufSlice(ns));
}

test getAllocMemSize {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer free(allocator, s);
    try expectEqual(@sizeOf(MemSizedHdr5) + 5, getAllocMemSize(s));
}

test "incrLen-" {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer free(allocator, s);
    incrLen(s, -3);

    try expectEqual(2, getLen(s));
}

test "incrLen+" {
    const allocator = testing.allocator;
    var s = try new(allocator, "hello");
    defer free(allocator, s);
    s = try makeRoomFor(allocator, s, 3);
    incrLen(s, 3);
    try expectEqual(8, getLen(s));
}

test growZero {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");

    const ns = try growZero(allocator, s, 10);
    defer free(allocator, ns);

    try expectEqual(10, getLen(ns));
    try expectEqualSlices(
        u8,
        &.{ 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0 },
        bufSlice(ns),
    );
}

test copy {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");

    var cp = try copy(allocator, s, "world");
    try expectEqual(getLen(cp), 5);
    try expectEqualStrings("world", bufSlice(cp));
    try expectEqual(s, cp);

    cp = try copy(allocator, cp, "world!");
    defer free(allocator, cp);
    try expectEqual(getLen(cp), 6);
    try expectEqualStrings("world!", bufSlice(cp));
    try expect(s != cp);
}

test join {
    const allocator = testing.allocator;
    var tokens = std.ArrayList([]const u8).empty;
    defer tokens.deinit(allocator);

    try tokens.append(allocator, "hello");
    try tokens.append(allocator, "world");
    try tokens.append(allocator, "zig");

    const joined = try join(allocator, tokens.items, "|");
    defer free(allocator, joined);

    try expectEqualSlices(u8, "hello|world|zig", bufSlice(joined));
}

test split {
    const allocator = testing.allocator;
    const tokens = try split(allocator, "hello|world|zig", "|");
    defer freeSplitRes(allocator, tokens);

    try expect(tokens.len == 3);
    try expectEqualStrings("hello", bufSlice(tokens[0]));
    try expectEqualStrings("world", bufSlice(tokens[1]));
    try expectEqualStrings("zig", bufSlice(tokens[2]));
}

test trim {
    const allocator = testing.allocator;
    const s = try new(allocator, "AA...AA.a.aa.aHelloWorld     :::");
    defer free(allocator, s);

    trim(s, "Aa. :");
    try expectEqualStrings("HelloWorld", bufSlice(s));

    trim(s, "d");
    try expectEqualStrings("HelloWorl", bufSlice(s));
}

test "range(1, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, 1);

    try expectEqualStrings("h", bufSlice(s));
}

test "range(1, -1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, -1);

    try expectEqualStrings("hello!", bufSlice(s));
}

test "range(-2, -1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, -2, -1);

    try expectEqualStrings("o!", bufSlice(s));
}

test "range(2, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 2, 1);

    try expectEqualStrings("", bufSlice(s));
}

test "range(1, 100)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 1, 100);

    try expectEqualStrings("hello!", bufSlice(s));
}

test "range(100, 100)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 100, 100);

    try expectEqualStrings("", bufSlice(s));
}

test "range(0, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer free(allocator, s);
    range(s, 0, 1);

    try expectEqualStrings("!h", bufSlice(s));
}

test toLower {
    const allocator = testing.allocator;
    const s = try new(allocator, "Hello1");
    defer free(allocator, s);
    toLower(s);

    try expectEqualStrings("hello1", bufSlice(s));
}

test toUpper {
    const allocator = testing.allocator;
    const s = try new(allocator, "Hello1");
    defer free(allocator, s);
    toUpper(s);

    try expectEqualStrings("HELLO1", bufSlice(s));
}

test "cmp.gt" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "foo");
    defer free(allocator, s1);

    const s2 = try new(allocator, "foa");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.gt, cmp(s1, s2));
}

test "cmp.eq" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "bar");
    defer free(allocator, s1);

    const s2 = try new(allocator, "bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.eq, cmp(s1, s2));
}

test "cmp.lt" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "aar");
    defer free(allocator, s1);

    const s2 = try new(allocator, "bar");
    defer free(allocator, s2);

    try expectEqual(std.math.Order.lt, cmp(s1, s2));
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const debug = std.debug;
const ctypes = @import("ctypes.zig");
const int = ctypes.int;
const uint = ctypes.uint;
const longlong = ctypes.longlong;
const builtin = @import("builtin");
const assert = std.debug.assert;
const testing = std.testing;
const expect = testing.expect;
const expectEqualStrings = testing.expectEqualStrings;
const expectEqual = testing.expectEqual;
const expectEqualSlices = testing.expectEqualSlices;
const expectStringEndsWith = testing.expectStringEndsWith;
