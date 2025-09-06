len: u32,
avail: u32,
buf: [0]u8,

const MAX_PREALLOC = 1024 * 1024;

const Sds = @This();

pub fn new(allocator: Allocator, init: []const u8) Allocator.Error!*Sds {
    const mem = try allocator.alignedAlloc(
        u8,
        .of(Sds),
        @sizeOf(Sds) + init.len,
    );
    const s: *Sds = @ptrCast(mem.ptr);
    s.len = @intCast(init.len);
    s.avail = 0;

    if (init.len > 0) {
        const buf_ptr = @as([*]u8, mem.ptr + @sizeOf(Sds));
        @memcpy(buf_ptr[0..init.len], init);
    }
    return s;
}

test new {
    const allocator = testing.allocator;
    const init = "hello";
    const s = try new(allocator, init);
    defer s.free(allocator);

    try testing.expectEqual(init.len, s.len);
    try testing.expectEqual(0, s.avail);
    try testing.expectEqualStrings(init, s.toSlice());
}

pub fn empty(allocator: Allocator) Allocator.Error!*Sds {
    return new(allocator, "");
}

test empty {
    const allocator = testing.allocator;
    const s = try empty(allocator);
    defer s.free(allocator);

    try testing.expectEqual(0, s.len);
    try testing.expectEqual(0, s.avail);
    try testing.expectStringEndsWith("", s.toSlice());
}

pub fn fromI64(
    allocator: Allocator,
    value: i64,
) Allocator.Error!*Sds {
    var buf: [20]u8 = undefined;
    const digits = std.fmt.bufPrint(&buf, "{d}", .{value}) catch unreachable;
    return new(allocator, digits);
}

test fromI64 {
    const allocator = testing.allocator;

    const min = try fromI64(allocator, std.math.minInt(i64));
    defer min.free(allocator);
    try testing.expectEqualStrings("-9223372036854775808", min.toSlice());

    const max = try fromI64(allocator, std.math.maxInt(i64));
    defer max.free(allocator);
    try testing.expectEqualStrings("9223372036854775807", max.toSlice());
}

pub fn dupe(s: *Sds, allocator: Allocator) Allocator.Error!*Sds {
    return new(allocator, s.toSlice());
}

test dupe {
    const allocator = testing.allocator;

    const s = try new(allocator, "hello");
    defer s.free(allocator);

    const dup = try s.dupe(allocator);
    defer dup.free(allocator);

    try testing.expectEqualStrings(s.toSlice(), dup.toSlice());
}

pub fn clear(s: *Sds) void {
    s.avail += s.len;
    s.len = 0;
}

test clear {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer s.free(allocator);

    s.clear();
    try testing.expectEqual(0, s.len);
    try testing.expectEqual(5, s.avail);
    try testing.expectEqualStrings("", s.toSlice());
}

pub fn makeRoomFor(
    s: *Sds,
    allocator: Allocator,
    add_len: usize,
) Allocator.Error!*Sds {
    if (s.avail >= add_len) return s;

    const len = s.len;
    var new_len = len + add_len;
    if (new_len < MAX_PREALLOC) {
        new_len *= 2;
    } else {
        new_len += MAX_PREALLOC;
    }

    const old_mem_size = @sizeOf(Sds) + s.len + s.avail;
    const new_mem_size = @sizeOf(Sds) + new_len;

    const old_mem: [*]align(@alignOf(Sds)) u8 = @ptrCast(s);
    const new_mem = try allocator.realloc(
        old_mem[0..old_mem_size],
        new_mem_size,
    );

    const ns: *Sds = @ptrCast(new_mem.ptr);
    ns.avail = @intCast(new_len - len);
    return ns;
}

test makeRoomFor {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const s1 = try s.makeRoomFor(allocator, 5);
    defer s1.free(allocator);

    try testing.expectEqual(5, s1.len);
    try testing.expectEqual(15, s1.avail);
    try testing.expectEqualStrings("hello", s1.toSlice());
}

pub fn cat(s: *Sds, allocator: Allocator, t: []const u8) Allocator.Error!*Sds {
    const ns = try s.makeRoomFor(allocator, t.len);
    const buf_ptr: [*]u8 = ns.bufPtr();
    @memcpy(buf_ptr[ns.len .. ns.len + t.len], t);

    const add_len: u32 = @intCast(t.len);
    ns.len += add_len;
    ns.avail -= add_len;
    return ns;
}

test cat {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const ns = try s.cat(allocator, "world");
    defer ns.free(allocator);

    try testing.expectEqual(10, ns.len);
    try testing.expectEqualStrings("helloworld", ns.toSlice());
}

pub fn catSds(s: *Sds, allocator: Allocator, t: *Sds) Allocator.Error!*Sds {
    return s.cat(allocator, t.toSlice());
}

test catSds {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const t = try new(allocator, "world");
    defer t.free(allocator);

    const ns = try s.catSds(allocator, t);
    defer ns.free(allocator);

    try testing.expectEqual(10, ns.len);
    try testing.expectEqualStrings("helloworld", ns.toSlice());
}

pub fn catPrintf(
    s: *Sds,
    allocator: Allocator,
    comptime fmt: []const u8,
    args: anytype,
) (Allocator.Error)!*Sds {
    var staic_buf: [1024]u8 = undefined;
    const buf = std.fmt.bufPrint(&staic_buf, fmt, args) catch {
        @branchHint(.unlikely);
        const alloc_buf = try std.fmt.allocPrint(allocator, fmt, args);
        defer allocator.free(alloc_buf);
        return s.cat(allocator, alloc_buf);
    };
    return s.cat(allocator, buf);
}

test catPrintf {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    const ns = try s.catPrintf(allocator, " {s} {d}", .{ "world", 2025 });
    defer ns.free(allocator);
    try testing.expectEqualStrings("hello world 2025", ns.toSlice());
}

pub fn catRepr(
    s: *Sds,
    allocator: Allocator,
    p: []const u8,
) Allocator.Error!*Sds {
    var ns = try cat(s, allocator, "\"");
    for (p) |b| {
        switch (b) {
            '\\', '"' => ns = try ns.catPrintf(allocator, "\\{d}", .{b}),
            '\n' => ns = try ns.cat(allocator, "\\n"),
            '\r' => ns = try ns.cat(allocator, "\\r"),
            '\t' => ns = try ns.cat(allocator, "\\t"),
            // in c: '\a'
            0x07 => ns = try ns.cat(allocator, "\\a"),
            // in c: '\b'
            0x08 => ns = try ns.cat(allocator, "\\b"),
            else => {
                if (std.ascii.isPrint(b)) {
                    ns = try ns.catPrintf(allocator, "{c}", .{b});
                } else {
                    ns = try ns.catPrintf(allocator, "\\x{x:0>2}", .{b});
                }
            },
        }
    }
    ns = try ns.cat(allocator, "\"");
    return ns;
}

test catRepr {
    const allocator = testing.allocator;
    const s = try empty(allocator);
    const input = "\x07\n\x00foo\r"; // in c: "\a\n\0foo\r"
    const ns = try s.catRepr(allocator, input);
    defer ns.free(allocator);

    try testing.expectEqualStrings("\"\\a\\n\\x00foo\\r\"", ns.toSlice());
}

pub fn removeAvailSpace(s: *Sds, allocator: Allocator) Allocator.Error!*Sds {
    const old_mem_size = @sizeOf(Sds) + s.len + s.avail;
    const new_mem_size = @sizeOf(Sds) + s.len;

    const old_mem: [*]align(@alignOf(Sds)) u8 = @ptrCast(s);
    const new_mem = try allocator.realloc(
        old_mem[0..old_mem_size],
        new_mem_size,
    );

    const ns: *Sds = @ptrCast(new_mem.ptr);
    ns.avail = 0;
    return ns;
}

pub fn allocSize(s: *Sds) usize {
    return @sizeOf(Sds) + s.len + s.avail;
}

test allocSize {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer s.free(allocator);
    try testing.expectEqual(@sizeOf(Sds) + 5, s.allocSize());
}

pub fn incrLen(s: *Sds, incr: i32) void {
    if (incr >= 0) {
        debug.assert(s.avail >= incr);
    } else {
        debug.assert(s.len >= -incr);
    }
    s.avail = @intCast(@as(i64, s.avail) - @as(i64, incr));
    s.len = @intCast(@as(i64, s.len) + @as(i64, incr));
}

test "incrLen-" {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer s.free(allocator);
    s.incrLen(-3);

    try testing.expectEqual(2, s.len);
    try testing.expectEqual(3, s.avail);
}

test "incrLen+" {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");
    defer s.free(allocator);
    s.avail = 5;
    s.incrLen(3);
    try testing.expectEqual(8, s.len);
    try testing.expectEqual(2, s.avail);
    s.len = 5;
    s.avail = 0;
}

pub fn growZero(
    s: *Sds,
    allocator: Allocator,
    len: usize,
) Allocator.Error!*Sds {
    const curr_len = @as(usize, s.len);
    if (len <= curr_len) return s;

    const add_len = len - s.len;
    const ns = try s.makeRoomFor(allocator, add_len);

    const buf_ptr: [*]u8 = ns.bufPtr();
    @memset(buf_ptr[curr_len..len], 0);

    const total_len: usize = ns.len + ns.avail;
    ns.len = @intCast(len);
    ns.avail = @intCast(total_len - ns.len);
    return ns;
}

test growZero {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");

    const ns = try s.growZero(allocator, 10);
    defer ns.free(allocator);

    try testing.expectEqual(10, ns.len);
    try testing.expectEqualSlices(
        u8,
        &.{ 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0 },
        ns.toSlice(),
    );
}

pub fn copy(s: *Sds, allocator: Allocator, t: []const u8) Allocator.Error!*Sds {
    var ns = s;
    var total_len: usize = ns.len + ns.avail;
    if (t.len > total_len) {
        ns = try ns.makeRoomFor(allocator, t.len - total_len);
        total_len = ns.len + ns.avail;
    }
    const buf_ptr: [*]u8 = ns.bufPtr();
    @memcpy(buf_ptr[0..t.len], t);

    ns.len = @intCast(t.len);
    ns.avail = @intCast(total_len - ns.len);
    return ns;
}

test copy {
    const allocator = testing.allocator;
    const s = try new(allocator, "hello");

    var cp = try s.copy(allocator, "world");
    try testing.expectEqual(cp.len, 5);
    try testing.expectEqualStrings("world", cp.toSlice());
    try testing.expectEqual(s, cp);

    cp = try cp.copy(allocator, "world!");
    defer cp.free(allocator);
    try testing.expectEqual(cp.len, 6);
    try testing.expectEqualStrings("world!", cp.toSlice());
    try testing.expect(s != cp);
}

pub fn trim(s: *Sds, t: []const u8) void {
    const trimed = std.mem.trim(u8, s.toSlice(), t);
    const new_len: u32 = @intCast(trimed.len);
    const buf_ptr: [*]u8 = s.bufPtr();
    @memmove(buf_ptr[0..new_len], trimed);
    s.avail = s.avail + (s.len - new_len);
    s.len = new_len;
}

test trim {
    const allocator = testing.allocator;
    const s = try new(allocator, "AA...AA.a.aa.aHelloWorld     :::");
    defer s.free(allocator);

    s.trim("Aa. :");
    try testing.expectEqualStrings("HelloWorld", s.toSlice());

    s.trim("d");
    try testing.expectEqualStrings("HelloWorl", s.toSlice());
}

pub fn range(s: *Sds, start: i32, incl_end: i32) void {
    var from: i64 = start;
    var to: i64 = incl_end;
    const len: i64 = s.len;

    if (len == 0) return;

    if (from < 0) {
        from = len + from;
        if (from < 0) from = 0;
    }
    if (to < 0) {
        to = len + to;
        if (to < 0) to = 0;
    }

    var new_len: usize = @intCast(if (from > to) 0 else to - from + 1);
    if (new_len > 0) {
        if (from >= len) {
            new_len = 0;
        } else if (to >= len) {
            to = len - 1;
            new_len = @intCast(if (from > to) 0 else to - from + 1);
        }
    } else {
        from = 0;
    }

    if (from > 0 and new_len > 0) {
        const buf_ptr = s.bufPtr();
        const i: usize = @intCast(from);
        const dest = buf_ptr[0..new_len];
        const src = buf_ptr[i .. i + new_len];
        @memmove(dest, src);
    }

    s.avail = s.avail + (s.len - @as(u32, @intCast(new_len)));
    s.len = @intCast(new_len);
}

test "range(1, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(1, 1);

    try testing.expectEqualStrings("h", s.toSlice());
}

test "range(1, -1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(1, -1);

    try testing.expectEqualStrings("hello!", s.toSlice());
}

test "range(-2, -1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(-2, -1);

    try testing.expectEqualStrings("o!", s.toSlice());
}

test "range(2, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(2, 1);

    try testing.expectEqualStrings("", s.toSlice());
}

test "range(1, 100)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(1, 100);

    try testing.expectEqualStrings("hello!", s.toSlice());
}

test "range(100, 100)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(100, 100);

    try testing.expectEqualStrings("", s.toSlice());
}

test "range(0, 1)" {
    const allocator = testing.allocator;
    const s = try new(allocator, "!hello!");
    defer s.free(allocator);
    s.range(0, 1);

    try testing.expectEqualStrings("!h", s.toSlice());
}

pub fn toLower(s: *Sds) void {
    const slice = s.toSlice();
    for (slice, 0..) |c, i| {
        slice[i] = std.ascii.toLower(c);
    }
}

test toLower {
    const allocator = testing.allocator;
    const s = try new(allocator, "Hello1");
    defer s.free(allocator);
    s.toLower();

    try testing.expectEqualStrings("hello1", s.toSlice());
}

pub fn cmp(s1: *Sds, s2: *Sds) std.math.Order {
    const lhs = s1.toSlice();
    const rhs = s2.toSlice();
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

test "cmp.gt" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "foo");
    defer s1.free(allocator);

    const s2 = try new(allocator, "foa");
    defer s2.free(allocator);

    try testing.expectEqual(std.math.Order.gt, s1.cmp(s2));
}

test "cmp.eq" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "bar");
    defer s1.free(allocator);

    const s2 = try new(allocator, "bar");
    defer s2.free(allocator);

    try testing.expectEqual(std.math.Order.eq, s1.cmp(s2));
}

test "cmp.lt" {
    const allocator = testing.allocator;
    const s1 = try new(allocator, "aar");
    defer s1.free(allocator);

    const s2 = try new(allocator, "bar");
    defer s2.free(allocator);

    try testing.expectEqual(std.math.Order.lt, s1.cmp(s2));
}

pub fn free(s: *Sds, allocator: Allocator) void {
    const mem_size = @sizeOf(Sds) + s.len + s.avail;
    const mem: [*]align(@alignOf(Sds)) u8 = @ptrCast(s);
    allocator.free(mem[0..mem_size]);
}

fn bufPtr(s: *Sds) [*]u8 {
    return @ptrFromInt(@intFromPtr(s) + @sizeOf(Sds));
}

pub fn toSlice(s: *Sds) []u8 {
    return s.bufPtr()[0..s.len];
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const debug = std.debug;
