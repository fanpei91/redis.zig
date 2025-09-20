pub const Type = enum(u4) {
    string = 0,
    list = 1,
    set = 2,
    zset = 3,
    hash = 4,
};

pub const Encoding = enum(u4) {
    raw = 0,
    int = 1,
    ht = 2,
    zipmap = 3,
    linkedlist = 4,
    ziplist = 5,
    intset = 6,
    skiplist = 7,
    embstr = 8,
};

type: Type,
encoding: Encoding,
// lru: redis.LRU_BITS,
refcount: i32,
ptr: *anyopaque,

pub const Object = @This();

pub fn create(
    allocator: Allocator,
    typ: Type,
    ptr: *anyopaque,
) Allocator.Error!*Object {
    const obj = try allocator.create(Object);
    obj.* = .{
        .type = typ,
        .encoding = .raw,
        .ptr = ptr,
        //.lru =
        .refcount = 1,
    };
    return obj;
}

pub fn createRawString(
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*Object {
    return create(allocator, .string, try Sds.new(allocator, str));
}

pub fn createEmbeddedString(
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*Object {
    const memsize = @sizeOf(Object) + @sizeOf(Sds) + str.len;
    const mem = try allocator.alignedAlloc(u8, .of(Object), memsize);

    const obj: *Object = @ptrCast(@alignCast(mem));
    const sh: *Sds = @ptrFromInt(@intFromPtr(obj) + @sizeOf(Object));

    sh.len = @intCast(str.len);
    sh.avail = 0;
    _ = sh.copy(allocator, str) catch unreachable;

    obj.type = .string;
    obj.encoding = .embstr;
    obj.refcount = 1;
    obj.ptr = sh;

    return obj;
}

pub fn freeString(self: *Object, allocator: Allocator) void {
    if (self.encoding == .raw) {
        const sds: *Sds = @ptrCast(@alignCast(self.ptr));
        sds.free(allocator);
    }
}

pub fn compareStrings(self: *Object, other: *Object) std.math.Order {
    assert(self.type == .string);
    assert(other.type == .string);

    const a: *Sds = @ptrCast(@alignCast(self.ptr));
    const b: *Sds = @ptrCast(@alignCast(other.ptr));
    return a.cmp(b);
}

pub fn equalStrings(self: *Object, other: *Object) bool {
    if (self.encoding == .int and other.encoding == .int) {
        // How does it store int in pointer address? Example:
        // self.ptr  = @ptrFrontInt(100);
        // other.ptr = @ptrFrontInt(100);
        return self.ptr == other.ptr;
    }
    return self.compareStrings(other) == .eq;
}

pub fn decrRefCount(self: *Object, allocator: Allocator) void {
    if (self.refcount <= 0) @panic("Object.decrRefCount against refcount <= 0");
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(allocator),
            else => unreachable, // TODO: complete all branch
        }
        self.free(allocator);
        return;
    }
    self.refcount -= 1;
}

fn free(self: *Object, allocator: Allocator) void {
    if (self.type == .string and self.encoding == .embstr) {
        const sh: *Sds = @ptrCast(@alignCast(self.ptr));
        const memsize = @sizeOf(Object) + @sizeOf(Sds) + sh.len + sh.avail;
        const mem: [*]align(@alignOf(Object)) u8 = @ptrCast(@alignCast(self));
        allocator.free(mem[0..memsize]);
        return;
    }
    allocator.destroy(self);
}

test createEmbeddedString {
    const allocator = std.testing.allocator;
    var o = try createEmbeddedString(allocator, "hello");
    defer o.decrRefCount(allocator);
    var sh: *Sds = @ptrCast(@alignCast(o.ptr));
    try expectEqualStrings("hello", sh.toSlice());
}

const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;
const Sds = @import("Sds.zig");
const assert = std.debug.assert;
const redis = @import("redis.zig");
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
