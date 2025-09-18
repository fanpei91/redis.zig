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
// lru: u24,
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

pub fn decrRefCount(self: *Object, allocator: Allocator) void {
    if (self.refcount <= 0) @panic("Object.decrRefCount against refcount <= 0");
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(allocator),
            else => unreachable, // TODO: complete all branch
        }
        allocator.destroy(self);
        return;
    }
    self.refcount -= 1;
}

const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;
const Sds = @import("Sds.zig");
const assert = std.debug.assert;
