type: u4,
encoding: u4,
lru: u24,
refcount: i32,
ptr: *anyopaque,

pub const Object = @This();

pub fn decrRefCount(_: *Object, _: Allocator) void {}

const std = @import("std");
const meta = std.meta;
const redis = @import("redis.zig");
const Allocator = std.mem.Allocator;
