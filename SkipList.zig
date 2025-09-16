header: *Node,
tail: ?*Node,
length: usize,
level: i32,

pub const SkipList = @This();

pub fn create(allocator: Allocator) Allocator.Error!*SkipList {
    const sl = try allocator.create(SkipList);
    const header = try Node.create(allocator, MAX_LEVEL, 0, null);
    sl.* = .{
        .header = header,
        .tail = null,
        .length = 0,
        .level = 1,
    };
    for (0..MAX_LEVEL) |i| {
        header.level[i] = .{
            .forward = null,
            .span = 0,
        };
    }
    return sl;
}

pub fn free(self: *SkipList, allocator: Allocator) void {
    var node = self.header.level[0].forward;
    while (node) |n| {
        node = n.level[0].forward;
        n.free(allocator);
    }
    self.header.free(allocator);
    allocator.destroy(self);
}

const MAX_LEVEL = 32;

pub const Node = struct {
    obj: ?*Object,
    score: f64,
    backward: ?*Node,
    level: []Level,

    const Level = struct {
        forward: ?*Node,
        span: u32,
    };

    pub fn create(
        allocator: Allocator,
        level: u32,
        score: f64,
        obj: ?*Object,
    ) Allocator.Error!*Node {
        const node = try allocator.create(Node);
        const levels = try allocator.alloc(Level, level);
        node.* = .{
            .obj = obj,
            .score = score,
            .backward = null,
            .level = levels,
        };
        return node;
    }

    pub fn free(self: *Node, allocator: Allocator) void {
        if (self.obj) |obj| {
            obj.decrRefCount(allocator);
        }
        allocator.free(self.level);
        allocator.destroy(self);
    }
};

test SkipList {
    const allocator = testing.allocator;
    var sl = try create(allocator);
    defer sl.free(allocator);
}

const std = @import("std");
const Object = @import("Object.zig");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const assert = std.debug.assert;
const random = @import("random.zig");
