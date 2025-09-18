// Should be enough for 2^32 elements.
pub const MAX_LEVEL = 32;

pub const Node = struct {
    level: []Level,
    obj: ?*Object,
    score: f64,
    backward: ?*Node,
    _: [0]Level,

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
        const memsize: usize = @sizeOf(Node) + level * @sizeOf(Level);
        const mem = try allocator.alignedAlloc(
            u8,
            .of(Node),
            memsize,
        );
        const node: *Node = @ptrCast(@alignCast(mem));
        // CPU cache friendly.
        node.level.ptr = @ptrCast(@alignCast(mem.ptr + @sizeOf(Node)));
        node.level.len = level;
        node.obj = obj;
        node.score = score;
        node.backward = null;

        return node;
    }

    pub fn free(self: *Node, allocator: Allocator) void {
        if (self.obj) |obj| {
            obj.decrRefCount(allocator);
        }
        const mem: [*]align(@alignOf(Node)) u8 = @ptrCast(@alignCast(self));
        const memsize = @sizeOf(Node) + self.level.len * @sizeOf(Level);
        allocator.free(mem[0..memsize]);
    }
};

header: *Node,
tail: ?*Node,
length: usize,
level: u32,

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

pub fn insert(
    self: *SkipList,
    allocator: Allocator,
    score: f64,
    obj: *Object,
) Allocator.Error!*Node {
    assert(!isNan(score));

    var rank: [MAX_LEVEL]u32 = undefined;
    var update: [MAX_LEVEL]*Node = undefined;

    const zsl = self;
    var x = zsl.header;
    var i = zsl.level - 1;
    while (i >= 0) {
        rank[i] = if (i == zsl.level - 1) 0 else rank[i + 1];
        while (x.level[i].forward != null and
            (x.level[i].forward.?.score < score or
                x.level[i].forward.?.score == score and
                    x.level[i].forward.?.obj.?.compareStrings(obj) == .lt))
        {
            rank[i] += x.level[i].span;
            x = x.level[i].forward.?;
        }
        update[i] = x;
        if (i == 0) break;
        i -= 1;
    }

    const level = randomLevel();
    if (level > zsl.level) {
        i = zsl.level;
        while (i < level) : (i += 1) {
            rank[i] = 0;
            update[i] = zsl.header;
            update[i].level[i].span = @intCast(zsl.length);
        }
        zsl.level = level;
    }

    x = try Node.create(allocator, level, score, obj);
    i = 0;
    while (i < level) : (i += 1) {
        x.level[i].forward = update[i].level[i].forward;
        update[i].level[i].forward = x;

        x.level[i].span = update[i].level[i].span - (rank[0] - rank[i]);
        update[i].level[i].span = (rank[0] - rank[i]) + 1;
    }

    i = level;
    while (i < zsl.level) : (i += 1) {
        update[i].level[i].span += 1;
    }

    x.backward = if (update[0] == zsl.header) null else update[0];
    if (x.level[0].forward) |forward| {
        forward.backward = x;
    } else {
        zsl.tail = x;
    }
    zsl.length += 1;
    return x;
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

fn randomLevel() u32 {
    const P = 0.25;
    var level: u32 = 1;
    while (random.int(u16) < @as(u16, @intFromFloat(P * 0xFFFF))) {
        level += 1;
    }
    return @min(level, MAX_LEVEL);
}

test randomLevel {
    const level = randomLevel();
    try expect(level >= 1);
}

test SkipList {
    const allocator = testing.allocator;
    var sl = try create(allocator);
    defer sl.free(allocator);

    {
        const score01 = try sl.insert(
            allocator,
            0.1,
            try Object.createRawString(allocator, "score 0.1"),
        );
        try expect(score01.backward == null);
        try expect(sl.length == 1);
        try expect(sl.level == score01.level.len);

        var score02 = try sl.insert(
            allocator,
            0.2,
            try Object.createRawString(allocator, "score 0.2"),
        );
        try expect(sl.header.level[0].forward.? == score01);
        try expect(sl.tail == score02);
        try expect(sl.length == 2);
        try expect(sl.level == @max(score01.level.len, score02.level.len));

        score02 = try sl.insert(
            allocator,
            0.2,
            try Object.createRawString(allocator, "score 0.20"),
        );
        try expect(sl.tail == score02);
    }
}

const std = @import("std");
const Object = @import("Object.zig");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const assert = std.debug.assert;
const random = @import("random.zig");
const isNan = std.math.isNan;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
