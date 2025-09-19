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
    const zsl = self;

    var rank: [MAX_LEVEL]u32 = undefined;
    var update: [MAX_LEVEL]*Node = undefined;
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

pub fn delete(
    self: *SkipList,
    allocator: Allocator,
    score: f64,
    obj: *Object,
) bool {
    const zsl = self;

    var update: [MAX_LEVEL]*Node = undefined;
    var x = zsl.header;
    var i = zsl.level - 1;
    while (i >= 0) {
        while (x.level[i].forward != null and
            (x.level[i].forward.?.score < score or
                x.level[i].forward.?.score == score and
                    x.level[i].forward.?.obj.?.compareStrings(obj) == .lt))
        {
            x = x.level[i].forward.?;
        }
        update[i] = x;
        if (i == 0) break;
        i -= 1;
    }

    // We may have multiple elements with the same score, what we need
    // is to find the element with both the right score and object.
    if (x.level[0].forward) |f| {
        if (score == f.score and f.obj.?.equalStrings(obj)) {
            self.deleteNode(f, &update);
            f.free(allocator);
            return true;
        }
    }
    return false;
}

fn deleteNode(self: *SkipList, x: *Node, update: []*Node) void {
    const zsl = self;
    var i: u32 = 0;
    while (i < zsl.level) : (i += 1) {
        if (update[i].level[i].forward == x) {
            // Don't use update[i].level[i].span += x.level[i].span - 1.
            // This is for avoiding integer overflow because `x` maybe is the
            // last node with 0 span.
            update[i].level[i].span += x.level[i].span;
            update[i].level[i].span -= 1;
            update[i].level[i].forward = x.level[i].forward;
        } else {
            update[i].level[i].span -= 1;
        }
    }
    if (x.level[0].forward) |forward| {
        forward.backward = x.backward;
    } else {
        zsl.tail = x.backward;
    }

    while (zsl.level > 1 and zsl.header.level[zsl.level - 1].forward == null) {
        zsl.level -= 1;
    }
    zsl.length -= 1;
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
        const score1 = try sl.insert(
            allocator,
            1,
            try Object.createRawString(allocator, "score 1"),
        );
        try expect(score1.backward == null);
        try expect(sl.length == 1);
        try expect(sl.level == score1.level.len);

        var score2 = try sl.insert(
            allocator,
            2,
            try Object.createRawString(allocator, "score 2"),
        );
        try expect(sl.header.level[0].forward.? == score1);
        try expect(sl.tail == score2);
        try expect(sl.length == 2);
        try expect(sl.level == @max(score1.level.len, score2.level.len));

        score2 = try sl.insert(
            allocator,
            2,
            try Object.createRawString(allocator, "score 2.0"),
        );
        try expect(sl.tail == score2);
        try expect(sl.length == 3);
    }

    {
        const obj = try Object.createRawString(allocator, "deleted");
        _ = try sl.insert(allocator, 3, obj);
        try expect(sl.length == 4);
        const deleted = sl.delete(allocator, 3, obj);
        try expect(deleted);
        try expect(sl.length == 3);
        try expect(sl.tail.?.score == 2);
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
