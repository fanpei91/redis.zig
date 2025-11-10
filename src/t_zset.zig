// Should be enough for 2^64 elements.
pub const ZSKIPLIST_MAXLEVEL = 64;

/// Hold a inclusive/exclusive range by score comparison.
pub const Range = struct {
    min: f64,
    max: f64,

    // Are min or max exclusive?
    minex: bool,
    maxex: bool,

    pub fn minLte(
        range: *const Range,
        value: f64,
    ) bool {
        return if (range.minex)
            range.min < value
        else
            range.min <= value;
    }

    pub fn maxGte(
        range: *const Range,
        value: f64,
    ) bool {
        return if (range.maxex)
            range.max > value
        else
            range.max >= value;
    }
};

pub const Zset = struct {
    zsl: *SkipList,
    dict: *Dict,

    pub fn create() *Zset {
        const z = allocator.create(Zset);
        z.dict = Dict.create(
            Server.zsetDictVTable,
            null,
        );
        z.zsl = SkipList.create();
        return z;
    }

    pub fn destroy(self: *Zset) void {
        self.dict.destroy();
        self.zsl.free();
        allocator.destroy(self);
    }
};

pub const SkipList = struct {
    header: *Node,
    tail: ?*Node,
    length: u64,
    level: u32,

    const SizedNode = struct {
        size: usize,
        node: Node,
    };

    pub const Node = struct {
        ele: ?sds.String,
        score: f64,
        backward: ?*Node,

        const Level = struct {
            forward: ?*Node,
            span: u64,
        };

        pub fn create(num_level: u32, score: f64, ele: ?sds.String) *Node {
            const mem_size: usize = @sizeOf(SizedNode) + num_level * @sizeOf(Level);
            const mem = allocator.alignedAlloc(
                u8,
                .of(SizedNode),
                mem_size,
            );
            const sno: *SizedNode = @ptrCast(@alignCast(mem));
            sno.size = mem_size;
            sno.node = .{
                .ele = ele,
                .score = score,
                .backward = null,
            };
            return &sno.node;
        }

        pub inline fn level(self: *const Node, i: usize) *Level {
            const offset = @sizeOf(Node) + @sizeOf(Level) * i;
            return @ptrFromInt(@intFromPtr(self) + offset);
        }

        pub fn free(self: *Node) void {
            if (self.ele) |ele| {
                sds.free(ele);
            }
            const parent: *SizedNode = @fieldParentPtr("node", self);
            const mem: [*]align(@alignOf(SizedNode)) u8 = @ptrCast(@alignCast(parent));
            allocator.free(mem[0..parent.size]);
        }
    };

    pub fn create() *SkipList {
        const sl = allocator.create(SkipList);
        const header = Node.create(ZSKIPLIST_MAXLEVEL, 0, null);
        for (0..ZSKIPLIST_MAXLEVEL) |i| {
            const lvl = header.level(i);
            lvl.forward = null;
            lvl.span = 0;
        }
        sl.* = .{
            .header = header,
            .tail = null,
            .length = 0,
            .level = 1,
        };
        return sl;
    }

    pub fn insert(
        self: *SkipList,
        score: f64,
        ele: sds.String,
    ) *Node {
        assert(!isNan(score));

        var rank: [ZSKIPLIST_MAXLEVEL]u64 = undefined;
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) {
            rank[i] = if (i == self.level - 1) 0 else rank[i + 1];
            while (x.level(i).forward != null and
                (x.level(i).forward.?.score < score or
                    x.level(i).forward.?.score == score and
                        sds.cmp(x.level(i).forward.?.ele.?, ele) == .lt))
            {
                rank[i] += x.level(i).span;
                x = x.level(i).forward.?;
            }
            update[i] = x;
            if (i == 0) break;
            i -= 1;
        }

        const level = zslRandomLevel();
        if (level > self.level) {
            i = self.level;
            while (i < level) : (i += 1) {
                rank[i] = 0;
                update[i] = self.header;
                update[i].level(i).span = @intCast(self.length);
            }
            self.level = level;
        }

        x = Node.create(level, score, ele);
        i = 0;
        while (i < level) : (i += 1) {
            x.level(i).forward = update[i].level(i).forward;
            update[i].level(i).forward = x;

            x.level(i).span = update[i].level(i).span - (rank[0] - rank[i]);
            update[i].level(i).span = (rank[0] - rank[i]) + 1;
        }

        i = level;
        while (i < self.level) : (i += 1) {
            update[i].level(i).span += 1;
        }

        x.backward = if (update[0] == self.header) null else update[0];
        if (x.level(0).forward) |forward| {
            forward.backward = x;
        } else {
            self.tail = x;
        }
        self.length += 1;
        return x;
    }

    pub fn delete(
        self: *SkipList,
        score: f64,
        ele: sds.String,
    ) bool {
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) {
            while (x.level(i).forward != null and
                (x.level(i).forward.?.score < score or
                    x.level(i).forward.?.score == score and
                        sds.cmp(x.level(i).forward.?.ele.?, ele) == .lt))
            {
                x = x.level(i).forward.?;
            }
            update[i] = x;
            if (i == 0) break;
            i -= 1;
        }

        // We may have multiple elements with the same score, what we need
        // is to find the element with both the right score and sds.
        if (x.level(0).forward) |f| {
            if (score == f.score and sds.cmp(f.ele.?, ele) == .eq) {
                self.deleteNode(f, &update);
                f.free();
                return true;
            }
        }
        return false;
    }

    pub fn getRank(self: *const SkipList, score: f64, ele: sds.String) usize {
        var rank: u64 = 0;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) {
            while (x.level(i).forward != null and
                (x.level(i).forward.?.score < score or
                    x.level(i).forward.?.score == score and
                        sds.cmp(x.level(i).forward.?.ele.?, ele).compare(.lte)))
            {
                rank += x.level(i).span;
                x = x.level(i).forward.?;
            }

            // x might be equal to self.header, so test if ele is non-NULL
            if (x.ele != null and sds.cmp(x.ele.?, ele) == .eq) {
                return rank;
            }

            if (i == 0) break;
            i -= 1;
        }
        return rank;
    }

    pub fn firstInRange(self: *const SkipList, range: *const Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) {
            while (x.level(i).forward != null and
                !range.minLte(x.level(i).forward.?.score))
            {
                x = x.level(i).forward.?;
            }
            if (i == 0) break;
            i -= 1;
        }

        // This is an inner range, so the next node cannot be NULL.
        x = x.level(0).forward.?;
        if (!range.maxGte(x.score)) return null;
        return x;
    }

    pub fn lastInRange(self: *const SkipList, range: *const Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) {
            while (x.level(i).forward != null and
                range.maxGte(x.level(i).forward.?.score))
            {
                x = x.level(i).forward.?;
            }
            if (i == 0) break;
            i -= 1;
        }

        if (!range.minLte(x.score)) return null;
        return x;
    }

    pub fn free(self: *SkipList) void {
        var node = self.header.level(0).forward;
        self.header.free();
        while (node) |n| {
            node = n.level(0).forward;
            n.free();
        }
        allocator.destroy(self);
    }

    /// Returns if there is a part of the skiplist is in range.
    fn isInRange(self: *const SkipList, range: *const Range) bool {
        if (range.min > range.max) return false;
        if (range.min == range.max and (range.minex or range.maxex)) return false;

        const last = self.tail;
        if (last == null or !range.minLte(last.?.score)) return false;

        const first = self.header.level(0).forward;
        if (first == null or !range.maxGte(first.?.score)) return false;

        return true;
    }

    fn deleteNode(self: *SkipList, x: *Node, update: []*Node) void {
        var i: u32 = 0;
        while (i < self.level) : (i += 1) {
            if (update[i].level(i).forward == x) {
                // update[i].level(i).span += x.level(i).span - 1;
                // This is for avoiding integer overflow because `x` maybe is the
                // last node with 0 span.
                update[i].level(i).span += x.level(i).span;
                update[i].level(i).span -= 1;
                update[i].level(i).forward = x.level(i).forward;
            } else {
                update[i].level(i).span -= 1;
            }
        }
        if (x.level(0).forward) |forward| {
            forward.backward = x.backward;
        } else {
            self.tail = x.backward;
        }

        while (self.level > 1 and self.header.level(self.level - 1).forward == null) {
            self.level -= 1;
        }
        self.length -= 1;
    }
};

fn zslRandomLevel() u32 {
    const P = 0.25;
    var level: u32 = 1;
    while (random.int(u16) < @as(u16, @intFromFloat(P * 0xFFFF))) {
        level += 1;
    }
    return @min(level, ZSKIPLIST_MAXLEVEL);
}

test zslRandomLevel {
    const level = zslRandomLevel();
    try expect(level >= 1);
}

test SkipList {
    var sl = SkipList.create();
    defer sl.free();

    var ele = sds.new("score 1");
    try expect(sl.getRank(1, ele) == 0);
    const score1 = sl.insert(
        1,
        ele,
    );
    try expect(score1.backward == null);
    try expect(sl.length == 1);
    try expect(sl.getRank(1, ele) == 1);

    ele = sds.new("score 2");
    var score2 = sl.insert(
        2,
        ele,
    );
    try expect(sl.header.level(0).forward.? == score1);
    try expect(sl.tail == score2);
    try expect(sl.length == 2);

    var range = Range{
        .min = 1,
        .max = 2,
        .minex = false,
        .maxex = false,
    };
    const first = sl.firstInRange(&range);
    try expect(first != null);
    try expect(first.? == score1);
    var last = sl.lastInRange(&range);
    try expect(last != null);
    try expect(last.? == score2);
    range.maxex = true;
    last = sl.lastInRange(&range);
    try expect(last != null);
    try expect(last.? == score1);

    score2 = sl.insert(
        2,
        sds.new("score 2.0"),
    );
    try expect(sl.tail == score2);
    try expect(sl.length == 3);

    ele = sds.new("deleted");
    _ = sl.insert(3, ele);
    try expect(sl.length == 4);
    const deleted = sl.delete(3, ele);
    try expect(deleted);
    try expect(sl.length == 3);
    try expect(sl.tail.?.score == 2);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const assert = std.debug.assert;
const random = @import("random.zig");
const isNan = std.math.isNan;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const sds = @import("sds.zig");
const Server = @import("Server.zig");
const Dict = @import("Dict.zig");
