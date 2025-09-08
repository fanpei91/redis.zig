pub const Hash = u64;

pub const HT_INITIAL_SIZE = 4;

const force_resize_ratio = 5;

var dict_can_resize = true;

pub fn enableResize() void {
    dict_can_resize = true;
}

pub fn disableResize() void {
    dict_can_resize = false;
}

pub const ReplaceResult = enum {
    replace,
    add,
};

/// Context must be a struct type with six member functions:
///
///     fn eql(self, Key, Key) bool
///
///     fn hash(self, Key) Hash
///
///     fn dupeKey(self, Key) Allocation.Error!Key
///
///     fn dupeVal(self, Value) Allocation.Error!Value
///
///     fn freeKey(self, Key) void
///
///     fn freeVal(self, Value) void
pub fn Dict(
    comptime Key: type,
    comptime Value: type,
    comptime Context: type,
) type {
    return struct {
        ctx: Context,
        ht: [2]HashTable = .{ .{}, .{} },
        rehashidx: ?usize = null,
        iterators: usize = 0,

        const Self = @This();

        pub const Entry = struct {
            key: Key,
            val: Value,
            next: ?*Entry = null,

            fn create(
                allocator: Allocator,
                ctx: *Context,
                key: Key,
                val: Value,
                next: ?*Entry,
            ) Allocator.Error!*Entry {
                const entry = try allocator.create(Entry);
                entry.* = .{
                    .key = try ctx.dupeKey(key),
                    .val = try ctx.dupeVal(val),
                    .next = next,
                };
                return entry;
            }

            fn setVal(
                self: *Entry,
                ctx: *Context,
                val: Value,
            ) Allocator.Error!void {
                ctx.freeVal(self.val);
                self.val = try ctx.dupeVal(val);
            }

            fn destroy(self: *Entry, allocator: Allocator, ctx: *Context) void {
                ctx.freeKey(self.key);
                ctx.freeVal(self.val);
                allocator.destroy(self);
            }
        };

        pub const yieldCallback = *const fn (*Context) void;

        const HashTable = struct {
            size: usize = 0, // Must be power of two。
            used: usize = 0,
            sizemask: usize = 0, // size - 1
            table: ?[*]?*Entry = null,

            fn create(
                allocator: Allocator,
                len: usize,
            ) Allocator.Error!HashTable {
                const mem = try allocator.alloc(?*Entry, len);
                @memset(mem, null);
                return .{
                    .used = 0,
                    .size = len,
                    .sizemask = len - 1,
                    .table = mem.ptr,
                };
            }

            fn get(self: *const HashTable, idx: usize) ?*Entry {
                return self.table.?[idx];
            }

            fn set(self: *const HashTable, idx: usize, entry: ?*Entry) void {
                self.table.?[idx] = entry;
            }

            fn free(
                self: *HashTable,
                allocator: Allocator,
                ctx: *Context,
                callback: ?yieldCallback,
            ) void {
                var i: usize = 0;
                while (i < self.size and self.used > 0) : (i += 1) {
                    // Give the caller some opportunity to do other tasks.
                    if (callback != null and (i & 65535 == 0)) {
                        callback.?(ctx);
                    }
                    var entry = self.table.?[i];
                    while (entry) |ent| {
                        entry = ent.next;
                        ent.destroy(allocator, ctx);
                        self.used -= 1;
                    }
                }
                self.freeTable(allocator);
                self.reset();
            }

            fn reset(self: *HashTable) void {
                self.used = 0;
                self.size = 0;
                self.sizemask = 0;
                self.table = null;
            }

            fn freeTable(self: *HashTable, allocator: Allocator) void {
                if (self.table) |table| {
                    allocator.free(table[0..self.size]);
                }
            }
        };

        pub fn create(ctx: Context) Self {
            return .{ .ctx = ctx };
        }

        pub fn size(self: *Self) usize {
            return self.ht[0].used + self.ht[1].used;
        }

        pub fn slots(self: *Self) usize {
            return self.ht[0].size + self.ht[1].size;
        }

        pub fn add(
            self: *Self,
            allocator: Allocator,
            key: Key,
            val: Value,
        ) Allocator.Error!bool {
            if (self.isRehashing()) self.stepRehash(allocator);
            if (try self.expandIfNeeded(allocator) == false) {
                return false;
            }

            const index = self.freeIndexFor(key) orelse return false;
            var ht: *HashTable = if (self.isRehashing()) &self.ht[1] else &self.ht[0];
            const next = ht.get(index);
            const entry = try Entry.create(
                allocator,
                &self.ctx,
                key,
                val,
                next,
            );
            ht.set(index, entry);
            ht.used += 1;
            return true;
        }

        /// Return `.add` if the key was added from scratch, `.replace` if there
        /// was already an element with such key and `replace()` just performed
        /// update operation and discard the old value.
        pub fn replace(
            self: *Self,
            allocator: Allocator,
            key: Key,
            val: Value,
        ) Allocator.Error!ReplaceResult {
            if (try self.add(allocator, key, val)) {
                return .add;
            }
            const entry = self.find(allocator, key).?;
            try entry.setVal(&self.ctx, val);
            return .replace;
        }

        pub fn find(self: *Self, allocator: Allocator, key: Key) ?*Entry {
            if (self.ht[0].size == 0) return null;
            if (self.isRehashing()) self.stepRehash(allocator);

            const hash: Hash = self.ctx.hash(key);
            for (0..self.ht.len) |i| {
                var ht: *HashTable = &self.ht[i];
                const idx = hash & ht.sizemask;
                var entry = ht.get(idx);
                while (entry) |ent| {
                    if (self.ctx.eql(key, ent.key)) {
                        return ent;
                    }
                    entry = ent.next;
                }
                if (!self.isRehashing()) return null;
            }
            return null;
        }

        pub fn fetchValue(self: *Self, allocator: Allocator, key: Key) ?Value {
            const entry = self.find(allocator, key) orelse return null;
            return entry.val;
        }

        pub fn getRandom(self: *Self, allocator: Allocator) ?*Entry {
            if (self.size() == 0) return null;
            if (self.isRehashing()) self.stepRehash(allocator);

            var prng = std.Random.DefaultPrng.init(
                @intCast(std.time.microTimestamp()),
            );
            var rand = prng.random();

            const h0 = &self.ht[0];
            const h1 = &self.ht[1];
            var entry: ?*Entry = null;
            if (self.isRehashing()) {
                const rehashidx = self.rehashidx.?;
                while (entry == null) {
                    const random = rand.int(u32);
                    const idx = rehashidx + (random % (h0.size + h1.size - rehashidx));
                    entry = if (idx >= h0.size) h1.get(idx - h0.size) else h0.get(idx);
                }
            } else {
                while (entry == null) {
                    const random = rand.int(u32);
                    const idx = random & h0.sizemask;
                    entry = h0.get(idx);
                }
            }

            const head = entry.?;
            var list_len: usize = 0;
            while (entry != null) {
                entry = entry.?.next;
                list_len += 1;
            }
            const random = rand.int(u32);
            var list_ele = random % list_len;
            entry = head;
            while (list_ele > 0) : (list_ele -= 1) {
                entry = entry.?.next;
            }
            return entry;
        }

        pub fn delete(self: *Self, allocator: Allocator, key: Key) bool {
            if (self.ht[0].size == 0) return false;
            if (self.isRehashing()) self.stepRehash(allocator);

            const hash: Hash = self.ctx.hash(key);
            for (0..self.ht.len) |i| {
                var prev: ?*Entry = null;
                var ht: *HashTable = &self.ht[i];
                const idx = hash & ht.sizemask;
                var entry = ht.get(idx);
                while (entry) |ent| {
                    if (self.ctx.eql(key, ent.key)) {
                        if (prev) |prv| {
                            prv.next = ent.next;
                        } else {
                            ht.set(idx, ent.next);
                        }
                        ent.destroy(allocator, &self.ctx);
                        ht.used -= 1;
                        return true;
                    }
                    prev = ent;
                    entry = ent.next;
                }
                if (!self.isRehashing()) break;
            }
            return false;
        }

        pub fn isRehashing(self: *Self) bool {
            return self.rehashidx != null;
        }

        pub fn expand(
            self: *Self,
            allocator: Allocator,
            len: usize,
        ) Allocator.Error!bool {
            if (self.isRehashing() or self.ht[0].used > len) {
                return false;
            }

            const realsize = nextPower(len);
            if (realsize == self.ht[0].size) {
                return false;
            }

            const n = try HashTable.create(allocator, realsize);

            // First initialization.
            if (self.ht[0].size == 0) {
                self.ht[0] = n;
                return true;
            }

            // For incremental rehashing.
            self.ht[1] = n;
            self.rehashidx = 0;
            return true;
        }

        /// Rehash between ms and ms+1.
        pub fn rehashMilliseconds(self: *Self, ms: usize) usize {
            const start = std.time.milliTimestamp();
            var rehashes: usize = 0;
            const n = 100;
            while (self.rehash(n)) {
                rehashes += n;
                const end = std.time.milliTimestamp();
                if (end - start > ms) break;
            }
            return rehashes;
        }

        pub fn empty(
            self: *Self,
            allocator: Allocator,
            callback: ?yieldCallback,
        ) void {
            self.ht[0].free(allocator, &self.ctx, callback);
            self.ht[1].free(allocator, &self.ctx, callback);
            self.iterators = 0;
            self.rehashidx = null;
        }

        pub fn destroy(self: *Self, allocator: Allocator) void {
            self.ht[0].free(allocator, &self.ctx, null);
            self.ht[1].free(allocator, &self.ctx, null);
            self.* = undefined;
        }

        /// Return the index where the key should be inserted, or return null
        /// if `key` exists.
        fn freeIndexFor(self: *Self, key: Key) ?usize {
            const hash: Hash = self.ctx.hash(key);
            var idx: ?usize = null;
            for (0..self.ht.len) |i| {
                var ht: *HashTable = &self.ht[i];
                idx = hash & ht.sizemask;
                var entry = ht.get(idx.?);
                while (entry) |ent| {
                    if (self.ctx.eql(key, ent.key)) {
                        return null;
                    }
                    entry = ent.next;
                }
                if (!self.isRehashing()) break;
            }
            return idx;
        }

        fn expandIfNeeded(
            self: *Self,
            allocator: Allocator,
        ) Allocator.Error!bool {
            if (self.isRehashing()) return true;
            const h0 = &self.ht[0];
            if (h0.size == 0) {
                return self.expand(allocator, HT_INITIAL_SIZE);
            }

            const hused = h0.used;
            const hsize = h0.size;
            const ratio = hused / hsize;
            const force_resize = ratio > force_resize_ratio;
            if (hused >= hsize and (dict_can_resize or force_resize)) {
                return self.expand(allocator, hused * 2);
            }
            return true;
        }

        fn stepRehash(self: *Self, allocator: Allocator) void {
            if (self.iterators != 0) return;
            _ = self.rehash(allocator, 1);
        }

        /// More to rehash if returning true.
        fn rehash(self: *Self, allocator: Allocator, n: usize) bool {
            if (!self.isRehashing()) return false;

            var empty_visits = n * 10;
            var h0: *HashTable = &self.ht[0];
            var h1: *HashTable = &self.ht[1];
            var limit = n;
            while (limit > 0 and h0.used > 0) : (limit -= 1) {
                while (h0.get(self.rehashidx.?) == null) {
                    empty_visits -= 1;
                    self.rehashidx.? += 1;
                    if (empty_visits == 0) return true;
                }
                var entry = h0.get(self.rehashidx.?);
                while (entry) |ent| {
                    entry = ent.next;
                    const hash: Hash = self.ctx.hash(ent.key);
                    const idx = hash & h1.sizemask;
                    ent.next = h1.get(idx);
                    h1.set(idx, ent);
                    h1.used += 1;
                    h0.used -= 1;
                }
                h0.set(self.rehashidx.?, null);
                self.rehashidx.? += 1;
            }

            if (h0.used == 0) {
                h0.freeTable(allocator);
                h0.* = h1.*;
                h1.reset();
                self.rehashidx = null;
                return false;
            }

            return true;
        }
    };
}

fn nextPower(size: usize) usize {
    var i: usize = HT_INITIAL_SIZE;
    const max = std.math.maxInt(isize);
    if (size >= max) return max;
    while (true) {
        if (i >= size) {
            return i;
        }
        i *= 2;
    }
}

test "Dict.add | Dict.find | Dict.fetchVal" {
    const allocator = testing.allocator;
    var dict: UnitTestDict = .create(.init(allocator));
    defer dict.destroy(allocator);

    const cnt = 1024;
    for (0..cnt) |i| {
        const key = std.fmt.allocPrint(
            allocator,
            "key-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(key);

        const val = std.fmt.allocPrint(
            allocator,
            "val-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(val);

        var added = try dict.add(allocator, key, val);
        try testing.expect(added);

        added = try dict.add(allocator, key, val);
        try testing.expect(!added);
    }

    for (0..cnt) |i| {
        const key = std.fmt.allocPrint(
            allocator,
            "key-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(key);

        const val = std.fmt.allocPrint(
            allocator,
            "val-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(val);

        const found = dict.find(allocator, key);
        try testing.expect(found != null);
        try testing.expectEqualStrings(key, found.?.key);
        try testing.expectEqualStrings(val, found.?.val);

        const fetched_val = dict.fetchValue(allocator, key);
        try testing.expectEqualStrings(val, fetched_val.?);
    }

    try testing.expectEqual(cnt, dict.size());
}

test "Dict.getRandom" {
    const allocator = testing.allocator;
    var dict: UnitTestDict = .create(.init(allocator));
    defer dict.destroy(allocator);

    try testing.expect(dict.getRandom(allocator) == null);

    const cnt = 1024;
    for (0..cnt) |i| {
        const key = std.fmt.allocPrint(
            allocator,
            "key-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(key);

        const val = std.fmt.allocPrint(
            allocator,
            "val-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(val);
        _ = try dict.add(allocator, key, val);
    }

    const entry = dict.getRandom(allocator);
    try testing.expect(entry != null);
}

test "Dict.replace" {
    const allocator = testing.allocator;
    var dict: UnitTestDict = .create(.init(allocator));
    defer dict.destroy(allocator);

    _ = try dict.add(allocator, "k1", "v1");
    const ret = try dict.replace(allocator, "k1", "v2");
    try testing.expectEqual(ReplaceResult.replace, ret);

    const found = dict.find(allocator, "k1");
    try testing.expectEqualStrings("v2", found.?.val);
}

test "Dict.delete" {
    const allocator = testing.allocator;
    var dict: UnitTestDict = .create(.init(allocator));
    defer dict.destroy(allocator);

    _ = try dict.add(allocator, "k1", "v1");
    const deleted = dict.delete(allocator, "k1");

    try testing.expect(deleted);
    try testing.expectEqual(0, dict.size());
}

test "Dict.expand | Dict.rehash" {
    const allocator = testing.allocator;
    var dict: UnitTestDict = .create(.init(allocator));
    defer dict.destroy(allocator);

    var expanded = try dict.expand(allocator, 1023);
    try testing.expect(expanded);
    try testing.expectEqual(1024, dict.ht[0].size);
    try testing.expectEqual(0, dict.ht[1].size);

    _ = try dict.add(allocator, "k1", "v1");

    expanded = try dict.expand(allocator, 2045);
    try testing.expect(expanded);
    try testing.expect(dict.isRehashing());
    try testing.expectEqual(1024, dict.ht[0].size);
    try testing.expectEqual(2048, dict.ht[1].size);

    const more = dict.rehash(allocator, 1024);
    try testing.expect(more == false);
    try testing.expectEqual(2048, dict.ht[0].size);
    try testing.expectEqual(1, dict.ht[0].used);
    try testing.expectEqual(0, dict.ht[1].size);
    try testing.expectEqual(0, dict.ht[1].used);
    try testing.expectEqual(1, dict.size());
}

const UnitTestDict = Dict(
    UnitTestContext.Key,
    UnitTestContext.Value,
    UnitTestContext,
);

const UnitTestContext = struct {
    const Self = @This();
    const Key = []const u8;
    const Value = []const u8;

    allocator: Allocator,

    fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    fn eql(self: *Self, key: Key, other: Key) bool {
        _ = self;
        return std.mem.eql(u8, key, other);
    }

    fn hash(self: *Self, key: Key) Hash {
        _ = self;
        return std.hash.Wyhash.hash(0, key);
    }

    fn dupeKey(self: *Self, key: Key) Allocator.Error!Key {
        return try self.allocator.dupe(u8, key);
    }

    fn dupeVal(self: *Self, val: Value) Allocator.Error!Value {
        return try self.allocator.dupe(u8, val);
    }

    fn freeKey(self: *Self, key: Key) void {
        self.allocator.free(key);
    }

    fn freeVal(self: *Self, val: Value) void {
        self.allocator.free(val);
    }
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
