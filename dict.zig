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
///     pub fn eql(self, Key, Key) bool
///
///     pub fn hash(self, Key) Hash
///
///     pub fn dupeKey(self, Allocator, Key) Allocator.Error!Key
///
///     pub fn dupeVal(self, Allocator, Value) Allocator.Error!Value
///
///     pub fn freeKey(self, Allocator, Key) void
///
///     pub fn freeVal(self, Allocator, Value) void
///
///     pub fn yield(self) void
///
///     pub fn scan(self, *Entry) void
///
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
                ctx: Context,
                key: Key,
                val: Value,
                next: ?*Entry,
            ) Allocator.Error!*Entry {
                const entry = try allocator.create(Entry);
                entry.* = .{
                    .key = try ctx.dupeKey(allocator, key),
                    .val = try ctx.dupeVal(allocator, val),
                    .next = next,
                };
                return entry;
            }

            fn setVal(
                self: *Entry,
                allocator: Allocator,
                ctx: Context,
                val: Value,
            ) Allocator.Error!void {
                ctx.freeVal(allocator, self.val);
                self.val = try ctx.dupeVal(allocator, val);
            }

            fn destroy(self: *Entry, allocator: Allocator, ctx: Context) void {
                ctx.freeKey(allocator, self.key);
                ctx.freeVal(allocator, self.val);
                allocator.destroy(self);
            }
        };

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
                ctx: Context,
            ) void {
                var i: usize = 0;
                while (i < self.size and self.used > 0) : (i += 1) {
                    // Give the caller some opportunity to do other tasks.
                    if ((i & 65535 == 0)) {
                        ctx.yield();
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

        pub const Iterator = struct {
            dict: *Self,
            index: isize,
            table: u32,
            safe: bool,
            entry: ?*Entry,
            next_entry: ?*Entry,
            fingerprint: i64,

            pub fn next(self: *Iterator) ?*Entry {
                while (true) {
                    if (self.entry == null) {
                        var ht: *HashTable = &self.dict.ht[self.table];
                        if (self.index == -1 and self.table == 0) {
                            if (self.safe) {
                                self.dict.iterators += 1;
                            } else {
                                self.fingerprint = self.dict.fingerprint();
                            }
                        }
                        self.index += 1;
                        if (self.index > ht.size) {
                            if (self.dict.isRehashing() and self.table == 0) {
                                self.table += 1;
                                self.index = 0;
                                ht = &self.dict.ht[1];
                            } else {
                                break;
                            }
                        }
                        const index: usize = @intCast(self.index);
                        self.entry = ht.get(index);
                    } else {
                        self.entry = self.next_entry;
                    }
                    if (self.entry) |ent| {
                        self.next_entry = ent.next;
                        return ent;
                    }
                }
                return null;
            }

            pub fn release(self: *Iterator) void {
                if (!(self.index == -1 and self.table == 0)) {
                    if (self.safe) {
                        self.dict.iterators -= 1;
                    } else {
                        std.debug.assert(self.fingerprint == self.dict.fingerprint());
                    }
                }
            }
        };

        pub fn iterator(self: *Self, safe: bool) Iterator {
            const it: Iterator = .{
                .dict = self,
                .table = 0,
                .index = -1,
                .safe = safe,
                .entry = null,
                .next_entry = null,
                .fingerprint = undefined,
            };
            return it;
        }

        pub fn create(
            allocator: Allocator,
            ctx: Context,
        ) Allocator.Error!*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .ctx = ctx,
            };
            return self;
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
            if (self.isRehashing()) self.rehashStep(allocator);
            if (try self.expandIfNeeded(allocator) == false) {
                return false;
            }

            const index = self.freeIndexFor(key) orelse return false;
            var ht: *HashTable = if (self.isRehashing()) &self.ht[1] else &self.ht[0];
            const next = ht.get(index);
            const entry = try Entry.create(
                allocator,
                self.ctx,
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
        /// update operation and call `ctx.freeVal()` to free old value.
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
            try entry.setVal(allocator, self.ctx, val);
            return .replace;
        }

        pub fn find(self: *Self, allocator: Allocator, key: Key) ?*Entry {
            if (self.ht[0].size == 0) return null;
            if (self.isRehashing()) self.rehashStep(allocator);

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
            if (self.isRehashing()) self.rehashStep(allocator);

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

        pub fn getSome(
            self: *Self,
            allocator: Allocator,
            n: usize,
        ) Allocator.Error![]*Entry {
            const count: usize = @min(self.size(), n);

            for (0..count) |_| {
                if (self.isRehashing()) {
                    self.rehashStep(allocator);
                } else {
                    break;
                }
            }

            var items = try std.ArrayList(*Entry).initCapacity(
                allocator,
                count,
            );
            errdefer items.deinit(allocator);

            const h0 = &self.ht[0];
            const h1 = &self.ht[1];
            const tables: usize = if (self.isRehashing()) 2 else 1;
            var max_sizemask = h0.sizemask;
            if (tables > 1 and max_sizemask < h1.sizemask) {
                max_sizemask = h1.sizemask;
            }

            var prng = std.Random.DefaultPrng.init(
                @intCast(std.time.microTimestamp()),
            );
            var rand = prng.random();

            var i = rand.int(u32) & max_sizemask;
            var empty_visits: usize = 0;
            var stored: usize = 0;
            var max_steps = count * 10;
            outer: while (stored < count and max_steps > 0) : (max_steps -= 1) {
                for (0..tables) |j| {
                    if (tables == 2 and j == 0 and i < self.rehashidx.?) {
                        // This happens when going from big to small table.
                        if (i >= h1.size) i = self.rehashidx.?;
                        continue;
                    }
                    if (i >= self.ht[j].size) continue;
                    var entry = self.ht[j].get(i);
                    if (entry == null) {
                        empty_visits += 1;
                        if (empty_visits >= 5 and empty_visits > count) {
                            i = rand.int(u32) & max_sizemask;
                            empty_visits = 0;
                        }
                    } else {
                        empty_visits = 0;
                        while (entry) |ent| {
                            try items.append(allocator, ent);
                            entry = ent.next;
                            stored += 1;
                            if (stored == count) break :outer;
                        }
                    }
                }
                i = (i + 1) & max_sizemask; // Increment index by 1.
            }

            return try items.toOwnedSlice(allocator);
        }

        pub fn delete(self: *Self, allocator: Allocator, key: Key) bool {
            if (self.ht[0].size == 0) return false;
            if (self.isRehashing()) self.rehashStep(allocator);

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
                        ent.destroy(allocator, self.ctx);
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

            const real_size = nextPower(len);
            if (real_size == self.ht[0].size) {
                return false;
            }

            const n = try HashTable.create(allocator, real_size);

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
        pub fn rehashMilliseconds(self: *Self, ms: usize) void {
            const start = std.time.milliTimestamp();
            const n = 100;
            while (self.rehash(n)) {
                const end = std.time.milliTimestamp();
                if (end - start > ms) break;
            }
        }

        pub fn empty(
            self: *Self,
            allocator: Allocator,
        ) void {
            self.ht[0].free(allocator, self.ctx);
            self.ht[1].free(allocator, self.ctx);
            self.iterators = 0;
            self.rehashidx = null;
        }

        pub fn destroy(self: *Self, allocator: Allocator) void {
            self.ht[0].free(allocator, self.ctx);
            self.ht[1].free(allocator, self.ctx);
            allocator.destroy(self);
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

        fn rehashStep(self: *Self, allocator: Allocator) void {
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

        fn fingerprint(self: *Self) i64 {
            const h0 = self.ht[0];
            const h1 = self.ht[1];

            var intergers: [6]i64 = undefined;

            intergers[0] = if (h0.table == null) 0 else @bitCast(@intFromPtr(h0.table));
            intergers[1] = @bitCast(h0.size);
            intergers[2] = @bitCast(h0.used);

            intergers[3] = if (h1.table == null) 0 else @bitCast(@intFromPtr(h1.table));
            intergers[4] = @bitCast(h1.size);
            intergers[5] = @bitCast(h1.used);

            var hash: i64 = 0;
            var j: u8 = 0;
            while (j < 6) : (j += 1) {
                hash +%= intergers[j];
                hash = (~hash) +% (hash << 21);
                hash = hash ^ (hash >> 24);
                hash = (hash +% (hash << 3)) +% (hash << 8);
                hash = hash ^ (hash >> 14);
                hash = (hash +% (hash << 2)) +% (hash << 4);
                hash = hash ^ (hash >> 28);
                hash = hash +% (hash << 31);
            }
            return hash;
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
    const dict: *UnitTestDict = try .create(allocator, .init());
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
    const dict: *UnitTestDict = try .create(allocator, .init());
    defer dict.destroy(allocator);

    try testing.expect(dict.getRandom(allocator) == null);

    try UnitTestContext.addMany(dict, allocator, 1024);

    const entry = dict.getRandom(allocator);
    try testing.expect(entry != null);
}

test "Dict.getSome" {
    const allocator = testing.allocator;
    const dict: *UnitTestDict = try .create(allocator, .init());
    defer dict.destroy(allocator);

    try UnitTestContext.addMany(dict, allocator, 1024);

    const items = try dict.getSome(allocator, 100);
    defer allocator.free(items);
    try testing.expectEqual(100, items.len);
}

test "Dict.replace" {
    const allocator = testing.allocator;
    const dict: *UnitTestDict = try .create(allocator, .init());
    defer dict.destroy(allocator);

    _ = try dict.add(allocator, "k1", "v1");
    const ret = try dict.replace(allocator, "k1", "v2");
    try testing.expectEqual(ReplaceResult.replace, ret);

    const found = dict.find(allocator, "k1");
    try testing.expectEqualStrings("v2", found.?.val);
}

test "Dict.delete" {
    const allocator = testing.allocator;
    const dict: *UnitTestDict = try .create(allocator, .init());
    defer dict.destroy(allocator);

    _ = try dict.add(allocator, "k1", "v1");
    const deleted = dict.delete(allocator, "k1");

    try testing.expect(deleted);
    try testing.expectEqual(0, dict.size());
}

test "Dict.expand | Dict.rehash" {
    const allocator = testing.allocator;
    const dict: *UnitTestDict = try .create(allocator, .init());
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

test "Dict.iterator | Dict.fingerprint" {
    const allocator = testing.allocator;
    const dict: *UnitTestDict = try .create(allocator, .init());
    defer dict.destroy(allocator);

    const count = 1024;
    try UnitTestContext.addMany(dict, allocator, count);

    var iter = dict.iterator(false);

    const fingerprint = dict.fingerprint();
    var iters: usize = 0;
    while (iter.next()) |_| {
        iters += 1;
    }
    try testing.expectEqual(count, iters);
    iter.release();

    _ = try dict.add(allocator, "somekey1", "somevalue1");
    try testing.expect(fingerprint != dict.fingerprint());
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

    fn addMany(
        dict: *UnitTestDict,
        allocator: Allocator,
        cnt: usize,
    ) Allocator.Error!void {
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
    }

    fn init() Self {
        return .{};
    }

    fn eql(self: Self, key: Key, other: Key) bool {
        _ = self;
        return std.mem.eql(u8, key, other);
    }

    fn hash(self: Self, key: Key) Hash {
        _ = self;
        return std.hash.Wyhash.hash(0, key);
    }

    fn dupeKey(_: Self, allocator: Allocator, key: Key) Allocator.Error!Key {
        return try allocator.dupe(u8, key);
    }

    fn dupeVal(_: Self, allocator: Allocator, val: Value) Allocator.Error!Value {
        return try allocator.dupe(u8, val);
    }

    fn freeKey(_: Self, allocator: Allocator, key: Key) void {
        allocator.free(key);
    }

    fn freeVal(_: Self, allocator: Allocator, val: Value) void {
        allocator.free(val);
    }

    fn yield(_: Self) void {}

    fn scan(_: Self, _: *UnitTestDict.Entry) void {}
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
