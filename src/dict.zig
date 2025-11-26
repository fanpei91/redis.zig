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

var hash_seed: u64 = 0;

pub fn hashSeed(s: ?u64) void {
    hash_seed = s orelse std.crypto.random.int(u64);
}

pub fn genHash(key: []const u8) Hash {
    return std.hash.Wyhash.hash(
        hash_seed,
        key,
    );
}

pub fn genCaseHash(key: []const u8, comptime max_len: usize) Hash {
    var stack_impl = std.heap.stackFallback(
        max_len,
        allocator.child,
    );
    const stack_allocator = stack_impl.get();
    const s = sds.newLenAlloc(stack_allocator, key.ptr, key.len);
    defer sds.freeAlloc(stack_allocator, s);
    sds.toLower(s);
    return genHash(sds.asBytes(s));
}

pub fn Dict(comptime Key: type, comptime Value: type) type {
    return struct {
        pub const VTable = struct {
            hash: *const fn (key: Key) Hash,
            eql: ?*const fn (key1: Key, key2: Key) bool = null,
            dupeKey: ?*const fn (key: Key) Key = null,
            dupeVal: ?*const fn (val: Value) Value = null,
            freeKey: ?*const fn (key: Key) void = null,
            freeVal: ?*const fn (val: Value) void = null,
        };

        pub const Entry = struct {
            key: Key,
            val: Value,
            next: ?*Entry = null,

            fn destroy(
                self: *Entry,
                ctx: *Context,
            ) void {
                ctx.freeKey(self.key);
                ctx.freeVal(self.val);
                allocator.destroy(self);
            }
        };

        pub const Iterator = struct {
            dict: *HashMap,
            index: i64,
            table: u8,
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
                        if (self.index >= ht.size) {
                            if (self.dict.isRehashing() and self.table == 0) {
                                self.table = 1;
                                self.index = 0;
                                ht = &self.dict.ht[1];
                            } else {
                                break;
                            }
                        }
                        self.entry = ht.get(@intCast(self.index));
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
                        assert(self.fingerprint == self.dict.fingerprint());
                    }
                }
            }
        };

        const Context = struct {
            vtable: *const VTable,

            fn eql(self: *Context, key1: Key, key2: Key) bool {
                if (self.vtable.eql) |cmp| {
                    return cmp(key1, key2);
                }
                return std.meta.eql(key1, key2);
            }

            fn hash(self: *Context, key: Key) Hash {
                return self.vtable.hash(key);
            }

            fn dupeKey(self: *Context, key: Key) Key {
                if (self.vtable.dupeKey) |dupe| {
                    return dupe(key);
                }
                return key;
            }

            fn dupeVal(self: *Context, val: Value) Value {
                if (self.vtable.dupeVal) |dupe| {
                    return dupe(val);
                }
                return val;
            }

            fn freeKey(self: *Context, key: Key) void {
                if (self.vtable.freeKey) |free| {
                    free(key);
                }
            }

            fn freeVal(self: *Context, val: Value) void {
                if (self.vtable.freeVal) |free| {
                    free(val);
                }
            }
        };

        const HashTable = struct {
            size: u64 = 0, // Must be power of two。
            used: u64 = 0,
            sizemask: u64 = 0, // size - 1
            table: ?[*]?*Entry = null,

            fn create(len: usize) HashTable {
                const mem = allocator.alloc(?*Entry, len);
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
                ctx: *Context,
                callback: ?*const fn () void,
            ) void {
                var i: usize = 0;
                while (i < self.size and self.used > 0) : (i += 1) {
                    // Give the caller some opportunity to do other tasks.
                    if ((i & 65535 == 0)) {
                        if (callback) |cb| {
                            cb();
                        }
                    }
                    var entry = self.table.?[i];
                    while (entry) |ent| {
                        entry = ent.next;
                        ent.destroy(ctx);
                        self.used -= 1;
                    }
                }
                self.freeTable();
                self.reset();
            }

            fn reset(self: *HashTable) void {
                self.used = 0;
                self.size = 0;
                self.sizemask = 0;
                self.table = null;
            }

            fn freeTable(self: *HashTable) void {
                if (self.table) |table| {
                    allocator.free(table[0..self.size]);
                }
            }
        };

        const HashMap = @This();
        ctx: Context,
        ht: [2]HashTable = .{ .{}, .{} },
        rehashidx: i64 = -1,
        iterators: u64 = 0,

        pub fn iterator(self: *HashMap, safe: bool) Iterator {
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

        pub fn create(vtable: *const VTable) *HashMap {
            const self = allocator.create(HashMap);
            self.* = .{
                .ctx = .{ .vtable = vtable },
            };
            return self;
        }

        /// Add an element, discarding the old value if the key already exists.
        /// Return `.add` if the key was added from scratch, `.replace` if
        /// there was already an element with such key and replace() just
        /// performed a value update operation.
        pub fn replace(self: *HashMap, key: Key, val: Value) enum { replace, add } {
            var existing: ?*Entry = null;
            const entry = self.addRaw(key, &existing);
            if (entry) |ent| {
                self.setVal(ent, val);
                return .add;
            }
            self.freeVal(existing.?);
            self.setVal(existing.?, val);
            return .replace;
        }

        /// Add an element to the target hash table. Return FALSE if key exists,
        /// otherwise TRUE.
        pub fn add(self: *HashMap, key: Key, val: Value) bool {
            const entry = self.addRaw(key, null);
            if (entry) |ent| {
                self.setVal(ent, val);
                return true;
            }
            return false;
        }

        /// If key already exists null is returned, and `existing.*` is populated
        /// with the existing entry if existing is not null.
        ///
        /// If key was added, the hash entry is returned to be manipulated by
        /// the caller.
        pub fn addRaw(self: *HashMap, key: Key, existing: ?*?*Entry) ?*Entry {
            if (self.isRehashing()) self.rehashStep();
            if (self.expandIfNeeded() == false) {
                return null;
            }

            const index = self.keyIndex(key, existing) orelse return null;
            var ht: *HashTable = if (self.isRehashing()) &self.ht[1] else &self.ht[0];

            const entry = allocator.create(Entry);

            self.setKey(entry, key);
            entry.next = ht.get(index);

            ht.set(index, entry);
            ht.used += 1;
            return entry;
        }

        /// A version of `addRaw()` that always returns the hash entry of the
        /// specified key, even if the key already exists and can't be
        /// added (in that case the entry of the already existing key is returned.)
        pub fn addOrFind(self: *HashMap, key: Key) *Entry {
            var existing: ?*Entry = null;
            const entry = self.addRaw(key, &existing);
            if (entry) |ent| {
                return ent;
            }
            return existing.?;
        }

        pub fn find(self: *HashMap, key: Key) ?*Entry {
            if (self.ht[0].size == 0) return null;
            if (self.isRehashing()) self.rehashStep();

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

        pub fn fetchValue(self: *HashMap, key: Key) ?Value {
            const entry = self.find(key) orelse return null;
            return entry.val;
        }

        pub fn getRandom(self: *HashMap) ?*Entry {
            if (self.size() == 0) return null;
            if (self.isRehashing()) self.rehashStep();

            const h0 = &self.ht[0];
            const h1 = &self.ht[1];
            var entry: ?*Entry = null;
            if (self.isRehashing()) {
                const rehashidx: u64 = @abs(self.rehashidx);
                while (entry == null) {
                    const random = rand.int(u64);
                    const idx = rehashidx + (random % (h0.size + h1.size - rehashidx));
                    entry = if (idx >= h0.size) h1.get(idx - h0.size) else h0.get(idx);
                }
            } else {
                while (entry == null) {
                    const random = rand.int(u64);
                    const idx = random & h0.sizemask;
                    entry = h0.get(idx);
                }
            }

            const head = entry.?;
            var list_len: u64 = 0;
            while (entry != null) {
                entry = entry.?.next;
                list_len += 1;
            }
            const random = rand.int(u64);
            var list_ele = random % list_len;
            entry = head;
            while (list_ele > 0) : (list_ele -= 1) {
                entry = entry.?.next;
            }
            return entry;
        }

        pub fn getSome(self: *HashMap, n: u32) []*Entry {
            const count: u64 = @min(self.size(), n);

            for (0..count) |_| {
                if (self.isRehashing()) {
                    self.rehashStep();
                } else {
                    break;
                }
            }

            var items = std.ArrayList(*Entry).initCapacity(
                allocator.child,
                count,
            ) catch allocator.oom();

            const h0 = &self.ht[0];
            const h1 = &self.ht[1];
            const tables: u8 = if (self.isRehashing()) 2 else 1;
            var max_sizemask = h0.sizemask;
            if (tables > 1 and max_sizemask < h1.sizemask) {
                max_sizemask = h1.sizemask;
            }

            var i = rand.int(u64) & max_sizemask;
            var empty_visits: usize = 0;
            var stored: usize = 0;
            var max_steps = count * 10;
            outer: while (stored < count and max_steps > 0) : (max_steps -= 1) {
                for (0..tables) |j| {
                    if (tables == 2 and j == 0 and i < self.rehashidx) {
                        // This happens when going from big to small table.
                        if (i >= h1.size) i = @abs(self.rehashidx);
                        continue;
                    }
                    if (i >= self.ht[j].size) continue;
                    var entry = self.ht[j].get(i);
                    if (entry == null) {
                        empty_visits += 1;
                        if (empty_visits >= 5 and empty_visits > count) {
                            i = rand.int(u64) & max_sizemask;
                            empty_visits = 0;
                        }
                    } else {
                        empty_visits = 0;
                        while (entry) |ent| {
                            items.append(
                                allocator.child,
                                ent,
                            ) catch allocator.oom();
                            entry = ent.next;
                            stored += 1;
                            if (stored == count) break :outer;
                        }
                    }
                }
                i = (i + 1) & max_sizemask; // Increment index by 1.
            }

            return items.toOwnedSlice(allocator.child) catch allocator.oom();
        }

        pub const scanEntryFunc = *const fn (privdata: ?*anyopaque, e: *const Entry) void;
        pub const scanBucketFunc = *const fn (privdata: ?*anyopaque, e: *?*Entry) void;

        pub fn scan(
            self: *HashMap,
            cursor: u64,
            scanEntryFn: ?scanEntryFunc,
            scanBucketFn: ?scanBucketFunc,
            privdata: ?*anyopaque,
        ) u64 {
            if (self.size() == 0) return 0;

            var v = cursor;
            var t0: *HashTable = &self.ht[0];
            var m0 = t0.sizemask;

            if (!self.isRehashing()) {
                const idx = v & m0;
                if (scanBucketFn) |scanBucket| {
                    scanBucket(privdata, &t0.table.?[idx]);
                }
                var entry = t0.get(idx);
                while (entry) |ent| {
                    entry = ent.next;
                    if (scanEntryFn) |scanEntry| {
                        scanEntry(privdata, ent);
                    }
                }
            } else {
                var t1: *HashTable = &self.ht[1];
                if (t0.size > t1.size) {
                    t0 = &self.ht[1];
                    t1 = &self.ht[0];
                }

                const idx = v & m0;
                if (scanBucketFn) |scanBucket| {
                    scanBucket(privdata, &t0.table.?[idx]);
                }
                m0 = t0.sizemask;
                var entry = t0.get(idx);
                while (entry) |ent| {
                    entry = ent.next;
                    if (scanEntryFn) |scanEntry| {
                        scanEntry(privdata, ent);
                    }
                }

                const m1 = t1.sizemask;
                while (true) {
                    const i = v & m1;
                    if (scanBucketFn) |scanBucket| {
                        scanBucket(privdata, &t1.table.?[i]);
                    }
                    entry = t1.get(i);
                    while (entry) |ent| {
                        entry = ent.next;
                        if (scanEntryFn) |scanEntry| {
                            scanEntry(privdata, ent);
                        }
                    }
                    v = (((v | m0) + 1) & ~m0) | (v & m0);
                    if (v & (m0 ^ m1) == 0) break;
                }
            }

            v |= ~m0;
            v = @bitReverse(v);
            v +%= 1;
            v = @bitReverse(v);
            return v;
        }

        pub fn delete(self: *HashMap, key: Key) bool {
            const deleted = self.genericDelete(key, true);
            return deleted != null;
        }

        /// Shrink the table to the minimal size that contains all the elements,
        /// but with the invariant of a USED/BUCKETS ratio near to <= 1
        pub fn shrinkToFit(self: *HashMap) bool {
            if (!dict_can_resize or self.isRehashing()) {
                return false;
            }
            var minimal = self.ht[0].used;
            if (minimal < HT_INITIAL_SIZE) {
                minimal = HT_INITIAL_SIZE;
            }
            return self.expand(minimal);
        }

        pub fn unlink(self: *HashMap, key: Key) ?*Entry {
            return self.genericDelete(key, false);
        }

        /// You need to call this function to really free the entry after a call
        /// to unlink().
        pub fn freeUnlinkedEntry(self: *HashMap, entry: *Entry) void {
            entry.destroy(&self.ctx);
        }

        fn genericDelete(self: *HashMap, key: Key, free: bool) ?*Entry {
            if (self.ht[0].used == 0 and self.ht[1].used == 0) return null;
            if (self.isRehashing()) self.rehashStep();

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
                        if (free) ent.destroy(&self.ctx);
                        ht.used -= 1;
                        return ent;
                    }
                    prev = ent;
                    entry = ent.next;
                }
                if (!self.isRehashing()) break;
            }
            return null;
        }

        pub fn isRehashing(self: *HashMap) bool {
            return self.rehashidx != -1;
        }

        pub fn expand(self: *HashMap, len: u64) bool {
            if (self.isRehashing() or self.ht[0].used > len) {
                return false;
            }

            const real_size = nextPower(len);
            if (real_size == self.ht[0].size) {
                return false;
            }

            const n = HashTable.create(real_size);

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
        pub fn rehashMilliseconds(self: *HashMap, ms: usize) void {
            const start = std.time.milliTimestamp();
            const n = 100;
            while (self.rehash(n)) {
                const end = std.time.milliTimestamp();
                if (end - start > ms) break;
            }
        }

        pub fn empty(
            self: *HashMap,
            callback: ?*const fn () void,
        ) void {
            self.ht[0].free(&self.ctx, callback);
            self.ht[1].free(&self.ctx, callback);
            self.iterators = 0;
            self.rehashidx = -1;
        }

        pub fn size(self: *HashMap) u64 {
            return self.ht[0].used + self.ht[1].used;
        }

        pub fn slots(self: *HashMap) u64 {
            return self.ht[0].size + self.ht[1].size;
        }

        pub fn setKey(self: *HashMap, entry: *Entry, key: Key) void {
            entry.key = self.ctx.dupeKey(key);
        }

        pub fn freeKey(self: *HashMap, entry: *Entry) void {
            self.ctx.freeKey(entry.key);
        }

        pub fn setVal(self: *HashMap, entry: *Entry, val: Value) void {
            entry.val = self.ctx.dupeVal(val);
        }

        pub fn freeVal(self: *HashMap, entry: *Entry) void {
            self.ctx.freeVal(entry.val);
        }

        pub fn destroy(self: *HashMap) void {
            self.ht[0].free(&self.ctx, null);
            self.ht[1].free(&self.ctx, null);
            allocator.destroy(self);
        }

        /// Return the index where the key should be inserted, or return null
        /// if `key` exists, and the optional output parameter may be filled.
        fn keyIndex(self: *HashMap, key: Key, existing: ?*?*Entry) ?u64 {
            if (existing) |exit| exit.* = null;

            const hash: Hash = self.ctx.hash(key);
            var idx: ?u64 = null;
            for (0..self.ht.len) |i| {
                var ht: *HashTable = &self.ht[i];
                idx = hash & ht.sizemask;
                var entry = ht.get(idx.?);
                while (entry) |ent| {
                    if (self.ctx.eql(key, ent.key)) {
                        if (existing) |exit| exit.* = ent;
                        return null;
                    }
                    entry = ent.next;
                }
                if (!self.isRehashing()) break;
            }
            return idx;
        }

        fn expandIfNeeded(self: *HashMap) bool {
            if (self.isRehashing()) return true;
            const h0 = &self.ht[0];
            if (h0.size == 0) {
                return self.expand(HT_INITIAL_SIZE);
            }

            const hused = h0.used;
            const hsize = h0.size;
            const ratio = hused / hsize;
            const force_resize = ratio > force_resize_ratio;
            if (hused >= hsize and (dict_can_resize or force_resize)) {
                return self.expand(hused * 2);
            }
            return true;
        }

        fn rehashStep(self: *HashMap) void {
            if (self.iterators != 0) return;
            _ = self.rehash(1);
        }

        /// More to rehash if returning true.
        fn rehash(self: *HashMap, n: usize) bool {
            if (!self.isRehashing()) return false;

            var empty_visits = n * 10;
            var h0: *HashTable = &self.ht[0];
            var h1: *HashTable = &self.ht[1];
            var limit = n;
            while (limit > 0 and h0.used > 0) : (limit -= 1) {
                while (h0.get(@intCast(self.rehashidx)) == null) {
                    empty_visits -= 1;
                    self.rehashidx += 1;
                    if (empty_visits == 0) return true;
                }
                var entry = h0.get(@intCast(self.rehashidx));
                while (entry) |ent| {
                    entry = ent.next;
                    const hash: Hash = self.ctx.hash(ent.key);
                    const idx = hash & h1.sizemask;
                    ent.next = h1.get(idx);
                    h1.set(idx, ent);
                    h1.used += 1;
                    h0.used -= 1;
                }
                h0.set(@intCast(self.rehashidx), null);
                self.rehashidx += 1;
            }

            if (h0.used == 0) {
                h0.freeTable();
                h0.* = h1.*;
                h1.reset();
                self.rehashidx = -1;
                return false;
            }

            return true;
        }

        fn fingerprint(self: *HashMap) i64 {
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

fn nextPower(sz: u64) u64 {
    var i: u64 = HT_INITIAL_SIZE;
    const max = std.math.maxInt(i64);
    if (sz >= max) return max + 1;
    while (true) {
        if (i >= sz) {
            return i;
        }
        i *= 2;
    }
}

test "add() | addOrFind() || find() | fetchVal()" {
    const dict = TestDict.create();
    defer dict.destroy();

    const cnt = 100;
    for (0..cnt) |i| {
        const key = std.fmt.allocPrint(
            allocator.child,
            "key-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(key);

        const val = std.fmt.allocPrint(
            allocator.child,
            "val-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(val);

        const skey = sds.new(key);
        const sval = sds.new(val);
        var added = dict.add(
            skey,
            sval,
        );
        try expect(added);

        added = dict.add(
            skey,
            sval,
        );
        try expect(!added);
    }

    for (0..cnt) |i| {
        const key = std.fmt.allocPrint(
            allocator.child,
            "key-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(key);

        const val = std.fmt.allocPrint(
            allocator.child,
            "val-{}",
            .{i},
        ) catch unreachable;
        defer allocator.free(val);

        const skey = sds.new(key);
        defer sds.free(skey);
        const sval = sds.new(val);
        defer sds.free(sval);

        const found = dict.find(skey);
        try expect(found != null);
        try expectEqualStrings(
            sds.asBytes(skey),
            sds.asBytes(found.?.key),
        );
        try expectEqualStrings(
            sds.asBytes(sval),
            sds.asBytes(found.?.val),
        );

        const fetched_val = dict.fetchValue(skey);
        try expectEqualStrings(
            sds.asBytes(sval),
            sds.asBytes(fetched_val.?),
        );
    }

    try expectEqual(cnt, dict.size());

    const key = sds.new("addOrFind");
    const new_entry = dict.addOrFind(key);
    const val = sds.new("value");
    dict.setVal(new_entry, val);
    const existing_entry = dict.addOrFind(key);
    try expectEqual(new_entry, existing_entry);
    try expectEqualStrings(
        sds.asBytes(val),
        sds.asBytes(existing_entry.val),
    );
}

test "getRandom()" {
    const dict = TestDict.create();
    defer dict.destroy();

    try expect(dict.getRandom() == null);

    TestDict.batchAdd(dict, 100);

    const entry = dict.getRandom();
    try expect(entry != null);
}

test "getSome()" {
    const dict = TestDict.create();
    defer dict.destroy();

    TestDict.batchAdd(dict, 100);

    const items = dict.getSome(10);
    defer allocator.free(items);
    try expectEqual(10, items.len);
}

test "replace()" {
    const dict = TestDict.create();
    defer dict.destroy();

    const key = sds.new("k1");
    _ = dict.add(key, sds.new("v1"));
    const ret = dict.replace(key, sds.new("v2"));
    try expectEqual(.replace, ret);

    const found = dict.find(key);
    try expectEqualStrings("v2", sds.asBytes(found.?.val));
}

test "delete()" {
    const dict = TestDict.create();
    defer dict.destroy();

    const key = sds.new("k1");
    _ = dict.add(key, sds.new("v1"));
    const deleted = dict.delete(key);

    try expect(deleted);
    try expectEqual(0, dict.size());
}

test "unlink() | freeUnlinkedEntry()" {
    const dict = TestDict.create();
    defer dict.destroy();

    const key = sds.new("key1");
    _ = dict.add(key, sds.new("val2"));
    const unlinked = dict.unlink(key);
    try expect(unlinked != null);
    try expectEqual(0, dict.size());
    dict.freeUnlinkedEntry(unlinked.?);
}

test "expand() | rehash()" {
    const dict = TestDict.create();
    defer dict.destroy();

    var expanded = dict.expand(1023);
    try expect(expanded);
    try expectEqual(1024, dict.ht[0].size);
    try expectEqual(0, dict.ht[1].size);

    TestDict.batchAdd(dict, 1);

    expanded = dict.expand(2045);
    try expect(expanded);
    try expect(dict.isRehashing());
    try expectEqual(1024, dict.ht[0].size);
    try expectEqual(2048, dict.ht[1].size);

    const more = dict.rehash(1024);
    try expect(more == false);
    try expectEqual(2048, dict.ht[0].size);
    try expectEqual(1, dict.ht[0].used);
    try expectEqual(0, dict.ht[1].size);
    try expectEqual(0, dict.ht[1].used);
    try expectEqual(1, dict.size());
}

test "iterator() | fingerprint()" {
    const dict = TestDict.create();
    defer dict.destroy();

    const count = 10;
    TestDict.batchAdd(dict, count);

    var iter = dict.iterator(false);

    const fp = dict.fingerprint();
    var iters: usize = 0;
    while (iter.next()) |_| {
        iters += 1;
    }
    try expectEqual(count, iters);
    iter.release();

    const key = sds.new("nokey");
    defer sds.free(key);
    _ = dict.find(key);
    try expect(fp != dict.fingerprint());
}

test "scan()" {
    const dict = TestDict.create();
    defer dict.destroy();

    TestDict.batchAdd(dict, 3);

    _ = dict.expand(8);

    var times: usize = 0;
    var cursor: usize = 0;
    while (true) {
        cursor = dict.scan(cursor, null, null, null);
        times += 1;
        if (cursor == 0) break;
    }
    try expect(times != 0);
}

const TestDict = struct {
    const Self = @This();

    const HashMap = Dict(sds.String, sds.String);

    fn create() *HashMap {
        return HashMap.create(vtable);
    }

    const vtable: *const HashMap.VTable = &.{
        .hash = hash,
        .eql = eql,
        .dupeKey = null,
        .dupeVal = null,
        .freeKey = freeKey,
        .freeVal = freeVal,
    };

    fn batchAdd(dict: *HashMap, cnt: usize) void {
        for (0..cnt) |i| {
            const key = std.fmt.allocPrint(
                allocator.child,
                "key-{}",
                .{i},
            ) catch unreachable;
            defer allocator.free(key);

            const val = std.fmt.allocPrint(
                allocator.child,
                "val-{}",
                .{i},
            ) catch unreachable;
            defer allocator.free(val);
            _ = dict.add(
                sds.new(key),
                sds.new(val),
            );
        }
    }

    fn eql(key: sds.String, other: sds.String) bool {
        return std.mem.eql(
            u8,
            sds.asBytes(key),
            sds.asBytes(other),
        );
    }

    fn hash(key: sds.String) Hash {
        return genHash(sds.asBytes(key));
    }

    fn freeKey(key: sds.String) void {
        sds.free(key);
    }

    fn freeVal(val: sds.String) void {
        sds.free(val);
    }
};

const std = @import("std");
const allocator = @import("allocator.zig");
const testing = std.testing;
const rand = @import("random.zig");
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const expect = testing.expect;
const sds = @import("sds.zig");
const assert = std.debug.assert;
