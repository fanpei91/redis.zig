pub const PrivData = ?*anyopaque;
pub const Key = *anyopaque;
pub const Value = ?*anyopaque;
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

pub const Vtable = struct {
    hash: *const fn (priv_data: PrivData, key: Key) Hash,
    eql: ?*const fn (priv_data: PrivData, key1: Key, key2: Key) bool,
    dupeKey: ?*const fn (priv_data: PrivData, Allocator, key: Key) Allocator.Error!Key,
    dupeVal: ?*const fn (priv_data: PrivData, Allocator, val: Value) Allocator.Error!Value,
    freeKey: ?*const fn (priv_data: PrivData, Allocator, key: Key) void,
    freeVal: ?*const fn (priv_data: PrivData, Allocator, val: Value) void,
};

pub const Entry = struct {
    key: Key,
    v: union {
        val: Value,
        u64: u64,
        s64: i64,
        d: double,
    },
    next: ?*Entry = null,

    fn destroy(
        self: *Entry,
        allocator: Allocator,
        ctx: *Context,
    ) void {
        ctx.freeKey(allocator, self.key);
        ctx.freeVal(allocator, self.v.val);
        allocator.destroy(self);
    }
};

pub const Iterator = struct {
    dict: *Dict,
    index: long,
    table: u8,
    safe: bool,
    entry: ?*Entry,
    next_entry: ?*Entry,
    fingerprint: longlong,

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
                std.debug.assert(self.fingerprint == self.dict.fingerprint());
            }
        }
    }
};

const Context = struct {
    priv_data: PrivData,
    vtable: *const Vtable,

    fn eql(self: *Context, key1: Key, key2: Key) bool {
        if (self.vtable.eql) |cmp| {
            return cmp(self.priv_data, key1, key2);
        }
        return std.meta.eql(key1, key2);
    }

    fn hash(self: *Context, key: Key) Hash {
        return self.vtable.hash(self.priv_data, key);
    }

    fn dupeKey(
        self: *Context,
        allocator: Allocator,
        key: Key,
    ) Allocator.Error!Key {
        if (self.vtable.dupeKey) |dupe| {
            return dupe(self.priv_data, allocator, key);
        }
        return key;
    }

    fn dupeVal(
        self: *Context,
        allocator: Allocator,
        val: Value,
    ) Allocator.Error!Value {
        if (self.vtable.dupeVal) |dupe| {
            return dupe(self.priv_data, allocator, val);
        }
        return val;
    }

    fn freeKey(self: *Context, allocator: Allocator, key: Key) void {
        if (self.vtable.freeKey) |free| {
            free(self.priv_data, allocator, key);
        }
    }

    fn freeVal(self: *Context, allocator: Allocator, val: Value) void {
        if (self.vtable.freeVal) |free| {
            free(self.priv_data, allocator, val);
        }
    }
};

const HashTable = struct {
    size: ulong = 0, // Must be power of two。
    used: ulong = 0,
    sizemask: ulong = 0, // size - 1
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
        callback: ?*const fn (priv_data: PrivData) void,
    ) void {
        var i: usize = 0;
        while (i < self.size and self.used > 0) : (i += 1) {
            // Give the caller some opportunity to do other tasks.
            if ((i & 65535 == 0)) {
                if (callback) |cb| {
                    cb(ctx.priv_data);
                }
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

const Dict = @This();
ctx: Context,
ht: [2]HashTable = .{ .{}, .{} },
rehashidx: long = -1,
iterators: ulong = 0,

pub fn iterator(self: *Dict, safe: bool) Iterator {
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
    vtable: *const Vtable,
    priv_data: PrivData,
) Allocator.Error!*Dict {
    const self = try allocator.create(Dict);
    self.* = .{
        .ctx = .{ .priv_data = priv_data, .vtable = vtable },
    };
    return self;
}

/// Add an element, discarding the old value if the key already exists.
/// Return `.add` if the key was added from scratch, `.replace` if
/// there was already an element with such key and replace() just
/// performed a value update operation.
pub fn replace(
    self: *Dict,
    allocator: Allocator,
    key: Key,
    val: Value,
) Allocator.Error!enum { replace, add } {
    var existing: ?*Entry = null;
    const entry = try self.addRaw(allocator, key, &existing);
    if (entry) |ent| {
        errdefer {
            self.freeKey(allocator, ent);
            allocator.destroy(ent);
        }
        try self.setVal(allocator, ent, val);
        return .add;
    }
    self.freeVal(allocator, existing.?);
    try self.setVal(allocator, existing.?, val);
    return .replace;
}

pub fn add(
    self: *Dict,
    allocator: Allocator,
    key: Key,
    val: Value,
) Allocator.Error!bool {
    const entry = try self.addRaw(allocator, key, null);
    if (entry) |ent| {
        errdefer {
            self.freeKey(allocator, ent);
            allocator.destroy(ent);
        }
        try self.setVal(allocator, ent, val);
        return true;
    }
    return false;
}

/// If key already exists null is returned, and `existing.*` is populated
/// with the existing entry if existing is not null.
///
/// If key was added, the hash entry is returned to be manipulated by
/// the caller.
pub fn addRaw(
    self: *Dict,
    allocator: Allocator,
    key: Key,
    existing: ?*?*Entry,
) Allocator.Error!?*Entry {
    if (self.isRehashing()) self.rehashStep(allocator);
    if (try self.expandIfNeeded(allocator) == false) {
        return null;
    }

    const index = self.keyIndex(key, existing) orelse return null;
    var ht: *HashTable = if (self.isRehashing()) &self.ht[1] else &self.ht[0];

    const entry = try allocator.create(Entry);
    errdefer allocator.destroy(entry);

    try self.setKey(allocator, entry, key);
    entry.next = ht.get(index);

    ht.set(index, entry);
    ht.used += 1;
    return entry;
}

/// A version of `addRaw()` that always returns the hash entry of the
/// specified key, even if the key already exists and can't be
/// added (in that case the entry of the already existing key is returned.)
pub fn addOrFind(
    self: *Dict,
    allocator: Allocator,
    key: Key,
) Allocator.Error!*Entry {
    var existing: ?*Entry = null;
    const entry = try self.addRaw(allocator, key, &existing);
    if (entry) |ent| {
        return ent;
    }
    return existing.?;
}

pub fn find(
    self: *Dict,
    allocator: Allocator,
    key: Key,
) ?*Entry {
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

pub fn fetchValue(
    self: *Dict,
    allocator: Allocator,
    key: Key,
) Value {
    const entry = self.find(allocator, key) orelse return null;
    return entry.v.val;
}

pub fn getRandom(self: *Dict, allocator: Allocator) ?*Entry {
    if (self.size() == 0) return null;
    if (self.isRehashing()) self.rehashStep(allocator);

    const h0 = &self.ht[0];
    const h1 = &self.ht[1];
    var entry: ?*Entry = null;
    if (self.isRehashing()) {
        const rehashidx: ulong = @abs(self.rehashidx);
        while (entry == null) {
            const random = rand.int(ulong);
            const idx = rehashidx + (random % (h0.size + h1.size - rehashidx));
            entry = if (idx >= h0.size) h1.get(idx - h0.size) else h0.get(idx);
        }
    } else {
        while (entry == null) {
            const random = rand.int(ulong);
            const idx = random & h0.sizemask;
            entry = h0.get(idx);
        }
    }

    const head = entry.?;
    var list_len: ulong = 0;
    while (entry != null) {
        entry = entry.?.next;
        list_len += 1;
    }
    const random = rand.int(ulong);
    var list_ele = random % list_len;
    entry = head;
    while (list_ele > 0) : (list_ele -= 1) {
        entry = entry.?.next;
    }
    return entry;
}

pub fn getSome(
    self: *Dict,
    allocator: Allocator,
    n: uint,
) Allocator.Error![]*Entry {
    const count: ulong = @min(self.size(), n);

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
    const tables: u8 = if (self.isRehashing()) 2 else 1;
    var max_sizemask = h0.sizemask;
    if (tables > 1 and max_sizemask < h1.sizemask) {
        max_sizemask = h1.sizemask;
    }

    var i = rand.int(ulong) & max_sizemask;
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
                    i = rand.int(ulong) & max_sizemask;
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

pub const scanEntryFunc = *const fn (priv_data: PrivData, e: *const Entry) void;
pub const scanBucketFunc = *const fn (priv_data: PrivData, e: *?*Entry) void;

pub fn scan(
    self: *Dict,
    cursor: ulong,
    scanEntryFn: ?scanEntryFunc,
    scanBucketFn: ?scanBucketFunc,
    priv_data: PrivData,
) ulong {
    var v = cursor;
    var t0: *HashTable = &self.ht[0];
    var m0 = t0.sizemask;

    if (!self.isRehashing()) {
        const idx = v & m0;
        if (scanBucketFn) |scanBucket| {
            scanBucket(priv_data, &t0.table.?[idx]);
        }
        var entry = t0.get(idx);
        while (entry) |ent| {
            entry = ent.next;
            if (scanEntryFn) |scanEntry| {
                scanEntry(priv_data, ent);
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
            scanBucket(priv_data, &t0.table.?[idx]);
        }
        m0 = t0.sizemask;
        var entry = t0.get(idx);
        while (entry) |ent| {
            entry = ent.next;
            if (scanEntryFn) |scanEntry| {
                scanEntry(priv_data, ent);
            }
        }

        const m1 = t1.sizemask;
        while (true) {
            const i = v & m1;
            if (scanBucketFn) |scanBucket| {
                scanBucket(priv_data, &t1.table.?[i]);
            }
            entry = t1.get(i);
            while (entry) |ent| {
                entry = ent.next;
                if (scanEntryFn) |scanEntry| {
                    scanEntry(priv_data, ent);
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

pub fn delete(self: *Dict, allocator: Allocator, key: Key) bool {
    const deleted = self.genericDelete(allocator, key, true);
    return deleted != null;
}

pub fn unlink(
    self: *Dict,
    allocator: Allocator,
    key: Key,
) ?*Entry {
    return self.genericDelete(allocator, key, false);
}

pub fn freeUnlinkedEntry(
    self: *Dict,
    allocator: Allocator,
    entry: *Entry,
) void {
    entry.destroy(allocator, &self.ctx);
}

fn genericDelete(
    self: *Dict,
    allocator: Allocator,
    key: Key,
    free: bool,
) ?*Entry {
    if (self.ht[0].used == 0 and self.ht[1].used == 0) return null;
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
                if (free) ent.destroy(allocator, &self.ctx);
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

pub fn isRehashing(self: *Dict) bool {
    return self.rehashidx != -1;
}

pub fn expand(
    self: *Dict,
    allocator: Allocator,
    len: ulong,
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
pub fn rehashMilliseconds(self: *Dict, ms: usize) void {
    const start = std.time.milliTimestamp();
    const n = 100;
    while (self.rehash(n)) {
        const end = std.time.milliTimestamp();
        if (end - start > ms) break;
    }
}

pub fn empty(
    self: *Dict,
    allocator: Allocator,
    callback: ?*const fn (priv_data: PrivData) void,
) void {
    self.ht[0].free(allocator, &self.ctx, callback);
    self.ht[1].free(allocator, &self.ctx, callback);
    self.iterators = 0;
    self.rehashidx = -1;
}

pub fn size(self: *Dict) ulong {
    return self.ht[0].used + self.ht[1].used;
}

pub fn slots(self: *Dict) ulong {
    return self.ht[0].size + self.ht[1].size;
}

pub fn setKey(
    self: *Dict,
    allocator: Allocator,
    entry: *Entry,
    key: Key,
) Allocator.Error!void {
    entry.key = try self.ctx.dupeKey(allocator, key);
}

pub fn freeKey(
    self: *Dict,
    allocator: Allocator,
    entry: *Entry,
) void {
    self.ctx.freeKey(allocator, entry.key);
}

pub fn setVal(
    self: *Dict,
    allocator: Allocator,
    entry: *Entry,
    val: Value,
) Allocator.Error!void {
    entry.v = .{
        .val = try self.ctx.dupeVal(allocator, val),
    };
}

pub fn freeVal(
    self: *Dict,
    allocator: Allocator,
    entry: *Entry,
) void {
    self.ctx.freeVal(allocator, entry.v.val);
}

pub fn destroy(self: *Dict, allocator: Allocator) void {
    self.ht[0].free(allocator, &self.ctx, null);
    self.ht[1].free(allocator, &self.ctx, null);
    allocator.destroy(self);
}

/// Return the index where the key should be inserted, or return null
/// if `key` exists, and the optional output parameter may be filled.
fn keyIndex(self: *Dict, key: Key, existing: ?*?*Entry) ?ulong {
    if (existing) |exit| exit.* = null;

    const hash: Hash = self.ctx.hash(key);
    var idx: ?ulong = null;
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

fn expandIfNeeded(
    self: *Dict,
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

fn rehashStep(self: *Dict, allocator: Allocator) void {
    if (self.iterators != 0) return;
    _ = self.rehash(allocator, 1);
}

/// More to rehash if returning true.
fn rehash(self: *Dict, allocator: Allocator, n: usize) bool {
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
        h0.freeTable(allocator);
        h0.* = h1.*;
        h1.reset();
        self.rehashidx = -1;
        return false;
    }

    return true;
}

fn fingerprint(self: *Dict) longlong {
    const h0 = self.ht[0];
    const h1 = self.ht[1];

    var intergers: [6]longlong = undefined;

    intergers[0] = if (h0.table == null) 0 else @bitCast(@intFromPtr(h0.table));
    intergers[1] = @bitCast(h0.size);
    intergers[2] = @bitCast(h0.used);

    intergers[3] = if (h1.table == null) 0 else @bitCast(@intFromPtr(h1.table));
    intergers[4] = @bitCast(h1.size);
    intergers[5] = @bitCast(h1.used);

    var hash: longlong = 0;
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

fn nextPower(sz: ulong) ulong {
    var i: ulong = HT_INITIAL_SIZE;
    const max = std.math.maxInt(long);
    if (sz >= max) return max + 1;
    while (true) {
        if (i >= sz) {
            return i;
        }
        i *= 2;
    }
}

test "add() | addOrFind() || find() | fetchVal()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    const cnt = 100;
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

        const skey = try sds.new(allocator, key);
        const sval = try sds.new(allocator, val);
        var added = try dict.add(
            allocator,
            skey,
            sval,
        );
        try expect(added);

        added = try dict.add(
            allocator,
            skey,
            sval,
        );
        try expect(!added);
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

        const skey = try sds.new(allocator, key);
        defer sds.free(allocator, skey);
        const sval = try sds.new(allocator, val);
        defer sds.free(allocator, sval);

        const found = dict.find(allocator, skey);
        try expect(found != null);
        try expectEqualStrings(
            sds.bufSlice(skey),
            sds.bufSlice(sds.cast(found.?.key)),
        );
        try expectEqualStrings(
            sds.bufSlice(sval),
            sds.bufSlice(sds.cast(found.?.v.val.?)),
        );

        const fetched_val = dict.fetchValue(allocator, skey);
        try expectEqualStrings(
            sds.bufSlice(sval),
            sds.bufSlice(sds.cast(fetched_val.?)),
        );
    }

    try expectEqual(cnt, dict.size());

    const key = try sds.new(allocator, "addOrFind");
    const new_entry = try dict.addOrFind(allocator, key);
    const val = try sds.new(allocator, "value");
    try dict.setVal(allocator, new_entry, val);
    const existing_entry = try dict.addOrFind(allocator, key);
    try expectEqual(new_entry, existing_entry);
    try expectEqualStrings(
        sds.bufSlice(val),
        sds.bufSlice(sds.cast(existing_entry.v.val.?)),
    );
}

test "getRandom()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    try expect(dict.getRandom(allocator) == null);

    try TestSdsVtable.batchAdd(dict, allocator, 100);

    const entry = dict.getRandom(allocator);
    try expect(entry != null);
}

test "getSome()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    try TestSdsVtable.batchAdd(dict, allocator, 100);

    const items = try dict.getSome(allocator, 10);
    defer allocator.free(items);
    try expectEqual(10, items.len);
}

test "replace()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    const key = try sds.new(allocator, "k1");
    _ = try dict.add(allocator, key, try sds.new(allocator, "v1"));
    const ret = try dict.replace(allocator, key, try sds.new(allocator, "v2"));
    try expectEqual(.replace, ret);

    const found = dict.find(allocator, key);
    try expectEqualStrings("v2", sds.bufSlice(sds.cast(found.?.v.val.?)));
}

test "delete()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    const key = try sds.new(allocator, "k1");
    _ = try dict.add(allocator, key, try sds.new(allocator, "v1"));
    const deleted = dict.delete(allocator, key);

    try expect(deleted);
    try expectEqual(0, dict.size());
}

test "unlink() | freeUnlinkedEntry()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    const key = try sds.new(allocator, "key1");
    _ = try dict.add(allocator, key, try sds.new(allocator, "val2"));
    const unlinked = dict.unlink(allocator, key);
    try expect(unlinked != null);
    try expectEqual(0, dict.size());
    dict.freeUnlinkedEntry(allocator, unlinked.?);
}

test "expand() | rehash()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    var expanded = try dict.expand(allocator, 1023);
    try expect(expanded);
    try expectEqual(1024, dict.ht[0].size);
    try expectEqual(0, dict.ht[1].size);

    try TestSdsVtable.batchAdd(dict, allocator, 1);

    expanded = try dict.expand(allocator, 2045);
    try expect(expanded);
    try expect(dict.isRehashing());
    try expectEqual(1024, dict.ht[0].size);
    try expectEqual(2048, dict.ht[1].size);

    const more = dict.rehash(allocator, 1024);
    try expect(more == false);
    try expectEqual(2048, dict.ht[0].size);
    try expectEqual(1, dict.ht[0].used);
    try expectEqual(0, dict.ht[1].size);
    try expectEqual(0, dict.ht[1].used);
    try expectEqual(1, dict.size());
}

test "iterator() | fingerprint()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    const count = 10;
    try TestSdsVtable.batchAdd(dict, allocator, count);

    var iter = dict.iterator(false);

    const fp = dict.fingerprint();
    var iters: usize = 0;
    while (iter.next()) |_| {
        iters += 1;
    }
    try expectEqual(count, iters);
    iter.release();

    const key = try sds.new(allocator, "nokey");
    defer sds.free(allocator, key);
    _ = dict.find(allocator, key);
    try expect(fp != dict.fingerprint());
}

test "scan()" {
    const allocator = testing.allocator;
    const dict = try Dict.create(
        allocator,
        TestSdsVtable.vtable,
        null,
    );
    defer dict.destroy(allocator);

    try TestSdsVtable.batchAdd(dict, allocator, 3);

    _ = try dict.expand(allocator, 8);

    var times: usize = 0;
    var cursor: usize = 0;
    while (true) {
        cursor = dict.scan(cursor, null, null, null);
        times += 1;
        if (cursor == 0) break;
    }
    try expect(times != 0);
}

const TestSdsVtable = struct {
    const Self = @This();

    const vtable: *const Vtable = &.{
        .hash = hash,
        .eql = eql,
        .dupeKey = null,
        .dupeVal = null,
        .freeKey = releaseKey,
        .freeVal = releaseVal,
    };

    fn batchAdd(
        dict: *Dict,
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
            _ = try dict.add(
                allocator,
                try sds.new(allocator, key),
                try sds.new(allocator, val),
            );
        }
    }

    fn eql(_: PrivData, key: Key, other: Key) bool {
        return std.mem.eql(
            u8,
            sds.bufSlice(sds.cast(key)),
            sds.bufSlice(sds.cast(other)),
        );
    }

    fn hash(_: PrivData, key: Key) Hash {
        return genHash(sds.bufSlice(sds.cast(key)));
    }

    fn releaseKey(_: PrivData, allocator: Allocator, key: Key) void {
        sds.free(allocator, sds.cast(key));
    }

    fn releaseVal(_: PrivData, allocator: Allocator, val: Value) void {
        sds.free(allocator, sds.cast(val.?));
    }
};

const std = @import("std");
const Allocator = std.mem.Allocator;
const testing = std.testing;
const rand = @import("random.zig");
const ctypes = @import("ctypes.zig");
const long = ctypes.long;
const int = ctypes.int;
const longlong = ctypes.longlong;
const ulong = ctypes.ulong;
const uint = ctypes.uint;
const double = ctypes.double;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const expect = testing.expect;
const sds = @import("sds.zig");
