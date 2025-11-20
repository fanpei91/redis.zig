pub const Type = enum(u4) {
    string = 0,
    list = 1,
    set = 2,
    zset = 3,
    hash = 4,
    module = 5,
    stream = 6,
};

pub const Encoding = enum(u4) {
    raw = 0,
    int = 1,
    ht = 2,
    zipmap = 3,
    linkedlist = 4, // No longer used: old list encoding.
    ziplist = 5,
    intset = 6,
    skiplist = 7,
    embstr = 8,
    quicklist = 9,
    stream = 10,

    pub fn toString(self: Encoding) []const u8 {
        return switch (self) {
            .raw => "raw",
            .int => "int",
            .ht => "hashtable",
            .ziplist => "ziplist",
            .intset => "intset",
            .skiplist => "skiplist",
            .embstr => "embstr",
            .quicklist => "quicklist",
            else => "unknown",
        };
    }
};

const Object = @This();
type: Type,
encoding: Encoding,
lru: std.meta.Int(.unsigned, Server.LRU_BITS),
refcount: i32,
v: union {
    ptr: *anyopaque,
    int: i64,
},

pub fn create(typ: Type, ptr: *anyopaque) *Object {
    const obj = allocator.create(Object);
    obj.type = typ;
    obj.encoding = .raw;
    // Set the LRU to the current lruclock (minutes resolution), or
    // alternatively the LFU counter.
    if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
        obj.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | evict.LFU_INIT_VAL);
    } else {
        obj.lru = @intCast(evict.LRUClock());
    }
    obj.refcount = 1;
    obj.v = .{ .ptr = ptr };

    return obj;
}

pub fn createInt(int: i64) *Object {
    const obj = create(.string, @ptrFromInt(1));
    obj.encoding = .int;
    obj.v = .{ .int = int };
    return obj;
}

pub const SHARED_REFCOUNT = maxInt(i32);
/// Set a special refcount in the object to make it "shared":
/// incrRefCount() and decrRefCount() will test for this special refcount
/// and will not touch the object. This way it is free to access shared
/// objects such as small integers from different threads without any
/// mutex.
///
/// A common patter to create shared objects:
///
///  myobject: *Object = makeShared(create(...));
pub fn makeShared(self: *Object) *Object {
    self.refcount = SHARED_REFCOUNT;
    return self;
}

// TODO: This needs to be adjusted according to the allocator used.
// For now, just use the 44 that Redis has chosen for jemalloc.
const ENCODING_EMBSTR_SIZE_LIMIT = 44;
pub fn createString(str: []const u8) *Object {
    if (str.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        return createEmbeddedString(str);
    }
    return createRawString(str);
}

pub fn createRawString(str: []const u8) *Object {
    const s = sds.new(str);
    return create(.string, s);
}

fn createEmbeddedString(str: []const u8) *Object {
    const mem_size = @sizeOf(Object) + @sizeOf(sds.Hdr8) + str.len;
    const mem = allocator.alignedAlloc(u8, .of(Object), mem_size);

    const sh: *sds.Hdr8 = @ptrFromInt(@intFromPtr(mem.ptr) + @sizeOf(Object));
    sh.len = @intCast(str.len);
    sh.alloc = @intCast(str.len);
    sh.flags = sds.TYPE_8;
    const s: sds.String = @ptrFromInt(@intFromPtr(sh) + @sizeOf(sds.Hdr8));
    memcpy(s, str, str.len);

    const obj: *Object = @ptrCast(@alignCast(mem));
    obj.type = .string;
    obj.encoding = .embstr;
    if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
        obj.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | evict.LFU_INIT_VAL);
    } else {
        obj.lru = @intCast(evict.LRUClock());
    }
    obj.refcount = 1;
    obj.v = .{ .ptr = s };

    return obj;
}

/// Always demanding to create a shared object if possible.
pub fn createStringFromLonglong(value: i64) *Object {
    return createStringFromLonglongWithOptions(
        value,
        true,
    );
}

/// Avoiding a shared object when LFU/LRU info are needed, that is, when the
/// object is used as a value in the key space, and Redis is configured to evict
/// based on LFU/LRU.
pub fn createStringFromLonglongForValue(value: i64) *Object {
    return createStringFromLonglongWithOptions(
        value,
        false,
    );
}

/// Create a string object from a long long value. When possible returns a
/// shared integer object, or at least an integer encoded one.
///
/// If `from_shared` is false, the function avoids returning a a shared
/// integer, because the object is going to be used as value in the Redis key
/// space (for instance when the INCR command is used), so we want LFU/LRU
/// values specific for each key.
fn createStringFromLonglongWithOptions(value: i64, from_shared: bool) *Object {
    var enabled = from_shared;
    if (Server.instance.maxmemory == 0 or
        (Server.instance.maxmemory_policy &
            Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0)
    {
        // If the maxmemory policy permits, we can still return shared integers
        // even if `from_shared` is false.
        enabled = true;
    }
    if (value >= 0 and value < Server.OBJ_SHARED_INTEGERS and enabled) {
        const o = Server.shared.integers[@abs(value)];
        o.incrRefCount();
        return o;
    }

    if (value >= minInt(i64) and value <= maxInt(i64)) {
        const o = createInt(value);
        return o;
    }

    const s = sds.fromLonglong(value);
    return create(.string, s);
}

pub fn createStringFromLongDouble(value: f80, humanfriendly: bool) *Object {
    var buf: [util.MAX_LONG_DOUBLE_CHARS]u8 = undefined;
    const str = util.ld2string(&buf, value, humanfriendly);
    return createString(str);
}

pub fn dupeString(self: *const Object) *Object {
    assert(self.type == .string);

    switch (self.encoding) {
        .raw => {
            return createRawString(
                sds.asBytes(sds.cast(self.v.ptr)),
            );
        },
        .embstr => {
            return createEmbeddedString(
                sds.asBytes(sds.cast(self.v.ptr)),
            );
        },
        .int => {
            const o = createInt(self.v.int);
            return o;
        },
        else => @panic("Wrong encoding."),
    }
}

pub fn compareStrings(self: *Object, other: *Object) std.math.Order {
    assert(self.type == .string);
    assert(other.type == .string);

    if (self == other) return .eq;

    if (self.encoding == .int and other.encoding == .int) {
        return std.math.order(
            self.v.int,
            other.v.int,
        );
    }

    assert(self.encoding != .int);
    assert(other.encoding != .int);

    const a: sds.String = @ptrCast(self.v.ptr);
    const b: sds.String = @ptrCast(other.v.ptr);
    return sds.cmp(a, b);
}

pub fn equalStrings(self: *Object, other: *Object) bool {
    if (self.encoding == .int and other.encoding == .int) {
        return self.v.int == other.v.int;
    }
    return self.compareStrings(other) == .eq;
}

pub fn createQuickList() *Object {
    const ql = QuickList.create();
    const obj = create(.list, ql);
    obj.encoding = .quicklist;
    return obj;
}

pub fn createZipList() *Object {
    const zl = ZipList.new();
    const obj = create(.list, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createIntSet() *Object {
    const is = IntSet.new();
    const obj = create(.set, is);
    obj.encoding = .intset;
    return obj;
}

pub fn createSet() *Object {
    const d = set.create();
    const obj = create(.set, d);
    obj.encoding = .ht;
    return obj;
}

pub fn createHash() *Object {
    const zl = ZipList.new();
    const obj = create(.hash, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createZset() *Object {
    const zs = Zset.create();
    const obj = create(.zset, zs);
    obj.encoding = .skiplist;
    return obj;
}

pub fn createZsetZipList() *Object {
    const zl = ZipList.new();
    const obj = create(.zset, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn decrRefCount(self: *Object) void {
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(),
            .list => self.freeList(),
            .set => self.freeSet(),
            .zset => self.freeZset(),
            .hash => self.freeHash(),
            else => unreachable, // TODO: complete all branch
        }
        self.free();
        return;
    }
    if (self.refcount <= 0) @panic("Object.decrRefCount against refcount <= 0");
    if (self.refcount != SHARED_REFCOUNT) self.refcount -= 1;
}

pub fn incrRefCount(self: *Object) void {
    if (self.refcount != SHARED_REFCOUNT) self.refcount += 1;
}

pub fn resetRefCount(self: *Object) *Object {
    self.refcount = 0;
    return self;
}

pub fn stringLen(self: *Object) usize {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return sds.getLen(sds.cast(self.v.ptr));
    }
    return util.sdigits10(self.v.int);
}

pub fn isSdsRepresentableAsLongLong(s: sds.String, llval: *i64) bool {
    return util.string2ll(sds.asBytes(s), llval);
}

/// Get a decoded version of an encoded object (returned as a new object).
/// If the object is already raw-encoded just increment the ref count.
pub fn getDecoded(self: *Object) *Object {
    if (self.sdsEncoded()) {
        self.incrRefCount();
        return self;
    }
    if (self.type == .string and self.encoding == .int) {
        var buf: [20]u8 = undefined;
        const digits = util.ll2string(&buf, self.v.int);
        return createString(digits);
    }
    @panic("Unknown encoding type");
}

/// Try to encode a string object in order to save space.
pub fn tryEncoding(self: *Object) *Object {
    assert(self.type == .string);

    if (!self.sdsEncoded()) return self;
    if (self.refcount > 1) return self;

    const slice = sds.asBytes(sds.cast(self.v.ptr));
    var value: i64 = undefined;
    if (slice.len <= 20 and util.string2l(slice, &value)) {
        const use_shared_integers = (Server.instance.maxmemory == 0 or
            (Server.instance.maxmemory_policy & Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0);
        if (use_shared_integers and value > 0 and value < Server.OBJ_SHARED_INTEGERS) {
            self.decrRefCount();
            const obj = Server.shared.integers[@as(usize, @intCast(value))];
            obj.incrRefCount();
            return obj;
        } else {
            if (self.encoding == .raw) {
                sds.free(sds.cast(self.v.ptr));
                self.encoding = .int;
                self.v = .{ .int = value };
                return self;
            } else if (self.encoding == .embstr) {
                self.decrRefCount();
                return createStringFromLonglongForValue(value);
            }
        }
    }
    // If the string is small and is still RAW encoded,
    // try the EMBSTR encoding which is more efficient.
    // In this representation the object and the SDS string are allocated
    // in the same chunk of memory to save space and cache misses.
    if (slice.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        if (self.encoding == .embstr) return self;
        const emb = createEmbeddedString(slice);
        self.decrRefCount();
        return emb;
    }

    // We can't encode the object...
    //
    // Do the last try, and at least optimize the SDS string inside
    // the string object to require little space, in case there
    // is more than 10% of free space at the end of the SDS string.
    //
    // We do that only for relatively large strings as this branch
    // is only entered if the length of the string is greater than
    // ENCODING_EMBSTR_SIZE_LIMIT.
    self.trimStringIfNeeded();

    return self;
}

/// Optimize the SDS string inside the string object to require little space,
/// in case there is more than 10% of free space at the end of the SDS
/// string. This happens because SDS strings tend to overallocate to avoid
/// wasting too much time in allocations when appending to the string.
pub fn trimStringIfNeeded(self: *Object) void {
    if (self.encoding != .raw) {
        return;
    }
    var s = sds.cast(self.v.ptr);
    if (sds.getAvail(s) > sds.getLen(s) / 10) {
        s = sds.removeAvailSpace(s);
        self.v = .{ .ptr = s };
    }
}

pub fn getLongLongOrReply(
    self: *const Object,
    cli: *Client,
    target: *i64,
    msg: ?[]const u8,
) bool {
    if (!self.getLongLong(target)) {
        if (msg) |m| {
            cli.addReplyErr(m);
        } else {
            cli.addReplyErr("value is not an integer or out of range");
        }
        return false;
    }
    return true;
}

pub fn getLongLong(self: *const Object, llval: *i64) bool {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return util.string2ll(
            sds.asBytes(sds.cast(self.v.ptr)),
            llval,
        );
    }
    if (self.encoding == .int) {
        llval.* = self.v.int;
        return true;
    }
    @panic("Unknown string encoding");
}

pub fn sdsEncoded(self: *const Object) bool {
    return self.encoding == .raw or self.encoding == .embstr;
}

pub fn getLongLongFromObjectOrReply(
    self: *const Object,
    cli: *Client,
    target: *i64,
    msg: ?[]const u8,
) bool {
    if (self.getLongLongFromObject(target)) {
        return true;
    }
    if (msg) |err| {
        cli.addReplyErr(err);
    } else {
        cli.addReplyErr("value is not an integer or out of range");
    }
    return false;
}

pub fn getLongLongFromObject(self: *const Object, target: *i64) bool {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        target.* = std.fmt.parseInt(
            i64,
            sds.asBytes(sds.cast(self.v.ptr)),
            10,
        ) catch return false;
    } else if (self.encoding == .int) {
        target.* = self.v.int;
    } else {
        @panic("Unknown string encoding");
    }
    return true;
}

pub fn getLongDoubleFromObjectOrReply(
    self: *const Object,
    cli: *Client,
    target: *f80,
    msg: ?[]const u8,
) bool {
    if (self.getLongDoubleFromObject(target)) {
        return true;
    }
    if (msg) |err| {
        cli.addReplyErr(err);
    } else {
        cli.addReplyErr("value is not a valid float");
    }
    return false;
}

pub fn getLongDoubleFromObject(self: *const Object, target: *f80) bool {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        target.* = std.fmt.parseFloat(
            f80,
            sds.asBytes(sds.cast(self.v.ptr)),
        ) catch return false;
    } else if (self.encoding == .int) {
        target.* = @floatFromInt(self.v.int);
    } else {
        @panic("Unknown string encoding");
    }
    return true;
}

pub fn checkTypeOrReply(self: *const Object, cli: *Client, typ: Type) bool {
    if (self.type != typ) {
        cli.addReply(Server.shared.wrongtypeerr);
        return true;
    }
    return false;
}

pub fn strEncoding(self: *Object) []const u8 {
    return self.encoding.toString();
}

pub fn free(self: *Object) void {
    if (self.type == .string and self.encoding == .embstr) {
        const s: sds.String = @ptrCast(self.v.ptr);
        const mem_size = @sizeOf(Object) + sds.getAllocMemSize(s);
        const mem: [*]align(@alignOf(Object)) u8 = @ptrCast(@alignCast(self));
        allocator.free(mem[0..mem_size]);
        return;
    }
    allocator.destroy(self);
}

fn freeString(self: *Object) void {
    if (self.encoding == .raw) {
        const s: sds.String = @ptrCast(self.v.ptr);
        sds.free(s);
    }
}

fn freeList(self: *Object) void {
    if (self.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(self.v.ptr));
        ql.release();
    } else {
        @panic("Unknown list encoding type");
    }
}

fn freeSet(self: *Object) void {
    switch (self.encoding) {
        .intset => {
            const is: *IntSet = @ptrCast(@alignCast(self.v.ptr));
            is.free();
        },
        .ht => {
            const d: *set.Set.Dict = @ptrCast(@alignCast(self.v.ptr));
            d.destroy();
        },
        else => @panic("Unknown set encoding type"),
    }
}

fn freeZset(self: *Object) void {
    switch (self.encoding) {
        .skiplist => {
            const zl: *Zset = @ptrCast(@alignCast(self.v.ptr));
            zl.destroy();
        },
        .ziplist => {
            const zl: *ZipList = ZipList.cast(self.v.ptr);
            zl.free();
        },
        else => @panic("Unknown sorted set encoding type"),
    }
}

fn freeHash(self: *Object) void {
    switch (self.encoding) {
        .ht => @panic("Unimplemented"),
        .ziplist => {
            const zl: *ZipList = ZipList.cast(self.v.ptr);
            zl.free();
        },
        else => @panic("Unknown hash encoding type"),
    }
}

test createEmbeddedString {
    try Server.create(null, null);
    defer Server.destroy();

    var o = createEmbeddedString("hello");
    defer o.decrRefCount();
    const s: sds.String = @ptrCast(o.v.ptr);
    try expectEqualStrings("hello", sds.asBytes(s));
}

test isSdsRepresentableAsLongLong {
    var llval: i64 = undefined;

    var s = sds.new("123456789");
    defer sds.free(s);

    var ok = isSdsRepresentableAsLongLong(s, &llval);
    try expect(ok);
    try expectEqual(123456789, llval);

    sds.clear(s);
    s = sds.cat(s, "abc1");
    ok = isSdsRepresentableAsLongLong(s, &llval);
    try expect(!ok);
}

const std = @import("std");
const meta = std.meta;
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const assert = std.debug.assert;
const Server = @import("Server.zig");
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqual = std.testing.expectEqual;
const minInt = std.math.minInt;
const maxInt = std.math.maxInt;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const util = @import("util.zig");
const ZipList = @import("ZipList.zig");
const IntSet = @import("IntSet.zig");
const QuickList = @import("QuickList.zig");
const zset = @import("t_zset.zig");
const Zset = zset.Zset;
const Client = @import("networking.zig").Client;
const evict = @import("evict.zig");
const server = &Server.instance;
const dict = @import("dict.zig");
const set = @import("t_set.zig");
