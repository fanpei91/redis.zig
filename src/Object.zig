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
//lru: std.meta.Int(.unsigned, Server.LRU_BITS),
refcount: i32,
data: union {
    ptr: *anyopaque,
    int: i64,
},

pub fn create(
    allocator: Allocator,
    typ: Type,
    ptr: *anyopaque,
) Allocator.Error!*Object {
    const obj = try allocator.create(Object);
    obj.type = typ;
    obj.encoding = .raw;
    obj.data = .{ .ptr = ptr };
    obj.refcount = 1;

    // TODO: lru
    return obj;
}

pub fn createInt(
    allocator: Allocator,
    int: i64,
) Allocator.Error!*Object {
    const obj = try allocator.create(Object);
    obj.type = .string;
    obj.encoding = .int;
    obj.data = .{ .int = int };
    obj.refcount = 1;

    // TODO: lru
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
pub fn createString(
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*Object {
    if (str.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        return createEmbeddedString(allocator, str);
    }
    return createRawString(allocator, str);
}

pub fn createRawString(
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*Object {
    const s = try sds.new(allocator, str);
    errdefer sds.free(allocator, s);
    return try create(allocator, .string, s);
}

fn createEmbeddedString(
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*Object {
    const mem_size = @sizeOf(Object) + @sizeOf(sds.Hdr8) + str.len;
    const mem = try allocator.alignedAlloc(u8, .of(Object), mem_size);

    const sh: *sds.Hdr8 = @ptrFromInt(@intFromPtr(mem.ptr) + @sizeOf(Object));
    sh.len = @intCast(str.len);
    sh.alloc = @intCast(str.len);
    sh.flags = sds.TYPE_8;
    const s: sds.String = @ptrFromInt(@intFromPtr(sh) + @sizeOf(sds.Hdr8));
    memcpy(s, str, str.len);

    const obj: *Object = @ptrCast(@alignCast(mem));
    obj.type = .string;
    obj.encoding = .embstr;
    obj.refcount = 1;
    // TODO: obj.lru
    obj.data = .{ .ptr = s };

    return obj;
}

/// Always demanding to create a shared object if possible.
pub fn createStringFromLonglong(
    allocator: Allocator,
    value: i64,
) Allocator.Error!*Object {
    return createStringFromLonglongWithOptions(
        allocator,
        value,
        true,
    );
}

/// Avoiding a shared object when LFU/LRU info are needed, that is, when the
/// object is used as a value in the key space, and Redis is configured to evict
/// based on LFU/LRU.
pub fn createStringFromLonglongForValue(
    allocator: Allocator,
    value: i64,
) Allocator.Error!*Object {
    return createStringFromLonglongWithOptions(
        allocator,
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
fn createStringFromLonglongWithOptions(
    allocator: Allocator,
    value: i64,
    from_shared: bool,
) Allocator.Error!*Object {
    var enabled = from_shared;
    if (Server.instance.maxmemory == 0 or
        (Server.instance.maxmemory_policy &
            Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0)
    {
        // If the maxmemory policy permits, we can still return shared integers
        // even if `from_shared` is false.
        enabled = true;
    }
    if (value >= 0 and value < Server.SHARED_INTEGERS and enabled) {
        const o = Server.shared.integers[@abs(value)];
        o.incrRefCount();
        return o;
    }

    if (value >= minInt(i64) and value <= maxInt(i64)) {
        const o = try createInt(allocator, value);
        return o;
    }

    const s = try sds.fromLonglong(allocator, value);
    errdefer sds.free(allocator, s);
    return try create(allocator, .string, s);
}

pub fn createStringFromLongDouble(
    allocator: Allocator,
    value: f80,
    humanfriendly: bool,
) Allocator.Error!*Object {
    var buf: [util.MAX_LONG_DOUBLE_CHARS]u8 = undefined;
    const str = util.ld2string(&buf, value, humanfriendly);
    return createString(allocator, str);
}

pub fn dupeString(
    self: *const Object,
    allocator: Allocator,
) Allocator.Error!*Object {
    assert(self.type == .string);

    switch (self.encoding) {
        .raw => {
            return createRawString(
                allocator,
                sds.asBytes(sds.cast(self.data.ptr)),
            );
        },
        .embstr => {
            return createEmbeddedString(
                allocator,
                sds.asBytes(sds.cast(self.data.ptr)),
            );
        },
        .int => {
            const o = try createInt(allocator, self.data.int);
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
            self.data.int,
            other.data.int,
        );
    }

    assert(self.encoding != .int);
    assert(other.encoding != .int);

    const a: sds.String = @ptrCast(self.data.ptr);
    const b: sds.String = @ptrCast(other.data.ptr);
    return sds.cmp(a, b);
}

pub fn equalStrings(self: *Object, other: *Object) bool {
    if (self.encoding == .int and other.encoding == .int) {
        return self.data.int == other.data.int;
    }
    return self.compareStrings(other) == .eq;
}

pub fn createQuickList(allocator: Allocator) Allocator.Error!*Object {
    const ql = try QuickList.create(allocator);
    errdefer ql.release(allocator);
    const obj = try create(allocator, .list, ql);
    obj.encoding = .quicklist;
    return obj;
}

pub fn createZipList(allocator: Allocator) Allocator.Error!*Object {
    const zl = try ZipList.new(allocator);
    errdefer zl.free(allocator);
    const obj = try create(allocator, .list, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createIntSet(allocator: Allocator) Allocator.Error!*Object {
    const is = try IntSet.new(allocator);
    errdefer is.free(allocator);
    const obj = try create(allocator, .set, is);
    obj.encoding = .intset;
    return obj;
}

pub fn createSet(allocator: Allocator) Allocator.Error!*Object {
    const d = try Dict.create(
        allocator,
        Server.setDictVTable,
        null,
    );
    errdefer d.destroy(allocator);
    const obj = try create(allocator, .set, d);
    obj.encoding = .ht;
    return obj;
}

pub fn createHash(allocator: Allocator) Allocator.Error!*Object {
    const zl = try ZipList.new(allocator);
    errdefer zl.free(allocator);
    const obj = try create(allocator, .hash, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createZset(allocator: Allocator) Allocator.Error!*Object {
    const zs = try Zset.create(allocator);
    errdefer zs.destroy(allocator);
    const obj = try create(allocator, .zset, zs);
    obj.encoding = .skiplist;
    return obj;
}

pub fn createZsetZipList(allocator: Allocator) Allocator.Error!*Object {
    const zl = try ZipList.new(allocator);
    errdefer zl.free(allocator);
    const obj = try create(allocator, .zset, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn decrRefCount(self: *Object, allocator: Allocator) void {
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(allocator),
            .list => self.freeList(allocator),
            .set => self.freeSet(allocator),
            .zset => self.freeZset(allocator),
            .hash => self.freeHash(allocator),
            else => unreachable, // TODO: complete all branch
        }
        self.free(allocator);
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
        return sds.getLen(sds.cast(self.data.ptr));
    }
    return util.sdigits10(self.data.int);
}

pub fn isSdsRepresentableAsLongLong(s: sds.String, llval: *i64) bool {
    return util.string2ll(sds.asBytes(s), llval);
}

/// Get a decoded version of an encoded object (returned as a new object).
/// If the object is already raw-encoded just increment the ref count.
pub fn getDecoded(
    self: *Object,
    allocator: Allocator,
) Allocator.Error!*Object {
    if (self.sdsEncoded()) {
        self.incrRefCount();
        return self;
    }
    if (self.type == .string and self.encoding == .int) {
        var buf: [20]u8 = undefined;
        const digits = util.ll2string(&buf, self.data.int);
        return createString(allocator, digits);
    }
    @panic("Unknown encoding type");
}

/// Try to encode a string object in order to save space.
pub fn tryEncoding(
    self: *Object,
    allocator: Allocator,
) Allocator.Error!*Object {
    assert(self.type == .string);

    if (!self.sdsEncoded()) return self;
    if (self.refcount > 1) return self;

    const slice = sds.asBytes(sds.cast(self.data.ptr));
    var value: i64 = undefined;
    if (slice.len <= 20 and util.string2l(slice, &value)) {
        const use_shared_integers = (Server.instance.maxmemory == 0 or
            (Server.instance.maxmemory_policy & Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0);
        if (use_shared_integers and value > 0 and value < Server.SHARED_INTEGERS) {
            self.decrRefCount(allocator);
            const obj = Server.shared.integers[@as(usize, @intCast(value))];
            obj.incrRefCount();
            return obj;
        } else {
            if (self.encoding == .raw) {
                sds.free(allocator, sds.cast(self.data.ptr));
                self.encoding = .int;
                self.data = .{ .int = value };
                return self;
            } else if (self.encoding == .embstr) {
                self.decrRefCount(allocator);
                return createStringFromLonglongForValue(allocator, value);
            }
        }
    }
    // If the string is small and is still RAW encoded,
    // try the EMBSTR encoding which is more efficient.
    // In this representation the object and the SDS string are allocated
    // in the same chunk of memory to save space and cache misses.
    if (slice.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        if (self.encoding == .embstr) return self;
        const emb = try createEmbeddedString(allocator, slice);
        self.decrRefCount(allocator);
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
    try self.trimStringIfNeeded(allocator);

    return self;
}

/// Optimize the SDS string inside the string object to require little space,
/// in case there is more than 10% of free space at the end of the SDS
/// string. This happens because SDS strings tend to overallocate to avoid
/// wasting too much time in allocations when appending to the string.
pub fn trimStringIfNeeded(
    self: *Object,
    allocator: Allocator,
) Allocator.Error!void {
    if (self.encoding != .raw) {
        return;
    }
    var s = sds.cast(self.data.ptr);
    if (sds.getAvail(s) > sds.getLen(s) / 10) {
        s = try sds.removeAvailSpace(allocator, s);
        self.data = .{ .ptr = s };
    }
}

pub fn sdsEncoded(self: *const Object) bool {
    return self.encoding == .raw or self.encoding == .embstr;
}

pub fn getLongLong(self: *const Object, llval: *i64) bool {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return util.string2ll(
            sds.asBytes(sds.cast(self.data.ptr)),
            llval,
        );
    }
    if (self.encoding == .int) {
        llval.* = self.data.int;
        return true;
    }
    @panic("Unknown string encoding");
}

pub fn getLongLongOrReply(
    self: *const Object,
    cli: *Client,
    target: *i64,
    msg: ?[]const u8,
) Allocator.Error!bool {
    if (!self.getLongLong(target)) {
        if (msg) |m| {
            try cli.addReplyErr(m);
        } else {
            try cli.addReplyErr("value is not an integer or out of range");
        }
        return false;
    }
    return true;
}

pub fn strEncoding(self: *Object) []const u8 {
    return self.encoding.toString();
}

pub fn free(self: *Object, allocator: Allocator) void {
    if (self.type == .string and self.encoding == .embstr) {
        const s: sds.String = @ptrCast(self.data.ptr);
        const mem_size = @sizeOf(Object) + sds.getAllocMemSize(s);
        const mem: [*]align(@alignOf(Object)) u8 = @ptrCast(@alignCast(self));
        allocator.free(mem[0..mem_size]);
        return;
    }
    allocator.destroy(self);
}

fn freeString(self: *Object, allocator: Allocator) void {
    if (self.encoding == .raw) {
        const s: sds.String = @ptrCast(self.data.ptr);
        sds.free(allocator, s);
    }
}

fn freeList(self: *Object, allocator: Allocator) void {
    if (self.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(self.data.ptr));
        ql.release(allocator);
    } else {
        @panic("Unknown list encoding type");
    }
}

fn freeSet(self: *Object, allocator: Allocator) void {
    switch (self.encoding) {
        .intset => {
            const is: *IntSet = @ptrCast(@alignCast(self.data.ptr));
            is.free(allocator);
        },
        .ht => {
            const d: *Dict = @ptrCast(@alignCast(self.data.ptr));
            d.destroy(allocator);
        },
        else => @panic("Unknown set encoding type"),
    }
}

fn freeZset(self: *Object, allocator: Allocator) void {
    switch (self.encoding) {
        .skiplist => {
            const zl: *Zset = @ptrCast(@alignCast(self.data.ptr));
            zl.destroy(allocator);
        },
        .ziplist => {
            const zl: *ZipList = ZipList.cast(self.data.ptr);
            zl.free(allocator);
        },
        else => @panic("Unknown sorted set encoding type"),
    }
}

fn freeHash(self: *Object, allocator: Allocator) void {
    switch (self.encoding) {
        .ht => {
            const d: *Dict = @ptrCast(@alignCast(self.data.ptr));
            d.destroy(allocator);
        },
        .ziplist => {
            const zl: *ZipList = ZipList.cast(self.data.ptr);
            zl.free(allocator);
        },
        else => @panic("Unknown hash encoding type"),
    }
}

test createEmbeddedString {
    const allocator = std.testing.allocator;
    var o = try createEmbeddedString(allocator, "hello");
    defer o.decrRefCount(allocator);
    const s: sds.String = @ptrCast(o.data.ptr);
    try expectEqualStrings("hello", sds.asBytes(s));
}

test isSdsRepresentableAsLongLong {
    var llval: i64 = undefined;
    const allocator = std.testing.allocator;
    var s = try sds.new(allocator, "123456789");
    defer sds.free(allocator, s);

    var ok = isSdsRepresentableAsLongLong(s, &llval);
    try expect(ok);
    try expectEqual(123456789, llval);

    sds.clear(s);
    s = try sds.cat(allocator, s, "abc1");
    ok = isSdsRepresentableAsLongLong(s, &llval);
    try expect(!ok);
}

const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;
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
const Dict = @import("Dict.zig");
const zset = @import("t_zset.zig");
const Zset = zset.Zset;
const Client = @import("networking.zig").Client;
