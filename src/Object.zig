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
};

type: Type,
encoding: Encoding,
// lru: std.meta.Int(.unsigned, server.LRU_BITS),
refcount: int,
ptr: *anyopaque,

pub const Object = @This();

pub fn create(
    allocator: Allocator,
    typ: Type,
    ptr: *anyopaque,
) Allocator.Error!*Object {
    const obj = try allocator.create(Object);
    obj.type = typ;
    obj.encoding = .raw;
    obj.ptr = ptr;
    obj.refcount = 1;

    // TODO: lru
    return obj;
}

pub const SHARED_REFCOUNT = maxInt(int);
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
    obj.ptr = s;

    return obj;
}

/// Always demanding to create a shared object if possible.
pub fn createStringFromLonglong(
    allocator: Allocator,
    value: longlong,
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
    value: longlong,
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
    value: longlong,
    from_shared: bool,
) Allocator.Error!*Object {
    var enabled = from_shared;
    if (server.instance.maxmemory == 0 or
        (server.instance.maxmemory_policy &
            server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0)
    {
        // If the maxmemory policy permits, we can still return shared integers
        // even if `from_shared` is false.
        enabled = true;
    }
    if (value >= 0 and value < server.SHARED_INTEGERS and enabled) {
        const o = server.shared.integers[@abs(value)];
        o.incrRefCount();
        return o;
    }

    if (value >= minInt(long) and value <= maxInt(long)) {
        const uv: usize = @bitCast(@as(isize, value));
        const ptr: *allowzero usize = @ptrFromInt(uv);
        const o = try create(allocator, .string, ptr);
        o.encoding = .int;
        return o;
    }

    const s = try sds.fromLonglong(allocator, value);
    errdefer sds.free(allocator, s);
    return try create(allocator, .string, s);
}

pub fn createStringFromLongDouble(
    allocator: Allocator,
    value: longdouble,
    humanfriendly: bool,
) Allocator.Error!*Object {
    var buf: [util.MAX_LONG_DOUBLE_CHARS]u8 = undefined;
    const str = util.ld2string(&buf, value, humanfriendly);
    return createString(allocator, str);
}

pub fn dupeString(
    allocator: Allocator,
    str: *const Object,
) Allocator.Error!*Object {
    assert(str.type == .string);

    switch (str.encoding) {
        .raw => {
            const s: sds.String = @ptrCast(str.ptr);
            return createRawString(allocator, sds.bufSlice(s));
        },
        .embstr => {
            const s: sds.String = @ptrCast(str.ptr);
            return createEmbeddedString(allocator, sds.bufSlice(s));
        },
        .int => {
            const o = try create(allocator, .string, str.ptr);
            o.encoding = .int;
            return o;
        },
        else => @panic("Wrong encoding."),
    }
}

fn freeString(self: *Object, allocator: Allocator) void {
    if (self.encoding == .raw) {
        const s: sds.String = @ptrCast(self.ptr);
        sds.free(allocator, s);
    }
}

fn freeList(self: *Object, allocator: Allocator) void {
    if (self.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(self.ptr));
        ql.release(allocator);
    } else {
        @panic("Unknown list encoding type");
    }
}

pub fn compareStrings(self: *Object, other: *Object) std.math.Order {
    assert(self.type == .string);
    assert(other.type == .string);

    if (self == other) return .eq;

    const a: sds.String = @ptrCast(self.ptr);
    const b: sds.String = @ptrCast(other.ptr);
    return sds.cmp(a, b);
}

pub fn equalStrings(self: *Object, other: *Object) bool {
    if (self.encoding == .int and other.encoding == .int) {
        // How does it store int in pointer address? Example:
        // self.ptr  = @ptrFrontInt(100);
        // other.ptr = @ptrFrontInt(100);
        return self.ptr == other.ptr;
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
    const d = try set.Dict.create(
        allocator,
        set.priv_data,
        set.priv_data.vtable(),
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

pub fn decrRefCount(self: *Object, allocator: Allocator) void {
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(allocator),
            .list => self.freeList(allocator),
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
        const s: sds.String = @ptrCast(self.ptr);
        return sds.getLen(s);
    }
    const sv: isize = @bitCast(@intFromPtr(self.ptr));
    const v: long = @truncate(sv);
    return util.sdigits10(v);
}

pub fn getDecoded(
    self: *Object,
    allocator: Allocator,
) Allocator.Error!*Object {
    if (self.sdsEncoded()) {
        self.incrRefCount();
        return self;
    }
    if (self.type == .string and self.encoding == .int) {
        const sv: isize = @bitCast(@intFromPtr(self.ptr));
        const v: long = @truncate(sv);
        var buf: [20]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable;
        return createString(allocator, digits);
    }
    @panic("Unknown encoding type");
}

pub fn sdsEncoded(self: *Object) bool {
    return self.encoding == .raw or self.encoding == .embstr;
}

fn free(self: *Object, allocator: Allocator) void {
    if (self.type == .string and self.encoding == .embstr) {
        const s: sds.String = @ptrCast(self.ptr);
        const mem_size = @sizeOf(Object) + sds.getAllocMemSize(s);
        const mem: [*]align(@alignOf(Object)) u8 = @ptrCast(@alignCast(self));
        allocator.free(mem[0..mem_size]);
        return;
    }
    allocator.destroy(self);
}

test createEmbeddedString {
    const allocator = std.testing.allocator;
    var o = try createEmbeddedString(allocator, "hello");
    defer o.decrRefCount(allocator);
    const s: sds.String = @ptrCast(o.ptr);
    try expectEqualStrings("hello", sds.bufSlice(s));
}

const std = @import("std");
const meta = std.meta;
const Allocator = std.mem.Allocator;
const sds = @import("sds.zig");
const assert = std.debug.assert;
const server = @import("server.zig");
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
const ctypes = @import("ctypes.zig");
const longlong = ctypes.longlong;
const int = ctypes.int;
const long = ctypes.long;
const longdouble = ctypes.longdouble;
const minInt = std.math.minInt;
const maxInt = std.math.maxInt;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const util = @import("util.zig");
const ZipList = @import("ZipList.zig");
const IntSet = @import("IntSet.zig");
const QuickList = @import("QuickList.zig");
const set = @import("t_set.zig");
const zset = @import("t_zset.zig");
const Zset = zset.Zset;
