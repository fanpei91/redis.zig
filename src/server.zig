pub const LRU_BITS = 24;
pub const LRU_CLOCK_MAX = ((1 << LRU_BITS) - 1); // Max value of obj->lru
pub const LRU_CLOCK_RESOLUTION = 1000; // LRU clock resolution in ms

pub const MAXMEMORY_FLAG_LRU = 1 << 0;
pub const MAXMEMORY_FLAG_LFU = 1 << 1;
pub const MAXMEMORY_FLAG_NO_SHARED_INTEGERS = MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU;

// Static server configuration
pub const SHARED_INTEGERS = 10000;
pub var shared: SharedObjects = undefined;

pub var instance: Server = undefined;

const SharedObjects = struct {
    integers: [SHARED_INTEGERS]*Object,
    minstring: sds.String,
    maxstring: sds.String,
};

/// Set dictionary type. Keys are SDS strings, values are ot used.
pub const setDictVtable: *const Dict.Vtable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = dictSdsFree,
    .freeVal = null,
};

/// Sorted sets hash (note: a skiplist is used in addition to the hash table)
pub const zsetDictVtable: *const Dict.Vtable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = null, //  sds.String shared & freed by skiplist
    .freeVal = null,
};

pub const Server = struct {
    maxmemory: u64, // Max number of memory bytes to use
    maxmemory_policy: i32, // Policy for key eviction
    maxmemory_samples: i32, // Pricision of random sampling
};

fn dictSdsHash(_: Dict.PrivData, key: Dict.Key) Dict.Hash {
    return Dict.genHash(sds.bufSlice(sds.cast(key)));
}

fn dictSdsEql(_: Dict.PrivData, key1: Dict.Key, key2: Dict.Key) bool {
    return sds.cmp(sds.cast(key1), sds.cast(key2)) == .eq;
}

fn dictSdsFree(_: Dict.PrivData, allocator: Allocator, key: Dict.Key) void {
    sds.free(allocator, sds.cast(key));
}

fn initServerConfig(allocator: Allocator) Allocator.Error!void {
    _ = allocator;
}

pub fn main() !void {
    var debug = std.heap.DebugAllocator(.{}).init;
    const allocator = debug.allocator();
    defer _ = debug.deinit();

    random.seed(null);
    Dict.hashSeed(null);

    try Server.init(allocator);
}

test {
    _ = @import("sds.zig");
    _ = @import("adlist.zig");
    _ = @import("Dict.zig");
    _ = @import("IntSet.zig");
    _ = @import("ZipList.zig");
    _ = @import("QuickList.zig");
    _ = @import("Object.zig");
    _ = @import("util.zig");
    _ = @import("t_zset.zig");
}

const Object = @import("Object.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
const sds = @import("sds.zig");
const Dict = @import("Dict.zig");
const random = @import("random.zig");
