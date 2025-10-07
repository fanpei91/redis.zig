pub const Dict = dict.Dict(sds.String, void, DictVtable);

pub const priv_data = DictVtable{};

const DictVtable = struct {
    pub fn vtable(_: DictVtable) *const Dict.Vtable {
        return &.{
            .hash = hash,
            .eql = eql,
            .dupeKey = null,
            .dupeVal = null,
            .freeKey = freeKey,
            .freeVal = null,
        };
    }

    fn hash(_: DictVtable, key: sds.String) dict.Hash {
        return std.hash.Wyhash.hash(0, sds.bufSlice(key));
    }

    fn eql(_: DictVtable, key1: sds.String, key2: sds.String) bool {
        return sds.cmp(key1, key2) == .eq;
    }

    fn freeKey(_: DictVtable, allocator: Allocator, key: sds.String) void {
        sds.free(allocator, key);
    }
};

const dict = @import("dict.zig");
const sds = @import("sds.zig");
const std = @import("std");
const Allocator = std.mem.Allocator;
