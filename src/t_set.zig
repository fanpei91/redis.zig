pub const Set = struct {
    pub const Dict = dict.Dict(sds.String, void);

    const vtable: *const Dict.VTable = &.{
        .hash = hash,
        .eql = eql,
        .freeKey = freeKey,
    };

    fn hash(key: sds.String) dict.Hash {
        dict.genHash(sds.asBytes(key));
    }

    fn eql(k1: sds.String, k2: sds.String) bool {
        return sds.cmp(k1, k2) == .eq;
    }

    fn freeKey(key: sds.String) void {
        sds.free(key);
    }
};

pub fn create() *Set.Dict {
    return Set.Dict.create(Set.vtable);
}

const dict = @import("dict.zig");
const sds = @import("sds.zig");
const std = @import("std");
