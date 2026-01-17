var hash_seed: u64 = 0;

pub fn seed(s: ?u64) void {
    hash_seed = s orelse std.crypto.random.int(u64);
}

pub const Hash = u64;

pub fn hash(key: []const u8) Hash {
    return std.hash.Wyhash.hash(
        hash_seed,
        key,
    );
}

pub fn hashcase(key: []const u8, comptime max_len: usize) Hash {
    var stack_impl = std.heap.stackFallback(
        max_len,
        allocator.impl,
    );
    const stack_allocator = stack_impl.get();
    const s = sds.new(stack_allocator, key);
    defer sds.free(stack_allocator, s);
    sds.toLower(s);
    return hash(sds.asBytes(s));
}

const std = @import("std");
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
