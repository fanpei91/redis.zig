var prng: std.Random.DefaultPrng = .init(0);
var random: std.Random = prng.random();

pub fn seed(s: ?u64) void {
    prng = .init(s orelse @bitCast(std.time.microTimestamp()));
    random = prng.random();
}

pub fn int(comptime T: type) T {
    return random.int(T);
}

const std = @import("std");
const builtin = @import("builtin");
