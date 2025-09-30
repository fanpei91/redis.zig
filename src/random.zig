var prng: std.Random.DefaultPrng = undefined;
var random: std.Random = undefined;
var init = false;

pub fn seed(s: ?u64) void {
    if (init) return;
    init = true;
    prng = .init(s orelse @bitCast(std.time.microTimestamp()));
    random = prng.random();
}

pub fn int(comptime T: type) T {
    seed(null);
    return random.int(T);
}

const std = @import("std");
