/// Return the LRU clock, based on the clock resolution. This is a time
/// in a reduced-bits format that can be used to set and check the
/// object.lru field of Object structures.
pub fn getLRUClock() u32 {
    const clock: i64 = @divTrunc(
        std.time.milliTimestamp(),
        Server.LRU_CLOCK_RESOLUTION,
    );
    return @intCast(clock & Server.LRU_CLOCK_MAX);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const Server = @import("Server.zig");
