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

/// This function is used to obtain the current LRU clock.
/// If the current resolution is lower than the frequency we refresh the
/// LRU clock (as it should be in production servers) we return the
/// precomputed value, otherwise we need to resort to a system call.
pub fn LRUClock() u32 {
    if (@divFloor(1000, server.hz) <= Server.LRU_CLOCK_RESOLUTION) {
        return server.lruclock.get();
    }
    return getLRUClock();
}

// LFU (Least Frequently Used) implementation.
// We have 24 total bits of space in each object in order to implement
// an LFU (Least Frequently Used) eviction policy, since we re-use the
// LRU field for this purpose.
//
// We split the 24 bits into two fields:
//
//          16 bits      8 bits
//     +----------------+--------+
//     + Last decr time | LOG_C  |
//     +----------------+--------+
//
// LOG_C is a logarithmic counter that provides an indication of the access
// frequency. However this field must also be decremented otherwise what used
// to be a frequently accessed key in the past, will remain ranked like that
// forever, while we want the algorithm to adapt to access pattern changes.
//
// So the remaining 16 bits are used in order to store the "decrement time",
// a reduced-precision Unix time (we take 16 bits of the time converted
// in minutes since we don't care about wrapping around) where the LOG_C
// counter is halved if it has an high value, or just decremented if it
// has a low value.
//
// New keys don't start at zero, in order to have the ability to collect
// some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
// The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
// when incrementing the key, so that keys starting at COUNTER_INIT_VAL
// (or having a smaller value) have a very high chance of being incremented
// on access.
//
// During decrement, the value of the logarithmic counter is halved if
// its current value is greater than two times the COUNTER_INIT_VAL, otherwise
// it is just decremented by one.
pub const LFU_INIT_VAL = 5;

/// Return the current time in minutes, just taking the least significant
/// 16 bits. The returned time is suitable to be stored as LDT (last decrement
/// time) for the LFU implementation.
pub fn LFUGetTimeInMinutes() u64 {
    const minutes = @divFloor(server.unixtime.get(), 60);
    return @intCast(minutes & 65535);
}

/// Logarithmically increment a counter. The greater is the current counter value
/// the less likely is that it gets really implemented. Saturate it at 255.
pub fn LFULogIncr(counter: u8) u8 {
    if (counter == 255) return 255;
    var cnt = counter;
    const r = @as(f64, @floatFromInt(random.int(i32))) / @as(f64, @floatFromInt(std.math.maxInt(i32)));
    const baseval: i32 = cnt -| LFU_INIT_VAL;
    const p = @as(f64, 1.0) / @as(f64, @floatFromInt((baseval *| server.lfu_log_factor +| 1)));
    if (r < p) cnt += 1;
    return cnt;
}

/// If the object decrement time is reached decrement the LFU counter but
/// do not update LFU fields of the object, we update the access time
/// and counter in an explicit way when the object is really accessed.
/// And we will times halve the counter according to the times of
/// elapsed time than server.lfu_decay_time.
/// Return the object frequency counter.
///
/// This function is used in order to scan the dataset for the best object
/// to fit: as we check for the candidate, we incrementally decrement the
/// counter of the scanned objects if needed.
pub fn LFUDecrAndReturn(obj: *const Object) u8 {
    const ldt: u64 = obj.lru >> 8;
    var counter: u8 = @intCast(obj.lru & 255);
    const num_periods: u64 = if (server.lfu_decay_time > 0)
        @divFloor(LFUTimeElapsed(ldt), @as(u64, @intCast(server.lfu_decay_time)))
    else
        0;
    if (num_periods > 0) {
        counter = if (num_periods > counter)
            0
        else
            counter - @as(u8, @intCast(num_periods));
    }
    return counter;
}

/// Given an object last access time, compute the minimum number of minutes
/// that elapsed since the last access. Handle overflow (ldt greater than
/// the current 16 bits minutes time) considering the time as wrapping
/// exactly once.
fn LFUTimeElapsed(ldt: u64) u64 {
    const now = LFUGetTimeInMinutes();
    if (now >= ldt) return now - ldt;
    return 65535 - ldt + now;
}

/// Given an object returns the min number of milliseconds the object was never
/// requested, using an approximated LRU algorithm.
pub fn estimateObjectIdleTime(o: *Object) u64 {
    const lrulock = LRUClock();
    if (lrulock >= o.lru) {
        return (lrulock - o.lru) * Server.LRU_CLOCK_RESOLUTION;
    }
    return (lrulock + (Server.LRU_CLOCK_MAX - o.lru)) * Server.LRU_CLOCK_RESOLUTION;
}

const std = @import("std");
const allocator = @import("allocator.zig");
const Server = @import("Server.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const random = @import("random.zig");
const log = std.log.scoped(.evict);
