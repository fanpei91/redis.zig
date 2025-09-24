pub fn LittleEndian(comptime T: type, alignment: comptime_int) type {
    return struct {
        val: T align(alignment),

        pub fn get(self: *const @This()) T {
            return littleToNative(T, self.val);
        }

        pub fn set(self: *@This(), native: T) void {
            self.val = nativeToLittle(T, native);
        }
    };
}

const std = @import("std");
const littleToNative = std.mem.littleToNative;
const nativeToLittle = std.mem.nativeToLittle;
