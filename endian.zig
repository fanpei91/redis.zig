pub fn LittleEndian(comptime T: type) type {
    return struct {
        val: T,

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
