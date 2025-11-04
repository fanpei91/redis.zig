pub fn Value(comptime T: type) type {
    return struct {
        const Self = @This();

        v: std.atomic.Value(T),

        pub fn init(value: T) Self {
            return .{ .v = .init(value) };
        }

        pub fn store(self: *Self, value: T) void {
            self.v.store(value, .monotonic);
        }

        pub fn get(self: *Self) T {
            return self.v.load(.monotonic);
        }

        pub fn getIncr(self: *Self, operand: T) T {
            return self.v.fetchAdd(operand, .monotonic);
        }
    };
}

const std = @import("std");
