pub const child = if (builtin.is_test)
    std.testing.allocator
else
    debug.allocator();

pub fn alloc(comptime T: type, n: usize) []T {
    return child.alloc(T, n) catch oom();
}

pub fn realloc(old_mem: anytype, new_n: usize) t: {
    const Slice = @typeInfo(@TypeOf(old_mem)).pointer;
    break :t []align(Slice.alignment) Slice.child;
} {
    return child.realloc(old_mem, new_n) catch oom();
}

pub fn alignedAlloc(
    comptime T: type,
    /// null means naturally aligned
    comptime alignment: ?std.mem.Alignment,
    n: usize,
) []T {
    return child.alignedAlloc(T, alignment, n) catch oom();
}

pub fn dupe(comptime T: type, m: []const T) []T {
    return child.dupe(T, m) catch oom();
}

pub fn free(memory: anytype) void {
    child.free(memory);
}

pub fn create(comptime T: type) *T {
    return child.create(T) catch oom();
}

pub fn destroy(ptr: anytype) void {
    child.destroy(ptr);
}

pub inline fn oom() noreturn {
    @panic("Out Of Memory");
}

pub fn usedMemory() usize {
    // TODO:
    return 0;
}

var debug = std.heap.DebugAllocator(.{}).init;
const std = @import("std");
const builtin = @import("builtin");
