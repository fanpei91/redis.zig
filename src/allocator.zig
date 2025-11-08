pub fn alloc(comptime T: type, n: usize) []T {
    return allocator().alloc(T, n) catch oom();
}

pub fn realloc(old_mem: anytype, new_n: usize) t: {
    const Slice = @typeInfo(@TypeOf(old_mem)).pointer;
    break :t []align(Slice.alignment) Slice.child;
} {
    return allocator().realloc(old_mem, new_n) catch oom();
}

pub fn alignedAlloc(
    comptime T: type,
    /// null means naturally aligned
    comptime alignment: ?std.mem.Alignment,
    n: usize,
) []T {
    return allocator().alignedAlloc(T, alignment, n) catch oom();
}

pub fn dupe(comptime T: type, m: []const T) []T {
    return allocator().dupe(T, m) catch oom();
}

pub fn free(memory: anytype) void {
    allocator().free(memory);
}

pub fn create(comptime T: type) *T {
    return allocator().create(T) catch oom();
}

pub fn destroy(ptr: anytype) void {
    allocator().destroy(ptr);
}

pub inline fn oom() noreturn {
    @panic("OOM");
}

pub fn allocator() std.mem.Allocator {
    if (builtin.is_test) {
        return std.testing.allocator;
    }
    return debug.allocator();
}

var debug = std.heap.DebugAllocator(.{}).init;
const std = @import("std");
const builtin = @import("builtin");
