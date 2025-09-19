pub fn StatsAllocator(comptime thread_safe: bool) type {
    return struct {
        child: mem.Allocator,
        used: if (thread_safe) atomic.Value(usize) else usize,

        fn allocator(self: *@This()) mem.Allocator {
            return .{
                .ptr = self,
                .vtable = &.{
                    .alloc = alloc,
                    .resize = resize,
                    .remap = remap,
                    .free = free,
                },
            };
        }

        fn alloc(
            ctx: *anyopaque,
            n: usize,
            alignment: std.mem.Alignment,
            ra: usize,
        ) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child_allocator.rawAlloc(n, alignment, ra);
        }

        fn resize(
            ctx: *anyopaque,
            buf: []u8,
            alignment: std.mem.Alignment,
            new_len: usize,
            ret_addr: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child_allocator.rawResize(
                buf,
                alignment,
                new_len,
                ret_addr,
            );
        }

        fn remap(
            context: *anyopaque,
            memory: []u8,
            alignment: std.mem.Alignment,
            new_len: usize,
            return_address: usize,
        ) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(context));
            return self.child_allocator.rawRemap(
                memory,
                alignment,
                new_len,
                return_address,
            );
        }

        fn free(
            ctx: *anyopaque,
            buf: []u8,
            alignment: std.mem.Alignment,
            ret_addr: usize,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child_allocator.rawFree(buf, alignment, ret_addr);
        }

        fn updateStatAlloc(self: *@This(), size: usize) void {
            if (thread_safe) {
                _ = self.used.fetchAdd(size, .unordered);
            } else {
                self.used += size;
            }
        }

        fn usedMemory(self: *@This()) usize {
            if (thread_safe) {
                return self.used.fetchAdd(0, .unordered);
            }
            return self.used;
        }
    };
}

const std = @import("std");
const mem = std.mem;
const atomic = std.atomic;
