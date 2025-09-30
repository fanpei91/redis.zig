const liblzf = @cImport({
    @cInclude("lzf.h");
    @cInclude("lzfP.h");
});

pub fn compress(noalias in: []const u8, noalias out: []u8) uint {
    return liblzf.lzf_compress(
        in.ptr,
        @intCast(in.len),
        out.ptr,
        @intCast(out.len),
    );
}

pub fn decompress(noalias in: []const u8, noalias out: []u8) uint {
    return liblzf.lzf_decompress(
        in.ptr,
        @intCast(in.len),
        out.ptr,
        @intCast(out.len),
    );
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const ctypes = @import("ctypes.zig");
const uint = ctypes.uint;
