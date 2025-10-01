pub fn compress(noalias in: []const u8, noalias out: []u8) uint {
    return lzf_compress(
        in.ptr,
        @intCast(in.len),
        out.ptr,
        @intCast(out.len),
    );
}

pub fn decompress(noalias in: []const u8, noalias out: []u8) uint {
    return lzf_decompress(
        in.ptr,
        @intCast(in.len),
        out.ptr,
        @intCast(out.len),
    );
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const ctypes = @import("../ctypes.zig");
const uint = ctypes.uint;
const lzf = @cImport({
    @cInclude("lzf.h");
});
const lzf_compress = lzf.lzf_compress;
const lzf_decompress = lzf.lzf_decompress;
