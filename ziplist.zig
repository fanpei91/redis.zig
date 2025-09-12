const END = 0xFF; // zlend
const BIG_LEN = 254;

const STR_MSK = 0b11000000;
const STR_06B = 0b00000000;
const STR_14B = 0b01000000;
const STR_32B = 0b10000000;

const INT_MSK = 0b00110000;
const INT_08B = 0b11111110;
const INT_16B = 0b11000000;
const INT_24B = 0b11110000;
const INT_32B = 0b11010000;
const INT_64B = 0b11100000;

// 4 bit integer immediate encoding
const INT_IMM_MSK = 0b00001111;
const INT_IMM_MIN = 0b11110001;
const INT_IMM_MAX = 0b11111101;

const HEADER_SIZE = @sizeOf(u32) * 2 + @sizeOf(u16); // zlbytes + zltail + zllen

const Entry = struct {
    prev_rawlen_size: u32,
    prev_rawlen: u32,
    len_size: u32,
    len: u32,
    header_size: u32,
    encoding: u8,
    p: [*]u8,
};

/// Layout: zlbytes(u32) zltail(u32) zllen(u16) entry... zlend(0xFF);
pub const ZipList = [*]u8;

pub fn new(allocator: Allocator) Allocator.Error!ZipList {
    const bytes = HEADER_SIZE + 1;
    const zl = (try allocator.alignedAlloc(u8, .of(u32), bytes)).ptr;
    BYTES(zl).* = nativeToLittle(u32, bytes);
    TAIL_OFFSET(zl).* = nativeToLittle(u32, HEADER_SIZE);
    LENGTH(zl).* = nativeToLittle(u16, 0);
    zl[bytes - 1] = END;
    return zl;
}

pub fn blobLen(zl: ZipList) u32 {
    return littleToNative(u32, BYTES(zl).*);
}

pub fn free(allocator: Allocator, zl: ZipList) void {
    const buffer: [*]align(@alignOf(u32)) u8 = @ptrCast(@alignCast(zl));
    allocator.free(buffer[0..blobLen(zl)]);
}

inline fn INT_IMM_VAL(v: u8) u8 {
    return INT_IMM_MSK & v;
}

inline fn BYTES(zl: ZipList) *u32 {
    return @ptrCast(@alignCast(zl));
}

inline fn TAIL_OFFSET(zl: ZipList) *u32 {
    const tail: [*]u8 = @ptrFromInt(@intFromPtr(zl) + @sizeOf(u32));
    return @ptrCast(@alignCast(tail));
}

inline fn LENGTH(zl: ZipList) *u16 {
    const length: [*]u8 = @ptrFromInt(@intFromPtr(zl) + @sizeOf(u32) * 2);
    return @ptrCast(@alignCast(length));
}

test "empty entries layout" {
    const allocator = testing.allocator;
    const zl = try new(allocator);
    defer free(allocator, zl);
    const bytes = littleToNative(u32, BYTES(zl).*);
    try expectEqual(HEADER_SIZE + 1, bytes);
    try expectEqual(HEADER_SIZE, littleToNative(u32, TAIL_OFFSET(zl).*));
    try expectEqual(0, littleToNative(u32, LENGTH(zl).*));
    try expectEqual(END, zl[bytes - 1]);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
const testing = std.testing;
const expectEqual = testing.expectEqual;
