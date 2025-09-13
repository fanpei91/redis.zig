/// zlend
const END = 0xFF;

/// Indicate the length of the previous entry is larger than 254. Wht is not
/// 255? 255 is occupied by END.
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

/// zlbytes + zltail + zllen
const HEADER_SIZE = @sizeOf(ZipList);

/// ZIPLIST OVERALL LAYOUT:
/// The general layout of the ziplist is as follows:
/// <zlbytes><zltail><zllen><entry><entry><zlend>
///
/// <zlbytes> is an unsigned integer to hold the number of bytes that the
/// ziplist occupies. This value needs to be stored to be able to resize the
/// entire structure without the need to traverse it first.
///
/// <zltail> is the offset to the last entry in the list. This allows a pop
/// operation on the far side of the list without the need for full traversal.
///
/// <zllen> is the number of entries.When this value is larger than 2**16-2,
/// we need to traverse the entire list to know how many items it holds.
///
/// <zlend> is a single byte special value, equal to 255, which indicates the
/// end of the list.
pub const ZipList = @This();
bytes: LittleEndian(u32), // zlbytes
tail: LittleEndian(u32), // zltail
len: LittleEndian(u16), // zllen
entries: [0]u8, // entries
// zlend: 0xff

/// Every entry in the ziplist is prefixed by a header that contains two pieces
/// of information. First, the length of the previous entry is stored to be
/// able to traverse the list from back to front. Second, the encoding with an
/// optional string length of the entry itself is stored.
///
/// The length of the previous entry is encoded in the following way:
/// If this length is smaller than 254 bytes, it will only consume a single
/// byte that takes the length as value. When the length is greater than or
/// equal to 254, it will consume 5 bytes. The first byte is set to 254 to
/// indicate a larger value is following. The remaining 4 bytes take the
/// length of the previous entry as value.
///
/// The other header field of the entry itself depends on the contents of the
/// entry. When the entry is a string, the first 2 bits of this header will hold
/// the type of encoding used to store the length of the string, followed by the
/// actual length of the string. When the entry is an integer the first 2 bits
/// are both set to 1. The following 2 bits are used to specify what kind of
/// integer will be stored after this header. An overview of the different
/// types and encodings is as follows:
///
/// |00pppppp| - 1 byte
///      String value with length less than or equal to 63 bytes (6 bits).
/// |01pppppp|qqqqqqqq| - 2 bytes
///      String value with length less than or equal to 16383 bytes (14 bits).
/// |10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
///      String value with length greater than or equal to 16384 bytes.
/// |11000000| - 1 byte
///      Integer encoded as int16_t (2 bytes).
/// |11010000| - 1 byte
///      Integer encoded as int32_t (4 bytes).
/// |11100000| - 1 byte
///      Integer encoded as int64_t (8 bytes).
/// |11110000| - 1 byte
///      Integer encoded as 24 bit signed (3 bytes).
/// |11111110| - 1 byte
///      Integer encoded as 8 bit signed (1 byte).
/// |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
///      Unsigned integer from 0 to 12. The encoded value is actually from
///      1 to 13 because 0000 and 1111 can not be used, so 1 should be
///      subtracted from the encoded 4 bit value to obtain the right value.
/// |11111111| - End of ziplist.
const Entry = struct {
    /// The number of bytes required to store the length of the previous
    /// element. 1 or 5.
    prev_rawlen_size: u32,
    prev_rawlen: u32,

    encoding: u8,

    /// The bytes required to encode the entries length.
    len_size: u32,
    /// The entry length.
    len: u32,

    /// prev_rawlen_size + len_size
    header_size: u32,

    /// The start address of the current entry.
    p: [*]u8,

    fn decode(ptr: [*]u8) Entry {
        var entry: Entry = undefined;
        var p = ptr;
        entry.p = ptr;

        // The length of the previous entry.
        if (p[0] < BIG_LEN) {
            entry.prev_rawlen_size = 1;
        } else {
            entry.prev_rawlen_size = 5;
        }
        p += entry.prev_rawlen_size;

        entry.encoding = p[0];
        if (entry.encoding < STR_MSK) {
            entry.encoding &= STR_MSK;
        }

        if (entry.encoding < STR_MSK) {
            switch (entry.encoding) {
                STR_06B => {
                    entry.len_size = 1;
                    entry.len = p[0] & 0b00111111;
                },
                STR_14B => {
                    entry.len_size = 2;
                    entry.len = (@as(u32, (p[0] & 0b00111111)) << 8) | p[1];
                },
                STR_32B => {
                    entry.len_size = 5;
                    entry.len = @as(u32, p[1]) << 24;
                    entry.len |= (@as(u32, p[2]) << 16);
                    entry.len |= (@as(u32, p[3]) << 8);
                    entry.len |= p[4];
                },
                else => unreachable,
            }
        } else {
            entry.len_size = 1;
            entry.len = intSize(entry.encoding);
        }

        entry.header_size = entry.prev_rawlen_size + entry.len_size;

        return entry;
    }
};

pub fn new(allocator: Allocator) Allocator.Error!*ZipList {
    const bytes = HEADER_SIZE + 1;
    const ptr = (try allocator.alignedAlloc(u8, .of(ZipList), bytes)).ptr;
    const zl: *ZipList = @ptrCast(@alignCast(ptr));
    zl.bytes.set(bytes);
    zl.tail.set(HEADER_SIZE);
    zl.len.set(0);
    ptr[bytes - 1] = END;
    return zl;
}

pub fn numOfEntries(zl: *ZipList) u16 {
    var cnt = zl.len.get();
    if (cnt < std.math.maxInt(u16)) {
        return cnt;
    }

    cnt = 0;
    var p: [*]u8 = @ptrFromInt(@intFromPtr(zl) + HEADER_SIZE);
    while (p[0] != END) {
        p = @ptrFromInt(@intFromPtr(p) + rawEntryLength(p));
        cnt += 1;
    }
    // Re-store length if small enough.
    if (cnt < std.math.maxInt(u16)) {
        zl.len.set(cnt);
    }
    return cnt;
}

pub fn asBytes(zl: *ZipList) []align(@alignOf(ZipList)) u8 {
    const ptr: [*]align(@alignOf(ZipList)) u8 = @ptrCast(@alignCast(zl));
    return ptr[0..zl.bytes.get()];
}

pub fn blobLen(zl: *ZipList) u32 {
    return zl.bytes.get();
}

pub fn append(
    zl: *ZipList,
    allocator: Allocator,
    s: []u8,
) Allocator.Error!*ZipList {
    return zl.insert(allocator, zl.ENTRY_TAIL(), s);
}

pub fn prepend(
    zl: ZipList,
    allocator: Allocator,
    s: []u8,
) Allocator.Error!*ZipList {
    return zl.insert(allocator, zl.ENTRY_HEAD(), s);
}

pub fn insert(
    zl: *ZipList,
    allocator: Allocator,
    p: [*]u8,
    s: []u8,
) Allocator.Error!*ZipList {
    _ = zl;
    _ = allocator;
    _ = p;
    _ = s;
}

pub fn free(zl: *ZipList, allocator: Allocator) void {
    allocator.free(zl.asBytes());
}

inline fn ENTRY_HEAD(zl: *ZipList) [*]u8 {
    return @ptrFromInt(@intFromPtr(zl) + HEADER_SIZE);
}

inline fn ENTRY_TAIL(zl: *ZipList) [*]u8 {
    const offset = zl.bytes.get() - 1;
    return @ptrFromInt(@intFromPtr(zl) + offset);
}

fn rawEntryLength(p: [*]u8) u32 {
    const entry = Entry.decode(p);
    return entry.prev_rawlen_size + entry.len_size + entry.len;
}

fn intSize(encoding: u8) u32 {
    switch (encoding) {
        INT_08B => return 1,
        INT_16B => return 2,
        INT_24B => return 3,
        INT_32B => return 4,
        INT_64B => return 8,
        else => return 0, // 4 bit immediate
    }
    unreachable;
}

inline fn INT_IMM_VAL(v: u8) u8 {
    return INT_IMM_MSK & v;
}

test "empty entries layout" {
    const allocator = testing.allocator;
    const zl = try new(allocator);
    defer zl.free(allocator);
    const bytes = zl.bytes.get();
    try expectEqual(HEADER_SIZE + 1, bytes);
    try expectEqual(HEADER_SIZE, zl.tail.get());
    try expectEqual(0, zl.numOfEntries());
    try expectEqual(END, zl.asBytes()[bytes - 1]);
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
const testing = std.testing;
const expectEqual = testing.expectEqual;
const LittleEndian = @import("endian.zig").LittleEndian;
