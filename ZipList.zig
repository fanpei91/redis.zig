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
const INT_IMM_MIN = 0b11110001; // = 0, for 0b11110000 is occupied by INT_24B
const INT_IMM_MAX = 0b11111101; // = 12,

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
    prev_rawlen_size: u8,
    prev_rawlen: u32,

    encoding: u8,

    /// The bytes required to encode the entries length.
    len_size: u8,
    /// The entry length.
    len: u32,

    /// prev_rawlen_size + len_size
    header_size: u8,

    /// The start address of the current entry.
    ptr: [*]const u8,

    fn decodePrevLen(ptr: [*]const u8) struct { size: u8, len: u32 } {
        if (ptr[0] < BIG_LEN) {
            return .{ .size = 1, .len = ptr[0] };
        }
        return .{
            .size = 5,
            .len = readInt(u32, ptr[1..5], .little),
        };
    }

    fn decodeLength(ptr: [*]const u8) struct { encoding: u8, size: u8, len: u32 } {
        var encoding = ptr[0];
        if (encoding < STR_MSK) {
            encoding &= STR_MSK;
        }

        var len_size: u8 = 0;
        var len: u32 = 0;
        if (encoding < STR_MSK) {
            switch (encoding) {
                STR_06B => {
                    len_size = 1;
                    len = ptr[0] & 0b00111111;
                },
                STR_14B => {
                    len_size = 2;
                    len = (@as(u16, (ptr[0] & 0b00111111)) << 8) | @as(u16, ptr[1]);
                },
                STR_32B => {
                    len_size = 5;
                    len = readInt(u32, ptr[1..5], .big);
                },
                else => unreachable,
            }
        } else {
            len_size = 1;
            len = intSize(encoding);
        }
        return .{
            .encoding = encoding,
            .size = len_size,
            .len = len,
        };
    }

    fn decode(ptr: [*]const u8) Entry {
        var entry: Entry = undefined;
        entry.ptr = ptr;
        var p = ptr;

        // The length of the previous entry.
        const prevlen = decodePrevLen(p);
        entry.prev_rawlen_size = prevlen.size;
        entry.prev_rawlen = prevlen.len;

        p += entry.prev_rawlen_size;

        const length = decodeLength(p);
        entry.encoding = length.encoding;
        entry.len_size = length.size;
        entry.len = length.len;

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

pub fn numOfEntries(self: *ZipList) u32 {
    var cnt: u32 = self.len.get();
    if (cnt < maxInt(u16)) {
        return cnt;
    }

    cnt = 0;
    var p: [*]u8 = @ptrFromInt(@intFromPtr(self) + HEADER_SIZE);
    while (p[0] != END) {
        p = @ptrFromInt(@intFromPtr(p) + rawEntryLength(p));
        cnt += 1;
    }
    // Re-store length if small enough.
    if (cnt < maxInt(u16)) {
        self.len.set(@intCast(cnt));
    }
    return cnt;
}

pub fn asBytes(self: *const ZipList) []align(@alignOf(ZipList)) u8 {
    const ptr: [*]align(@alignOf(ZipList)) u8 = @ptrCast(@alignCast(@constCast(self)));
    return ptr[0..self.blobLen()];
}

pub fn blobLen(self: *const ZipList) u32 {
    return self.bytes.get();
}

pub fn append(
    self: *ZipList,
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*ZipList {
    return self.insert(allocator, self.entryEnd(), str);
}

pub fn prepend(
    self: *ZipList,
    allocator: Allocator,
    str: []const u8,
) Allocator.Error!*ZipList {
    return self.insert(allocator, self.entryHead(), str);
}

pub fn insert(
    self: *ZipList,
    allocator: Allocator,
    ptr: [*]u8,
    str: []const u8,
) Allocator.Error!*ZipList {
    var zl = self;
    var p = ptr;

    const curlen: u32 = zl.bytes.get();
    var prevlen: u32 = 0;
    if (p[0] != END) {
        prevlen = (Entry.decodePrevLen(p)).len;
    } else {
        const tail = zl.entryTail();
        prevlen = rawEntryLength(tail);
    }

    var value: i64 = undefined;
    var encoding: u8 = STR_06B;
    var reqlen: u32 = @intCast(str.len);
    if (tryEncoding(str, &value, &encoding)) {
        reqlen = intSize(encoding);
    }
    reqlen += encodePrevLength(null, prevlen);
    reqlen += encodeLength(null, encoding, @intCast(str.len));

    // When the insert position is not equal to the tail, we need to
    // make sure that the next entry can hold this entry's length in
    // its prevlen field.
    const nextdiff = if (p[0] != END) prevLenByteDiff(p, reqlen) else 0;

    // Store offset because a realloc may change the address of zl.
    var offset: u32 = @intCast(@intFromPtr(p) - @intFromPtr(zl));
    zl = try zl.resize(
        allocator,
        @intCast(@as(i64, curlen + reqlen) + @as(i64, nextdiff)),
    );
    p = @ptrFromInt(@intFromPtr(zl) + offset);

    // Apply memory move when necessary and update tail offset.
    if (p[0] != END) {
        const size: u32 = @intCast(
            @as(i64, curlen - offset) + @as(i64, nextdiff) - 1,
        );
        const dest = p + reqlen;
        const src = if (nextdiff > 0) p - @abs(nextdiff) else p + @abs(nextdiff);
        @memmove(dest[0..size], src[0..size]);

        // Encode this entry's raw length in the next entry.
        _ = encodePrevLength(p + reqlen, reqlen);

        // Update offset for tail.
        zl.tail.set(@intCast(@as(i64, zl.tail.get() + reqlen) + @as(i64, nextdiff)));
    } else {
        zl.tail.set(@intCast(@intFromPtr(p) - @intFromPtr(zl)));
    }

    // When nextdiff != 0, the raw length of the next entry has changed, so
    // we need to cascade the update throughout the ziplist
    if (nextdiff != 0) {
        offset = @intCast(@intFromPtr(p) - @intFromPtr(zl));
        zl = try cascadeUpdate(zl, allocator, p + reqlen);
        p = @ptrFromInt(@intFromPtr(zl) + offset);
    }

    p += encodePrevLength(p, prevlen);
    p += encodeLength(p, encoding, @intCast(str.len));
    if (isStr(encoding)) {
        @memcpy(p[0..str.len], str);
    } else {
        encodeInteger(p, value, encoding);
    }
    zl.incrLength(1);

    return zl;
}

pub fn free(self: *ZipList, allocator: Allocator) void {
    allocator.free(self.asBytes());
}

fn incrLength(self: *ZipList, incr: u16) void {
    var curlen = self.len.get();
    if (curlen < maxInt(u16)) {
        curlen +|= incr;
        self.len.set(curlen);
    }
}

fn encodeInteger(p: [*]u8, value: i64, encoding: u8) void {
    switch (encoding) {
        INT_08B => p[0] = @intCast(value),
        INT_16B => writeInt(i16, p[0..2], @intCast(value), .little),
        INT_24B => writeInt(i24, p[0..3], @intCast(value), .little),
        INT_32B => writeInt(i32, p[0..4], @intCast(value), .little),
        INT_64B => writeInt(i64, p[0..8], value, .little),
        INT_IMM_MIN...INT_IMM_MAX => {
            // Nothing to do, the value is stored in the encoding itself.
        },
        else => unreachable,
    }
}

fn cascadeUpdate(
    self: *ZipList,
    allocator: Allocator,
    ptr: [*]u8,
) Allocator.Error!*ZipList {
    var p = ptr;
    var zl = self;
    var curlen = zl.bytes.get();
    while (p[0] != END) {
        const cur = Entry.decode(p);
        const cur_rawlen = cur.header_size + cur.len;
        // Abort if there is no next entry.
        if (p[cur_rawlen] == END) break;

        const next = Entry.decode(p + cur_rawlen);
        // Abort when "prevlen" has not changed.
        if (next.prev_rawlen == cur_rawlen) break;

        const cur_rawlen_size = encodePrevLength(null, cur_rawlen);
        if (next.prev_rawlen_size < cur_rawlen_size) {
            const offset = @intFromPtr(p) - @intFromPtr(zl);
            const extra = cur_rawlen_size - next.prev_rawlen_size;
            zl = try zl.resize(allocator, curlen + extra);
            p = @ptrFromInt(@intFromPtr(zl) + offset);

            // Next element.
            const np = p + cur_rawlen;
            const noffset: u32 = @intCast(@intFromPtr(np) - @intFromPtr(zl));

            // Update tail offset when next element is not the tail element.
            const tail: [*]u8 = @ptrFromInt(@intFromPtr(zl) + zl.tail.get());
            if (tail != np) {
                zl.tail.set(zl.tail.get() + extra);
            }

            // Move the tail to the back.
            const nsize = curlen - noffset - next.prev_rawlen_size - 1;
            @memmove(
                (np + cur_rawlen_size)[0..nsize],
                (np + next.prev_rawlen_size)[0..nsize],
            );
            _ = encodePrevLength(np, cur_rawlen);

            p += cur_rawlen;
            curlen += extra;
        } else {
            // Next element.
            const np = p + cur_rawlen;
            if (next.prev_rawlen_size > cur_rawlen_size) {
                // This would result in shrinking, which we want to avoid.
                // So, set "cur_rawlen" in the available bytes.
                encodePrevLengthForceLarge(np, cur_rawlen);
            } else {
                _ = encodePrevLength(np, cur_rawlen);
            }
            break;
        }
    }
    return zl;
}

fn resize(
    self: *ZipList,
    allocator: Allocator,
    len: u32,
) Allocator.Error!*ZipList {
    const old_mem = self.asBytes();
    const new_mem = try allocator.realloc(old_mem, len);
    const zl: *ZipList = @ptrCast(@alignCast(new_mem));
    zl.bytes.set(len);
    new_mem[len - 1] = END;
    return zl;
}

inline fn entryHead(self: *const ZipList) [*]u8 {
    return @ptrFromInt(@intFromPtr(self) + HEADER_SIZE);
}

inline fn entryTail(self: *const ZipList) [*]u8 {
    return @ptrFromInt(@intFromPtr(self) + self.tail.get());
}

inline fn entryEnd(self: *const ZipList) [*]u8 {
    const offset = self.bytes.get() - 1;
    return @ptrFromInt(@intFromPtr(self) + offset);
}

fn encodePrevLength(ptr: ?[*]u8, len: u32) u8 {
    if (ptr) |p| {
        if (len < BIG_LEN) {
            p[0] = @intCast(len);
            return 1;
        }
        p[0] = BIG_LEN;
        writeInt(u32, p[1..5], len, .little);
        return 5;
    }
    return if (len < BIG_LEN) 1 else 5;
}

fn encodePrevLengthForceLarge(ptr: [*]u8, len: u32) void {
    ptr[0] = BIG_LEN;
    writeInt(u32, ptr[1..5], len, .little);
}

/// Store length in big-endian format (Redis ziplist convention).
fn encodeLength(ptr: ?[*]u8, encoding: u8, len: u32) u8 {
    var len_size: u8 = 1;
    var buf: [5]u8 = @splat(0);
    if (isStr(encoding)) {
        if (len <= 0b00111111) {
            if (ptr == null) return len_size;
            buf[0] = STR_06B | @as(u8, @truncate(len));
        } else if (len <= 0b00111111_11111111) {
            len_size += 1;
            if (ptr == null) return len_size;
            buf[0] = STR_14B | (@as(u8, @truncate(len >> 8)) & 0b00111111);
            buf[1] = @as(u8, @truncate(len));
        } else {
            len_size += 4;
            if (ptr == null) return len_size;
            buf[0] = STR_32B;
            buf[1] = @as(u8, @truncate(len >> 24));
            buf[2] = @as(u8, @truncate(len >> 16));
            buf[3] = @as(u8, @truncate(len >> 8));
            buf[4] = @as(u8, @truncate(len));
        }
    } else {
        // Integer encoding.
        if (ptr == null) return len_size;
        buf[0] = encoding;
    }
    @memcpy(ptr.?[0..len_size], buf[0..len_size]);
    return len_size;
}

/// Check if string pointed to by `str` can be encoded as an integer.
/// Stores the integer value in `out_int` and its encoding in `out_encoding`.
fn tryEncoding(str: []const u8, out_int: *i64, out_encoding: *u8) bool {
    if (str.len >= 32 or str.len == 0) return false;

    const value = std.fmt.parseInt(i64, str, 10) catch {
        return false;
    };
    if (value >= 0 and value <= 12) {
        out_encoding.* = INT_IMM_MIN + @as(u8, @intCast(value));
    } else if (value >= minInt(i8) and value <= maxInt(i8)) {
        out_encoding.* = INT_08B;
    } else if (value >= minInt(i16) and value <= maxInt(i16)) {
        out_encoding.* = INT_16B;
    } else if (value >= maxInt(i24) and value <= maxInt(i24)) {
        out_encoding.* = INT_24B;
    } else if (value >= maxInt(i32) and value <= maxInt(i32)) {
        out_encoding.* = INT_32B;
    } else {
        out_encoding.* = INT_64B;
    }
    out_int.* = value;
    return true;
}

/// Return the difference in number of bytes needed to store the length of the
/// previous element `len`, in the entry pointed to by `ptr`.
fn prevLenByteDiff(ptr: [*]const u8, len: u32) i8 {
    const prevlen = Entry.decodePrevLen(ptr);
    const a: i8 = @intCast(encodePrevLength(null, len));
    const b: i8 = @intCast(prevlen.size);
    return a - b;
}

/// Return the total number of bytes used by the entry pointed to by `ptr`.
fn rawEntryLength(ptr: [*]const u8) u32 {
    const entry = Entry.decode(ptr);
    return entry.prev_rawlen_size + entry.len_size + entry.len;
}

/// Return bytes needed to store integer encoded by `encoding`.
fn intSize(encoding: u8) u8 {
    return switch (encoding) {
        INT_08B => 1,
        INT_16B => 2,
        INT_24B => 3,
        INT_32B => 4,
        INT_64B => 8,
        else => 0, // 4 bit immediate
    };
}

inline fn isStr(encoding: u8) bool {
    return encoding & STR_MSK < STR_MSK;
}

test ZipList {
    const allocator = testing.allocator;
    var zl = try new(allocator);
    defer zl.free(allocator);

    const bytes = zl.bytes.get();
    try expectEqual(HEADER_SIZE + 1, bytes);
    try expectEqual(HEADER_SIZE, zl.tail.get());
    try expectEqual(0, zl.numOfEntries());
    try expectEqual(END, zl.asBytes()[bytes - 1]);

    const hello = "hello";
    zl = try zl.prepend(allocator, hello);
    try expectEqual(1, zl.numOfEntries());
    try expectEqual(@sizeOf(ZipList) + 1 + 1 + hello.len + 1, zl.bytes.get());
    try expectEqual(@sizeOf(ZipList), zl.tail.get());

    zl = try zl.prepend(allocator, hello);
    try expectEqual(2, zl.numOfEntries());

    zl = try zl.append(allocator, hello);
    try expectEqual(3, zl.numOfEntries());

    const large_str: [255]u8 = @splat(0);
    zl = try zl.prepend(allocator, &large_str);
    try expectEqual(4, zl.numOfEntries());

    for (0..maxInt(u16)) |_| {
        zl = try zl.append(allocator, hello);
    }
    try expectEqual(@as(u32, maxInt(u16)) + 4, zl.numOfEntries());
    try expectEqual(maxInt(u16), zl.len.get());
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
const testing = std.testing;
const expectEqual = testing.expectEqual;
const LittleEndian = @import("endian.zig").LittleEndian;
const minInt = std.math.minInt;
const maxInt = std.math.maxInt;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
