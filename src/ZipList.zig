/// zlend
const END = 0xFF;

/// Indicate the length of the previous entry is larger than 254. Wht is not
/// 255? 255 is occupied by END.
const BIG_PREVLEN = 254;

const STR_MSK = 0b11000000;
const STR_06B = 0b00000000;
const STR_14B = 0b01000000;
const STR_32B = 0b10000000;

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
const END_SIZE = @sizeOf(u8);

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
bytes: LittleEndian(u32, 1), // zlbytes
tail: LittleEndian(u32, 1), // zltail
len: LittleEndian(u16, 1), // zllen
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
    ptr: [*]u8,

    fn decode(entry: [*]u8) Entry {
        var ent: Entry = undefined;
        ent.ptr = entry;
        var p = entry;

        // The length of the previous entry.
        const prevlen = loadPrevEntryLength(p);
        ent.prev_rawlen_size = prevlen.size;
        ent.prev_rawlen = prevlen.len;

        p += ent.prev_rawlen_size;

        const enc = loadEntryEncoding(p);
        ent.encoding = enc.encoding;
        ent.len_size = enc.size;
        ent.len = enc.len;

        ent.header_size = ent.prev_rawlen_size + ent.len_size;
        return ent;
    }
};

pub inline fn cast(ptr: *anyopaque) *ZipList {
    return @ptrCast(@alignCast(ptr));
}

pub fn new(allocator: Allocator) Allocator.Error!*ZipList {
    const bytes = HEADER_SIZE + END_SIZE;
    const ptr = (try allocator.alignedAlloc(u8, .of(ZipList), bytes)).ptr;
    const zl: *ZipList = @ptrCast(@alignCast(ptr));
    zl.bytes.set(bytes);
    zl.tail.set(HEADER_SIZE);
    zl.len.set(0);
    ptr[bytes - 1] = END;
    return zl;
}

/// Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
///
/// NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
/// Either 'first' or 'second' can be used for the result.  The parameter not
/// used will be free'd and set to NULL.
///
/// After calling this function, the input parameters are no longer valid since
/// they are changed and free'd in-place.
///
/// The result ziplist is the contents of 'first' followed by 'second'.
///
/// On failure: returns NULL if the merge is impossible.
/// On success: returns the merged ziplist (which is expanded version of either
/// 'first' or 'second', also frees the other unused input ziplist, and sets the
/// input ziplist argument equal to newly reallocated ziplist return value.
pub fn merge(
    allocator: Allocator,
    first: *?*ZipList,
    second: *?*ZipList,
) Allocator.Error!?*ZipList {
    if (first.* == null) return null;
    if (second.* == null) return null;
    if (first.* == second.*) return null;

    const first_bytes: usize = first.*.?.bytes.get();
    const first_len: usize = first.*.?.len.get();
    const second_bytes: usize = second.*.?.bytes.get();
    const second_len: usize = second.*.?.*.len.get();

    var source: *ZipList = undefined;
    var source_bytes: usize = undefined;
    var target: *ZipList = undefined;
    var target_bytes: usize = undefined;
    var append: bool = undefined;
    if (first_len >= second_len) {
        target = first.*.?;
        target_bytes = first_bytes;
        source = second.*.?;
        source_bytes = second_bytes;
        append = true;
    } else {
        target = second.*.?;
        target_bytes = second_bytes;
        source = first.*.?;
        source_bytes = first_bytes;
        append = false;
    }

    const zlbytes: usize = first_bytes + second_bytes - HEADER_SIZE - END_SIZE;
    var zllength: usize = first_len + second_len;
    zllength = if (zllength < maxInt(u16)) zllength else maxInt(u16);

    // Larger values can't be stored into zlbytes.
    assert(zlbytes < maxInt(u32));

    const first_tail: usize = first.*.?.tail.get();
    const second_tail: usize = second.*.?.tail.get();

    const ptr = (try allocator.realloc(target.toBytes(), zlbytes)).ptr;
    target = @ptrCast(@alignCast(ptr));
    errdefer target.free(allocator);

    if (append) {
        memcpy(
            target.addr() + target_bytes - END_SIZE,
            source.addr() + HEADER_SIZE,
            source_bytes - HEADER_SIZE,
        );
    } else {
        memmove(
            target.addr() + source_bytes - END_SIZE,
            target.addr() + HEADER_SIZE,
            target_bytes - HEADER_SIZE,
        );
        memcpy(target.addr(), source.addr(), source_bytes - END_SIZE);
    }

    target.bytes.set(@intCast(zlbytes));
    target.len.set(@intCast(zllength));
    target.tail.set(@intCast((first_bytes - END_SIZE) + (second_tail - HEADER_SIZE)));
    target = try target.cascadeUpdate(allocator, target.addr() + first_tail);

    if (append) {
        second.*.?.free(allocator);
        second.* = null;
        first.* = target;
    } else {
        first.*.?.free(allocator);
        first.* = null;
        second.* = target;
    }
    return target;
}

pub fn numOfEntries(self: *ZipList) u32 {
    var cnt: u32 = self.len.get();
    if (cnt < maxInt(u16)) {
        return cnt;
    }

    cnt = 0;
    var entry: [*]u8 = self.addr() + HEADER_SIZE;
    while (entry[0] != END) {
        entry += rawEntryLength(entry);
        cnt += 1;
    }
    // Re-store length if small enough.
    if (cnt < maxInt(u16)) {
        self.len.set(@intCast(cnt));
    }
    return cnt;
}

pub fn index(self: *const ZipList, idx: i32) ?[*]u8 {
    var i = idx;
    var p: [*]u8 = undefined;
    if (i < 0) {
        i = (-i) - 1;
        p = self.entryTail();
        if (p[0] != END) {
            var prevlen = loadPrevEntryLength(p);
            while (prevlen.len > 0 and i > 0) : (i -= 1) {
                p -= prevlen.len;
                prevlen = loadPrevEntryLength(p);
            }
        }
    } else {
        p = self.entryHead();
        while (p[0] != END and i > 0) : (i -= 1) {
            p += rawEntryLength(p);
        }
    }
    return if (p[0] == END or i > 0) null else p;
}

pub fn next(self: *const ZipList, p: [*]u8) ?[*]u8 {
    if (p[0] == END) return null;
    if (p == self.entryTail()) {
        return null;
    }
    return p + rawEntryLength(p);
}

pub fn prev(self: *const ZipList, p: [*]u8) ?[*]u8 {
    if (p[0] == END) {
        const tail = self.entryTail();
        if (tail[0] == END) {
            return null;
        }
        return tail;
    }
    if (p == self.entryHead()) {
        return null;
    }
    const prevlen = loadPrevEntryLength(p);
    std.debug.assert(prevlen.len > 0);
    return p - prevlen.len;
}

pub fn get(entry: [*]u8) ?union(enum) { num: i64, str: []u8 } {
    if (entry[0] == END) return null;
    const ent = Entry.decode(entry);
    if (isStr(ent.encoding)) {
        const start = ent.header_size;
        const end = start + ent.len;
        return .{ .str = ent.ptr[start..end] };
    }
    return .{
        .num = loadInteger(
            entry + ent.header_size,
            ent.encoding,
        ),
    };
}

pub fn eql(entry: [*]u8, str: []const u8) bool {
    if (entry[0] == END) return false;
    const ent = Entry.decode(entry);
    if (isStr(ent.encoding)) {
        if (ent.len == str.len) {
            const start = ent.header_size;
            const end = start + ent.len;
            return std.mem.eql(u8, entry[start..end], str);
        }
        return false;
    }
    var value: i64 = undefined;
    var encoding: u8 = undefined;
    if (tryEncoding(str, &value, &encoding)) {
        const zval = loadInteger(entry + ent.header_size, encoding);
        return value == zval;
    }
    return false;
}

/// Find pointer to the entry equal to the specified `str`.
/// Skip `skip` entries between every comparison.
///
/// The `skip` parameter is typically set to 1 when the ziplist stores
/// structured data as alternating key/value pairs:
/// - To search keys: start from position 0 with skip=1
/// - To search values: start from position 1 with skip=1
///
/// This allows efficient searching of only the relevant data type while
/// skipping over the alternating entries.
///
/// Returns null when the field could not be found.
pub fn find(entry: [*]u8, str: []const u8, skip: u32) ?[*]u8 {
    var p = entry;
    var skipcnt: u32 = 0;
    var value: i64 = 0;
    var encoding: u8 = 0;
    while (p[0] != END) {
        const ent = Entry.decode(p);
        const q = p + ent.prev_rawlen_size + ent.len_size;
        if (skipcnt == 0) {
            if (isStr(ent.encoding)) {
                if (str.len == ent.len and std.mem.eql(u8, q[0..ent.len], str)) {
                    return p;
                }
            } else {
                // Find out if the searched field can be encoded. Note that
                // we do it only the first time, once done encoding is set
                // to non-zero and value is set to the integer value.
                if (encoding == 0) {
                    if (!tryEncoding(str, &value, &encoding)) {
                        encoding = maxInt(u8);
                    }
                }
                if (encoding != maxInt(u8)) {
                    if (value == loadInteger(q, encoding)) {
                        return p;
                    }
                }
            }
            skipcnt = skip;
        } else {
            skipcnt -= 1;
        }
        p = q + ent.len;
    }
    return null;
}

pub inline fn blobLen(self: *const ZipList) u32 {
    return self.bytes.get();
}

pub fn push(
    self: *ZipList,
    allocator: Allocator,
    str: []const u8,
    where: enum { head, tail },
) Allocator.Error!*ZipList {
    if (where == .head) {
        return self.insert(allocator, self.entryHead(), str);
    }
    return self.insert(allocator, self.entryEnd(), str);
}

/// Insert item at ptr.
pub fn insert(
    self: *ZipList,
    allocator: Allocator,
    ptr: [*]u8,
    str: []const u8,
) Allocator.Error!*ZipList {
    var zl = self;

    const curlen: u32 = zl.bytes.get();
    var prevlen: u32 = 0;
    if (ptr[0] != END) {
        prevlen = (loadPrevEntryLength(ptr)).len;
    } else {
        const tail = zl.entryTail();
        if (tail[0] != END) {
            prevlen = rawEntryLength(tail);
        }
    }

    var value: i64 = undefined;
    var encoding: u8 = STR_06B;
    var reqlen: u32 = @intCast(str.len);
    if (tryEncoding(str, &value, &encoding)) {
        reqlen = intSize(encoding);
    }
    reqlen += storePrevEntryLength(null, prevlen);
    reqlen += storeEntryEncoding(null, encoding, @intCast(str.len));

    // When the insert position is not equal to the tail, we need to
    // make sure that the next entry can hold this entry's length in
    // its prevlen field.
    const nextdiff = if (ptr[0] != END) prevLenByteDiff(ptr, reqlen) else 0;

    // Store offset because a realloc may change the address of zl.
    var offset = ptr - zl.addr();
    zl = try zl.resize(
        allocator,
        @intCast(@as(i64, curlen + reqlen) + @as(i64, nextdiff)),
    );

    var p: [*]u8 = zl.addr() + offset;

    // Apply memory move when necessary and update tail offset.
    if (p[0] != END) {
        const size: u32 = @intCast(
            @as(i64, curlen) - @as(i64, @intCast(offset)) + @as(i64, nextdiff) - 1,
        );
        const dest = p + reqlen;
        const src = if (nextdiff > 0) p - @abs(nextdiff) else p + @abs(nextdiff);
        memmove(dest, src, size);

        // Encode this entry's raw length in the next entry.
        _ = storePrevEntryLength(p + reqlen, reqlen);

        // Update offset for tail.
        zl.tail.set(@intCast(@as(i64, zl.tail.get() + reqlen) + @as(i64, nextdiff)));
    } else {
        zl.tail.set(@intCast(p - zl.addr()));
    }

    // When nextdiff != 0, the raw length of the next entry has changed, so
    // we need to cascade the update throughout the ziplist
    if (nextdiff != 0) {
        offset = p - zl.addr();
        zl = try cascadeUpdate(zl, allocator, p + reqlen);
        p = zl.addr() + offset;
    }

    p += storePrevEntryLength(p, prevlen);
    p += storeEntryEncoding(p, encoding, @intCast(str.len));
    if (isStr(encoding)) {
        memcpy(p, str, str.len);
    } else {
        saveInteger(p, value, encoding);
    }
    zl.incrLength();

    return zl;
}

/// Delete a single entry from the ziplist, pointed to by ptr.*.
/// Also update ptr.* in place, to be able to iterate over the
/// ziplist, while deleting entries.
pub fn delete(
    self: *ZipList,
    allocator: Allocator,
    ptr: *[*]u8,
) Allocator.Error!*ZipList {
    const p = ptr.*;
    const offset = p - self.addr();
    const zl = try self.cascadeDelete(allocator, p, 1);
    ptr.* = zl.addr() + offset;
    return zl;
}

pub fn deleteRange(
    self: *ZipList,
    allocator: Allocator,
    idx: i32,
    num: u32,
) Allocator.Error!*ZipList {
    if (self.index(idx)) |ptr| {
        return try self.cascadeDelete(allocator, ptr, num);
    }
    return self;
}

/// Don't let ziplists grow over 1GB in any case, don't wanna risk overflow in
/// zlbytes.
const MAX_SAFETY_SIZE = 1 << 30; // 1GB
pub fn safeToAdd(
    self: ?*ZipList,
    add: usize,
) Allocator.Error!bool {
    const length: usize = if (self) |zl| {
        zl.blobLen();
    } else 0;
    return (length + add) <= MAX_SAFETY_SIZE;
}

pub fn free(self: *ZipList, allocator: Allocator) void {
    allocator.free(self.toBytes());
}

pub fn toBytes(self: *const ZipList) []align(@alignOf(ZipList)) u8 {
    return self.addr()[0..self.blobLen()];
}

/// Delete "num" entries, starting at "ptr". Returns pointer to the ziplist.
fn cascadeDelete(
    self: *ZipList,
    allocator: Allocator,
    ptr: [*]u8,
    num: u32,
) Allocator.Error!*ZipList {
    var p = ptr;
    var zl = self;
    const first = Entry.decode(p);

    var deleted: u32 = 0;
    var i: i8 = 0;
    while (p[0] != END and i < num) : (i += 1) {
        p += rawEntryLength(p);
        deleted += 1;
    }
    const totlen: u32 = @intCast(p - first.ptr);
    if (totlen > 0) {
        var nextdiff: i32 = 0;
        if (p[0] != END) {
            // Storing `prev_rawlen` in this entry may increase or decrease the
            // number of bytes required compare to the current `prev_rawlen`.
            // There always is room to store this, because it was previously
            // stored by an entry that is now being deleted.
            nextdiff = prevLenByteDiff(p, first.prev_rawlen);
            p = if (nextdiff > 0) p - @abs(nextdiff) else p + @abs(nextdiff);
            _ = storePrevEntryLength(p, first.prev_rawlen);

            // When the tail contains more than one entry, we need to take
            // "nextdiff" in account as well. Otherwise, a change in the
            // size of prevlen doesn't have an effect on the *tail* offset.
            zl.tail.set(zl.tail.get() - totlen);
            const tail = Entry.decode(p);
            if (p[tail.header_size + tail.len] != END) {
                var t = zl.tail.get();
                t = if (nextdiff > 0) t + @abs(nextdiff) else t - @abs(nextdiff);
                zl.tail.set(t);
            }

            // Move the tail to the first deleted position.
            const offset: u32 = @intCast(p - zl.addr());
            const size = zl.bytes.get() - offset - 1;
            memmove(first.ptr, p, size);
        } else {
            // The entire tail was deleted. No need to move memory.
            const offset = first.ptr - first.prev_rawlen - zl.addr();
            zl.tail.set(@intCast(offset));
        }

        const offset = first.ptr - zl.addr();
        var new_len = zl.bytes.get() - totlen;
        new_len = if (nextdiff > 0) new_len + @abs(nextdiff) else new_len - @abs(nextdiff);
        zl = try zl.resize(allocator, new_len);
        zl.decrLength(deleted);

        p = zl.addr() + offset;
        if (nextdiff != 0) {
            zl = try cascadeUpdate(zl, allocator, p);
        }
    }
    return zl;
}

/// When an entry is inserted, we need to set the prevlen field of the next
/// entry to equal the length of the inserted entry. It can occur that this
/// length cannot be encoded in 1 byte and the next entry needs to be grow
/// a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
/// because this only happens when an entry is already being inserted (which
/// causes a realloc and memmove). However, encoding the prevlen may require
/// that this entry is grown as well. This effect may cascade throughout
/// the ziplist when there are consecutive entries with a size close to
/// BIG_PREVLEN, so we need to check that the prevlen can be encoded in
/// every consecutive entry.
///
/// Note that this effect can also happen in reverse, where the bytes required
/// to encode the prevlen field can shrink. This effect is deliberately ignored,
/// because it can cause a "flapping" effect where a chain prevlen fields is
/// first grown and then shrunk again after consecutive inserts. Rather, the
/// field is allowed to stay larger than necessary, because a large prevlen
/// field implies the ziplist is holding large entries anyway.
///
/// The pointer "ptr" points to the first entry that does NOT need to be
/// updated, i.e. consecutive fields MAY need an update.
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

        const next_entry = Entry.decode(p + cur_rawlen);
        // Abort when "prevlen" has not changed.
        if (next_entry.prev_rawlen == cur_rawlen) break;

        const cur_rawlen_size = storePrevEntryLength(null, cur_rawlen);
        if (next_entry.prev_rawlen_size < cur_rawlen_size) {
            const offset = p - zl.addr();
            const extra = cur_rawlen_size - next_entry.prev_rawlen_size;
            zl = try zl.resize(allocator, curlen + extra);
            p = zl.addr() + offset;

            // Next element.
            const np = p + cur_rawlen;
            const noffset: u32 = @intCast(np - zl.addr());

            // Update tail offset when next element is not the tail element.
            const tail: [*]u8 = zl.addr() + zl.tail.get();
            if (tail != np) {
                zl.tail.set(zl.tail.get() + extra);
            }

            // Move the tail to the back.
            memmove(
                np + cur_rawlen_size,
                np + next_entry.prev_rawlen_size,
                curlen - noffset - next_entry.prev_rawlen_size - 1,
            );
            _ = storePrevEntryLength(np, cur_rawlen);

            p += cur_rawlen;
            curlen += extra;
        } else {
            // Next element.
            const np = p + cur_rawlen;
            if (next_entry.prev_rawlen_size > cur_rawlen_size) {
                // This would result in shrinking, which we want to avoid.
                // So, set "cur_rawlen" in the available bytes.
                storePrevEntryLengthLarge(np, cur_rawlen);
            } else {
                _ = storePrevEntryLength(np, cur_rawlen);
            }
            break;
        }
    }
    return zl;
}

fn incrLength(self: *ZipList) void {
    var curlen = self.len.get();
    if (curlen < maxInt(u16)) {
        curlen += 1;
        self.len.set(curlen);
    }
}

fn decrLength(self: *ZipList, decr: u32) void {
    var curlen = self.len.get();
    if (curlen < maxInt(u16)) {
        curlen -= @intCast(decr);
        self.len.set(curlen);
    }
}

fn saveInteger(p: [*]u8, value: i64, encoding: u8) void {
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

fn loadInteger(p: [*]u8, encoding: u8) i64 {
    return switch (encoding) {
        INT_08B => p[0],
        INT_16B => readInt(i16, p[0..2], .little),
        INT_24B => readInt(i24, p[0..3], .little),
        INT_32B => readInt(i32, p[0..4], .little),
        INT_64B => readInt(i64, p[0..8], .little),
        INT_IMM_MIN...INT_IMM_MAX => blk: {
            break :blk (encoding & INT_IMM_MSK) - 1;
        },
        else => unreachable,
    };
}

fn resize(
    self: *ZipList,
    allocator: Allocator,
    len: u32,
) Allocator.Error!*ZipList {
    const old_mem = self.toBytes();
    const new_mem = try allocator.realloc(old_mem, len);
    const zl: *ZipList = @ptrCast(@alignCast(new_mem));
    zl.bytes.set(len);
    new_mem[len - 1] = END;
    return zl;
}

inline fn entryHead(self: *const ZipList) [*]u8 {
    return self.addr() + HEADER_SIZE;
}

inline fn entryTail(self: *const ZipList) [*]u8 {
    return self.addr() + self.tail.get();
}

inline fn entryEnd(self: *const ZipList) [*]u8 {
    return self.addr() + self.bytes.get() - 1;
}

inline fn addr(self: *const ZipList) [*]align(@alignOf(ZipList)) u8 {
    return @ptrCast(@alignCast(@constCast(self)));
}

fn storePrevEntryLength(ptr: ?[*]u8, len: u32) u8 {
    if (ptr) |p| {
        if (len < BIG_PREVLEN) {
            p[0] = @intCast(len);
            return 1;
        }
        storePrevEntryLengthLarge(p, len);
        return 5;
    }
    return if (len < BIG_PREVLEN) 1 else 5;
}

fn storePrevEntryLengthLarge(ptr: [*]u8, len: u32) void {
    ptr[0] = BIG_PREVLEN;
    writeInt(u32, ptr[1..5], len, .little);
}

fn loadPrevEntryLength(entry: [*]u8) struct { size: u8, len: u32 } {
    if (entry[0] < BIG_PREVLEN) {
        return .{ .size = 1, .len = entry[0] };
    }
    return .{
        .size = 5,
        .len = readInt(u32, entry[1..5], .little),
    };
}

/// Write the encoidng header of the entry in `ptr`. If ptr is NULL,
/// it just returns the amount of bytes required to encode such a length.
///
/// Arguments:
///
/// `encoding` is the encoding we are using for the entry. It could be
/// ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
/// for single-byte small immediate integers.
///
/// `len` is only used for ZIP_STR_* encodings and is the length of the
/// srting that this entry represents.
///
/// The function returns the number of bytes used by the encoding/length
/// header stored in `ptr`.
fn storeEntryEncoding(ptr: ?[*]u8, encoding: u8, len: u32) u8 {
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
            buf[1] = @truncate(len);
        } else {
            len_size += 4;
            if (ptr == null) return len_size;
            buf[0] = STR_32B;
            buf[1] = @truncate(len >> 24);
            buf[2] = @truncate(len >> 16);
            buf[3] = @truncate(len >> 8);
            buf[4] = @truncate(len);
        }
    } else {
        // Integer encoding.
        if (ptr == null) return len_size;
        buf[0] = encoding;
    }
    memcpy(ptr.?, buf, len_size);
    return len_size;
}

fn loadEntryEncoding(entry: [*]u8) struct { encoding: u8, size: u8, len: u32 } {
    var encoding = entry[0];
    if (encoding < STR_MSK) {
        encoding &= STR_MSK;
    }

    var len_size: u8 = 0;
    var len: u32 = 0;
    if (encoding < STR_MSK) {
        switch (encoding) {
            STR_06B => {
                len_size = 1;
                len = entry[0] & 0b00111111;
            },
            STR_14B => {
                len_size = 2;
                len = (@as(u16, (entry[0] & 0b00111111)) << 8) | @as(u16, entry[1]);
            },
            STR_32B => {
                len_size = 5;
                len = readInt(u32, entry[1..5], .big);
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

/// Check if string pointed to by `str` can be encoded as an integer.
/// Stores the integer value in `out_int` and its encoding in `out_encoding`.
fn tryEncoding(str: []const u8, value: *i64, encoding: *u8) bool {
    if (str.len >= 32 or str.len == 0) return false;
    const val = std.fmt.parseInt(i64, str, 10) catch {
        return false;
    };

    value.* = val;
    if (val >= 0 and val <= 12) {
        encoding.* = INT_IMM_MIN + @as(u8, @intCast(val));
        return true;
    }
    if (val >= minInt(i8) and val <= maxInt(i8)) {
        encoding.* = INT_08B;
        return true;
    }
    if (val >= minInt(i16) and val <= maxInt(i16)) {
        encoding.* = INT_16B;
        return true;
    }
    if (val >= maxInt(i24) and val <= maxInt(i24)) {
        encoding.* = INT_24B;
        return true;
    }
    if (val >= maxInt(i32) and val <= maxInt(i32)) {
        encoding.* = INT_32B;
        return true;
    }
    encoding.* = INT_64B;
    return true;
}

/// Return the difference in number of bytes needed to store the length of the
/// previous element `len`, in the entry pointed to by `ptr`.
fn prevLenByteDiff(ptr: [*]u8, prev_len: u32) i8 {
    const prev_len_size: i8 = @intCast(storePrevEntryLength(null, prev_len));
    const ptr_prev_len_size: i8 = @intCast(loadPrevEntryLength(ptr).size);
    return prev_len_size - ptr_prev_len_size;
}

/// Return the total number of bytes used by the entry pointed to by `ptr`.
fn rawEntryLength(ptr: [*]u8) u32 {
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
        else => blk: {
            if (encoding >= INT_IMM_MIN and encoding <= INT_IMM_MAX) {
                break :blk 0;
            }
            panic("Invalid integer encoding: 0x{X:0>2}\n", .{encoding});
        },
    };
}

inline fn isStr(encoding: u8) bool {
    return encoding & STR_MSK < STR_MSK;
}

test ZipList {
    const allocator = testing.allocator;
    var zl = try new(allocator);
    defer zl.free(allocator);

    {
        const bytes = zl.bytes.get();
        try expectEqual(HEADER_SIZE + 1, bytes);
        try expectEqual(HEADER_SIZE, zl.tail.get());
        try expectEqual(0, zl.numOfEntries());
        try expectEqual(END, zl.toBytes()[bytes - 1]);
    }

    const hello = "hello";
    {
        zl = try zl.push(allocator, hello, .head);
        try expectEqual(1, zl.numOfEntries());
        try expectEqual(HEADER_SIZE + 1 + 1 + hello.len + 1, zl.bytes.get());
        try expectEqual(HEADER_SIZE, zl.tail.get());
    }

    {
        zl = try zl.push(allocator, hello, .head);
        try expectEqual(2, zl.numOfEntries());

        zl = try zl.push(allocator, hello, .tail);
        try expectEqual(3, zl.numOfEntries());

        const large_str: [255]u8 = @splat(0);
        zl = try zl.push(allocator, &large_str, .head);
        try expectEqual(4, zl.numOfEntries());
    }

    {
        for (0..maxInt(u16)) |_| {
            zl = try zl.push(allocator, hello, .tail);
        }
        try expectEqual(@as(u32, maxInt(u16)) + 4, zl.numOfEntries());
        try expectEqual(maxInt(u16), zl.len.get());
        const large: [255]u8 = @splat('a');
        zl = try zl.push(allocator, &large, .head);
        try expectEqual(maxInt(u16) + 5, zl.numOfEntries());
        const head = zl.entryHead();
        try expect(eql(head, &large));
    }

    {
        zl = try zl.push(allocator, "4294967295", .tail);
        const head = zl.entryHead();
        const found = find(head, "4294967295", 0);
        try expect(found != null);
    }

    {
        zl = try zl.push(allocator, "4294967295", .head);
        const ptr = zl.index(0).?;
        const ret = get(ptr);
        try expect(ret != null);
        try expect(ret.?.num == 4294967295);
        try expect(eql(ptr, "4294967295"));
    }

    {
        zl = try zl.push(allocator, "first", .head);
        const first = zl.index(0);
        try expect(first != null);
        try expectEqualStrings(
            "first",
            get(first.?).?.str,
        );
        try expect(eql(first.?, "first"));
    }

    {
        zl = try zl.push(allocator, "last", .tail);
        const last = zl.index(-1);
        try expect(last != null);
        try expectEqualStrings(
            "last",
            get(last.?).?.str,
        );
    }

    {
        var next_ptr = zl.next(zl.entryTail());
        try expectEqual(null, next_ptr);

        next_ptr = zl.next(zl.entryHead());
        try expect(next_ptr != null);

        next_ptr = zl.next(zl.entryEnd());
        try expectEqual(null, next_ptr);
    }

    {
        var prev_ptr = zl.prev(zl.entryEnd());
        try expect(prev_ptr != null);
        try expectEqual(zl.entryTail(), prev_ptr.?);

        prev_ptr = zl.prev(zl.entryHead());
        try expectEqual(null, prev_ptr);

        prev_ptr = zl.prev(zl.entryTail());
        try expect(prev_ptr != null);
    }

    {
        var entry = zl.index(3).?;
        const len = zl.numOfEntries();
        zl = try zl.delete(allocator, &entry);
        try expectEqual(len - 1, zl.numOfEntries());

        entry = zl.entryHead();
        zl = try zl.delete(allocator, &entry);
        try expectEqual(len - 2, zl.numOfEntries());

        entry = zl.entryTail();
        zl = try zl.delete(allocator, &entry);
        try expectEqual(len - 3, zl.numOfEntries());

        zl = try zl.deleteRange(allocator, 3, 4);
        try expectEqual(len - 7, zl.numOfEntries());
    }

    {
        var first: ?*ZipList = try new(allocator);
        first = try first.?.push(allocator, "first-1", .tail);

        var second: ?*ZipList = try new(allocator);
        second = try second.?.push(allocator, "second-1", .tail);
        second = try second.?.push(allocator, "second-2", .tail);

        const target = try merge(allocator, &first, &second);
        try expect(target != null);
        try expect(first == null);
        try expect(second != null);
        try expect(second.? == target.?);
        try expectEqual(3, target.?.numOfEntries());
        try expect(eql(target.?.index(0).?, "first-1"));
        try expect(eql(target.?.index(1).?, "second-1"));
        try expect(eql(target.?.index(2).?, "second-2"));
        second.?.free(allocator);
    }
}

const std = @import("std");
const Allocator = std.mem.Allocator;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
const testing = std.testing;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const expect = testing.expect;
const LittleEndian = @import("endian.zig").LittleEndian;
const minInt = std.math.minInt;
const maxInt = std.math.maxInt;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const panic = std.debug.panic;
const assert = std.debug.assert;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const memmove = memzig.memmove;
