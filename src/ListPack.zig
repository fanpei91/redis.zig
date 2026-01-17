const HDR_SIZE = 6; // 32 bit total len + 16 bit number of elements.
const HDR_NUMELE_UNKNOWN = std.math.maxInt(u16);
const MAX_INT_ENCODING_LEN = 9;
const MAX_BACKLEN_SIZE = 5;
const ENCODING_INT = 0;
const ENCODING_STRING = 1;
const EOF = 0xff;

const ENCODING_7BIT_UINT = 0b00000000;
const ENCODING_7BIT_UINT_MASK = 0b10000000;
inline fn encodingIs7BitUint(byte: u8) bool {
    return ENCODING_7BIT_UINT_MASK & byte == ENCODING_7BIT_UINT;
}

const ENCODING_6BIT_STR = 0b10000000;
const ENCODING_6BIT_STR_MASK = 0b11000000;
inline fn encodingIs6BitStr(byte: u8) bool {
    return ENCODING_6BIT_STR_MASK & byte == ENCODING_6BIT_STR;
}

const ENCODING_13BIT_INT = 0b11000000;
const ENCODING_13BIT_INT_MASK = 0b11100000;
inline fn encodingIs13BitInt(byte: u8) bool {
    return ENCODING_13BIT_INT_MASK & byte == ENCODING_13BIT_INT;
}

const ENCODING_12BIT_STR = 0b11100000;
const ENCODING_12BIT_STR_MASK = 0b11110000;
inline fn encodingIs12BitStr(byte: u8) bool {
    return ENCODING_12BIT_STR_MASK & byte == ENCODING_12BIT_STR;
}

const ENCODING_16BIT_INT = 0b11110001;
const ENCODING_16BIT_INT_MASK = 0b11111111;
inline fn encodingIs16BitInt(byte: u8) bool {
    return ENCODING_16BIT_INT_MASK & byte == ENCODING_16BIT_INT;
}

const ENCODING_24BIT_INT = 0b11110010;
const ENCODING_24BIT_INT_MASK = 0b11111111;
inline fn encodingIs24BitInt(byte: u8) bool {
    return ENCODING_24BIT_INT_MASK & byte == ENCODING_24BIT_INT;
}

const ENCODING_32BIT_INT = 0b11110011;
const ENCODING_32BIT_INT_MASK = 0b11111111;
inline fn encodingIs32BitInt(byte: u8) bool {
    return ENCODING_32BIT_INT_MASK & byte == ENCODING_32BIT_INT;
}

const ENCODING_64BIT_INT = 0b11110100;
const ENCODING_64BIT_INT_MASK = 0b11111111;
inline fn encodingIs64BitInt(byte: u8) bool {
    return ENCODING_64BIT_INT_MASK & byte == ENCODING_64BIT_INT;
}

const ENCODING_32BIT_STR = 0b11110000;
const ENCODING_32BIT_STR_MASK = 0b11111111;
inline fn encodingIs32BitStr(byte: u8) bool {
    return ENCODING_32BIT_STR_MASK & byte == ENCODING_32BIT_STR;
}

/// 20 digits of -2^63 + 1 null term = 21.
pub const INTBUF_SIZE = 21;

pub const Where = enum {
    before,
    after,
    replace,
};

const ListPack = @This();
bytes: LittleEndian(u32, 1),
num: LittleEndian(u16, 1),
// EOF: 0xff

/// Create a new, empty listpack.
pub fn new() *ListPack {
    const bytes = allocator.alignedAlloc(u8, .of(ListPack), HDR_SIZE + 1);
    const lp: *ListPack = @ptrCast(@alignCast(bytes));
    lp.bytes.set(HDR_SIZE + 1);
    lp.num.set(0);
    bytes[HDR_SIZE] = EOF;
    return lp;
}

pub fn cast(ptr: *anyopaque) *ListPack {
    return @ptrCast(@alignCast(ptr));
}

/// Insert, delete or replace the specified element 'ele' at the specified
/// position 'p', with 'p' being a listpack element pointer obtained with
/// first(), last(), index(), next(), prev() or seek().
///
/// The element is inserted before, after, or replaces the element pointed
/// by 'p' depending on the 'where' argument, that can be .before, .after
/// or .replace.
///
/// If 'ele' is set to NULL, the function removes the element pointed by 'p'
/// instead of inserting one.
///
/// Returns NULL on out of memory or when the listpack total length would exceed
/// the max allowed size of 2^32-1, otherwise the new pointer to the ListPack
/// holding the new element is returned (and the old pointer passed is no longer
/// considered valid)
///
/// If 'newp' is not NULL, at the end of a successful call '*newp' will be set
/// to the address of the element just added, so that it will be possible to
/// continue an interation with next() and prev().
///
/// For deletion operations ('ele' set to NULL) 'newp' is set to the next
/// element, on the right of the deleted one, or to NULL if the deleted element
/// was the last one.
pub fn insert(
    self: *ListPack,
    ele: ?[]const u8,
    p: [*]u8,
    where: Where,
    newp: ?*?[*]u8,
) ?*ListPack {
    var lp = self;
    var ptr = p;
    var nwhere = where;
    var intenc: [MAX_INT_ENCODING_LEN]u8 = undefined;
    var backlen: [MAX_BACKLEN_SIZE]u8 = undefined;

    // The length of the encoded element.
    var enclen: u64 = undefined;

    // An element pointer set to NULL means deletion, which is conceptually
    // replacing the element with a zero-length element. So whatever we
    // get passed as 'where', set it to .replace.
    if (ele == null) {
        nwhere = .replace;
    }

    // If we need to insert after the current element, we just jump to the
    // next element (that could be the EOF one) and handle the case of
    // inserting before. So the function will actually deal with just two
    // cases: .before and .replace.
    if (nwhere == .after) {
        ptr = skip(ptr);
        nwhere = .before;
    }

    // Store the offset of the element 'ptr', so that we can obtain its
    // address again after a reallocation.
    const offset = ptr - lp.addr();

    // Calling encodeGetType() results into the encoded version of the
    // element to be stored into 'intenc' in case it is representable as
    // an integer: in that case, the function returns ENCODING_INT.
    // Otherwise if ENCODING_STRING is returned, we'll have to call
    // encodeString() to actually write the encoded string on place later.
    //
    // Whatever the returned encoding is, 'enclen' is populated with the
    // length of the encoded element.
    const enctype = blk: {
        if (ele) |e| {
            break :blk encodeGetType(e, &intenc, &enclen);
        }
        enclen = 0;
        break :blk -1;
    };

    // We need to also encode the backward-parsable length of the element
    // and append it to the end: this allows to traverse the listpack from
    // the end to the start.
    const backlen_size: u64 = if (ele != null) encodeBacklen(
        &backlen,
        enclen,
    ) else 0;
    const old_listpack_bytes: u64 = lp.bytes.get();
    var replaced_len: u32 = 0;
    if (nwhere == .replace) {
        replaced_len = currentEncodedSize(ptr);
        replaced_len += encodeBacklen(null, replaced_len);
    }

    const new_listpack_bytes = old_listpack_bytes + enclen + backlen_size -
        replaced_len;
    if (new_listpack_bytes > std.math.maxInt(u32)) {
        return null;
    }

    // We now need to reallocate in order to make space or shrink the
    // allocation (in case 'where' value is .replace and the new element is
    // smaller). However we do that before memmoving the memory to
    // make room for the new element if the final allocation will get
    // larger, or we do it after if the final allocation will get smaller.

    var dst = lp.addr() + offset; // May be updated after reallocation.

    // Realloc before: we need more room.
    if (new_listpack_bytes > old_listpack_bytes) {
        const new_memory = allocator.impl.realloc(
            lp.asBytes(),
            new_listpack_bytes,
        ) catch {
            return null;
        };
        lp = @ptrCast(@alignCast(new_memory));
        dst = lp.addr() + offset;
    }

    // Setup the listpack relocating the elements to make the exact room
    // we need to store the new one.
    if (nwhere == .before) {
        memmove(dst + enclen + backlen_size, dst, old_listpack_bytes - offset);
    } else { // .replace
        const lendiff = @as(i64, @intCast(enclen + backlen_size)) - replaced_len;
        var dest = dst + replaced_len;
        dest = if (lendiff < 0) dest - @abs(lendiff) else dest + @abs(lendiff);
        memmove(
            dest,
            dst + replaced_len,
            old_listpack_bytes - offset - replaced_len,
        );
    }

    // Realloc after: we need to free space.
    if (new_listpack_bytes < old_listpack_bytes) {
        const new_memory = allocator.impl.realloc(
            lp.asBytes(),
            new_listpack_bytes,
        ) catch {
            return null;
        };
        lp = @ptrCast(@alignCast(new_memory));
        dst = lp.addr() + offset;
    }

    // Store the entry.
    if (newp) |np| {
        np.* = dst;
        // In case of deletion, set 'newp' to NULL if the next element is
        // the EOF element.
        if (ele == null and dst[0] == EOF) np.* = null;
    }
    if (ele) |el| {
        if (enctype == ENCODING_INT) {
            memcpy(dst, &intenc, enclen);
        } else {
            encodeString(dst, el);
        }
        dst += enclen;
        memcpy(dst, &backlen, backlen_size);
        dst += backlen_size;
    }

    // Update header
    if (nwhere != .replace or ele == null) {
        const num_elements = lp.num.get();
        if (num_elements != HDR_NUMELE_UNKNOWN) {
            if (ele != null) {
                lp.num.set(num_elements + 1);
            } else {
                lp.num.set(num_elements - 1);
            }
        }
    }
    lp.bytes.set(@intCast(new_listpack_bytes));
    return lp;
}

/// Append the specified element 'ele' at the end of the listpack. It is
/// implemented in terms of insert(), so the return value is the same as
/// insert().
pub fn append(self: *ListPack, ele: []const u8) ?*ListPack {
    return self.insert(
        ele,
        self.addr() + self.bytes.get() - 1,
        .before,
        null,
    );
}

/// This is just a wrapper for pppend() to directly use a 64 bit integer
/// instead of a string.
pub fn appendInteger(self: *ListPack, value: i64) ?*ListPack {
    var buf: [32]u8 = undefined;
    const str = util.ll2string(&buf, value);
    return self.append(str);
}

/// This is just a wrapper for insert() to directly use a 64 bit integer
/// instead of a string to replace the current element. The function returns
/// the new listpack as return value, and also updates the current cursor
/// by updating '*pos'.
pub fn replaceInteger(self: *ListPack, pos: *?[*]u8, value: i64) ?*ListPack {
    var buf: [32]u8 = undefined;
    const str = util.ll2string(&buf, value);
    return self.insert(str, pos.*.?, .replace, pos);
}

/// Remove the element pointed by 'p', and return the resulting listpack.
/// If 'newp' is not NULL, the next element pointer (to the right of the
/// deleted one) is returned by reference. If the deleted element was the
/// last one, '*newp' is set to NULL.
pub fn delete(self: *ListPack, p: [*]u8, newp: ?*?[*]u8) ?*ListPack {
    return self.insert(null, p, .replace, newp);
}

/// Return the number of elements inside the listpack. This function attempts
/// to use the cached value when within range, otherwise a full scan is
/// needed. As a side effect of calling this function, the listpack header
/// could be modified, because if the count is found to be already within
/// the 'num' header field range, the new value is set.
pub fn length(self: *ListPack) u32 {
    const num = self.num.get();
    if (num != HDR_NUMELE_UNKNOWN) {
        return num;
    }

    // Too many elements inside the listpack. We need to scan in order
    // to get the total number.
    var count: u32 = 0;
    var ptr = self.first();
    while (ptr) |p| {
        count += 1;
        ptr = self.next(p);
    }

    // If the count is again within range of the header numele field,
    // set it.
    if (count < HDR_NUMELE_UNKNOWN) {
        self.num.set(count);
    }
    return count;
}

/// Return a pointer to the first element of the listpack, or NULL if the
/// listpack has no elements.
pub fn first(self: *ListPack) ?[*]u8 {
    var ptr = self.addr();
    ptr += HDR_SIZE; // Skip the header.
    if (ptr[0] == EOF) {
        return null;
    }
    return ptr;
}

/// Return a pointer to the last element of the listpack, or NULL if the
/// listpack has no elements.
pub fn last(self: *ListPack) ?[*]u8 {
    // Seek EOF element.
    const eof = self.addr() + self.bytes.get() - 1;
    // Will return NULL if EOF is the only element.
    return self.prev(eof);
}

/// If 'p' points to an element of the listpack, calling next() will return
/// the pointer to the next element (the one on the right), or NULL if 'p'
/// already pointed to the last element of the listpack.
pub fn next(self: *ListPack, p: [*]u8) ?[*]u8 {
    _ = self; // self is not used for now. However prev() uses it.
    const n = skip(p);
    if (n[0] == EOF) {
        return null;
    }
    return n;
}

/// If 'p' points to an element of the listpack, calling prev() will return
/// the pointer to the preivous element (the one on the left), or NULL if 'p'
/// already pointed to the first element of the listpack.
pub fn prev(self: *ListPack, p: [*]u8) ?[*]u8 {
    if (p - self.addr() == HDR_SIZE) {
        return null;
    }
    // Seek the first backlen byte of the last element.
    const ptr = p - 1;
    var prevlen = decodeBacklen(ptr);
    prevlen += encodeBacklen(null, prevlen);
    // Seek the first byte of the previous entry.
    return ptr - prevlen + 1;
}

/// Seek the specified element and returns the pointer to the seeked element.
/// Positive indexes specify the zero-based element to seek from the head to
/// the tail, negative indexes specify elements starting from the tail, where
/// -1 means the last element, -2 the penultimate and so forth. If the index
/// is out of range, NULL is returned.
pub fn seek(self: *ListPack, index: i32) ?[*]u8 {
    // Seek forward by default.
    var forward = true;
    var idx = index;

    // We want to seek from left to right or the other way around
    // depending on the listpack length and the element position.
    // However if the listpack length cannot be obtained in constant time,
    // we always seek from left to right.
    const numele = self.num.get();
    if (numele != HDR_NUMELE_UNKNOWN) {
        if (idx < 0) idx = @as(i32, @intCast(numele)) + idx;
        if (idx < 0) return null; // Index still < 0 means out of range.
        if (idx >= numele) return null; // Out of range the other side.
        //  We want to scan right-to-left if the element we are looking for
        // is past the half of the listpack.
        if (idx > @divFloor(numele, 2)) {
            forward = false;
            // Right-to-left scanning always expects a negative index. Convert
            // our index to negative form.
            idx -= numele;
        }
    } else {
        // If the listpack length is unspecified, for negative indexes we
        // want to always scan right-to-left.
        if (idx < 0) forward = false;
    }

    // Forward and backward scanning is trivially based on next()/prev().
    if (forward) {
        var ele = self.first();
        while (idx > 0 and ele != null) {
            ele = self.next(ele.?);
            idx -= 1;
        }
        return ele;
    } else {
        var ele = self.last();
        while (idx < -1 and ele != null) {
            ele = self.prev(ele.?);
            idx += 1;
        }
        return ele;
    }
}

pub fn free(self: *ListPack) void {
    allocator.free(self.asBytes());
}

pub inline fn addr(self: *ListPack) [*]align(@alignOf(ListPack)) u8 {
    return @ptrCast(@alignCast(self));
}

/// Return the listpack element pointed by 'p'.
///
/// The function changes behavior depending on the passed 'intbuf' value.
/// Specifically, if 'intbuf' is NULL:
///
/// If the element is internally encoded as an integer, the function returns
/// NULL and populates the integer value by reference in 'count'. Otherwise if
/// the element is encoded as a string a pointer to the string (pointing inside
/// the listpack itself) is returned, and 'count' is set to the length of the
/// string.
///
/// If instead 'intbuf' points to a buffer passed by the caller, that must be
/// at least LP_INTBUF_SIZE bytes, the function always returns the element as
/// it was a string (returning the pointer to the string and setting the
/// 'count' argument to the string length by reference). However if the element
/// is encoded as an integer, the 'intbuf' buffer is used in order to store
/// the string representation.
///
/// The user should use one or the other form depending on what the value will
/// be used for. If there is immediate usage for an integer value returned
/// by the function, than to pass a buffer (and convert it back to a number)
/// is of course useless.
///
/// Similarly, there is no error returned since the listpack normally can be
/// assumed to be valid, so that would be a very high API cost.
pub fn get(p: [*]u8, count: *i64, intbuf: ?[]u8) ?[*]u8 {
    var val: i64 = undefined;
    if (encodingIs6BitStr(p[0])) {
        count.* = encoding6BitStrLen(p);
        return p + 1;
    } else if (encodingIs12BitStr(p[0])) {
        count.* = encoding12BitStrLen(p);
        return p + 2;
    } else if (encodingIs32BitStr(p[0])) {
        count.* = encoding32BitStrLen(p);
        return p + 5;
    }
    if (encodingIs7BitUint(p[0])) {
        val = p[0] & 0b01111111;
    } else if (encodingIs13BitInt(p[0])) {
        val = @as(i13, @bitCast((@as(u13, p[0] & 0b00011111) << 8) | p[1]));
    } else if (encodingIs16BitInt(p[0])) {
        val = readInt(i16, p[1..3], .little);
    } else if (encodingIs24BitInt(p[0])) {
        val = readInt(i24, p[1..4], .little);
    } else if (encodingIs32BitInt(p[0])) {
        val = readInt(i32, p[1..5], .little);
    } else if (encodingIs64BitInt(p[0])) {
        val = readInt(i64, p[1..9], .little);
    } else {
        @panic("unexpected encoding");
    }
    // Return the string representation of the integer or the value itself
    // depending on intbuf being NULL or not.
    if (intbuf) |buf| {
        const f = std.fmt.bufPrint(buf, "{}", .{val}) catch unreachable;
        count.* = @intCast(f.len);
        return buf.ptr;
    }
    count.* = val;
    return null;
}

/// This is a wrapper function for get() to directly get an integer value
/// from the listpack (that may store numbers as a string), converting
/// the string if needed.
pub fn getInteger(ele: [*]u8) i64 {
    var v: i64 = undefined;
    const e = get(ele, &v, null) orelse {
        return v;
    };
    // The following code path should never be used for how listpacks work:
    // they should always be able to store an i64 value in integer
    // encoded form. However the implementation may change.
    return util.string2ll(e[0..@intCast(v)]).?;
}

pub inline fn asBytes(self: *ListPack) []align(@alignOf(ListPack)) u8 {
    return self.addr()[0..self.bytes.get()];
}

/// Skip the current entry returning the next. It is invalid to call this
/// function if the current element is the EOF element at the end of the
/// listpack, however, while this function is used to implement next(),
/// it does not return NULL when the EOF element is encountered.
fn skip(p: [*]u8) [*]u8 {
    var entrylen = currentEncodedSize(p);
    entrylen += encodeBacklen(null, entrylen);
    return p + entrylen;
}

/// Return the encoded length of the listpack element pointed by 'p'. If the
/// element encoding is wrong then 0 is returned.
fn currentEncodedSize(p: [*]u8) u32 {
    const byte = p[0];
    if (encodingIs7BitUint(byte)) return 1;
    if (encodingIs6BitStr(byte)) return 1 + encoding6BitStrLen(p);
    if (encodingIs13BitInt(byte)) return 2;
    if (encodingIs16BitInt(byte)) return 3;
    if (encodingIs24BitInt(byte)) return 4;
    if (encodingIs32BitInt(byte)) return 5;
    if (encodingIs64BitInt(byte)) return 9;
    if (encodingIs12BitStr(byte)) return 2 + encoding12BitStrLen(p);
    if (encodingIs32BitStr(byte)) return 5 + encoding32BitStrLen(p);
    return 0;
}

/// Store a reverse-encoded variable length field, representing the length
/// of the previous element of size 'l', in the target buffer 'buf'.
/// The function returns the number of bytes used to encode it, from
/// 1 to 5. If 'buf' is NULL the function just returns the number of bytes
/// needed in order to encode the backlen.
fn encodeBacklen(buf: ?[*]u8, l: u64) u32 {
    if (l <= 0b01111111) {
        if (buf) |b| b[0] = @intCast(l);
        return 1;
    }
    if (l <= 0b00111111_11111111) {
        if (buf) |b| {
            b[0] = @intCast(l >> 7);
            b[1] = @intCast((l & 0b01111111) | 0b10000000);
        }
        return 2;
    }
    if (l <= 0b00011111_11111111_11111111) {
        if (buf) |b| {
            b[0] = @intCast(l >> 14);
            b[1] = @intCast(((l >> 7) & 0b01111111) | 0b10000000);
            b[2] = @intCast((l & 0b01111111) | 0b10000000);
        }
        return 3;
    }
    if (l <= 0b00001111_11111111_11111111_11111111) {
        if (buf) |b| {
            b[0] = @intCast(l >> 21);
            b[1] = @intCast(((l >> 14) & 0b01111111) | 0b10000000);
            b[2] = @intCast(((l >> 7) & 0b01111111) | 0b10000000);
            b[3] = @intCast((l & 0b01111111) | 0b10000000);
        }
        return 4;
    }
    if (buf) |b| {
        b[0] = @intCast(l >> 28);
        b[1] = @intCast(((l >> 21) & 0b01111111) | 0b10000000);
        b[2] = @intCast(((l >> 14) & 0b01111111) | 0b10000000);
        b[3] = @intCast(((l >> 7) & 0b01111111) | 0b10000000);
        b[4] = @intCast((l & 0b01111111) | 0b10000000);
    }
    return 5;
}

/// Decode the backlen and returns it. If the encoding looks invalid (more than
/// 5 bytes are used), UINT64_MAX is returned to report the problem.
fn decodeBacklen(p: [*]u8) u64 {
    var val: u64 = 0;
    var shift: u6 = 0;
    var ptr = p;
    while (true) {
        val |= @as(u64, ptr[0] & 0b0111111) << shift;
        if (ptr[0] & 0b10000000 == 0) break;
        shift += 7;
        ptr -= 1;
        if (shift > 28) return std.math.maxInt(u64);
    }
    return val;
}

inline fn encoding6BitStrLen(p: [*]u8) u32 {
    return 0b00111111 & p[0];
}

inline fn encoding12BitStrLen(p: [*]u8) u32 {
    return (@as(u32, 0b00001111 & p[0]) << 8) | @as(u32, p[1]);
}

inline fn encoding32BitStrLen(p: [*]u8) u32 {
    return readInt(u32, p[1..5], .little);
}

/// Given an element 'ele', determine if the element can be represented inside
/// the listpack encoded as integer, and returns ENCODING_INT if so. Otherwise
/// returns ENCODING_STRING if no integer encoding is possible.
///
/// If the ENCODING_INT is returned, the function stores the integer encoded
/// representation of the element in the 'intenc' buffer.
///
/// Regardless of the returned encoding, 'enclen' is populated by reference to
/// the number of bytes that the string or integer encoded element will require
/// in order to be represented.
fn encodeGetType(ele: []const u8, intenc: []u8, enclen: *u64) i32 {
    const v: i64 = std.fmt.parseInt(i64, ele, 10) catch {
        if (ele.len < 64) {
            // 6bit string
            enclen.* = 1 + ele.len;
        } else if (ele.len < 4096) {
            // 12bit string
            enclen.* = 2 + ele.len;
        } else {
            // 32bit string
            enclen.* = 5 + ele.len;
        }
        return ENCODING_STRING;
    };
    if (v >= 0 and v <= 127) {
        intenc[0] = @intCast(v);
        enclen.* = 1;
    } else if (v >= minInt(i13) and v <= maxInt(i13)) {
        const uv: u13 = @bitCast(@as(i13, @intCast(v)));
        intenc[0] = @intCast((uv >> 8) | ENCODING_13BIT_INT);
        intenc[1] = @intCast(uv & 0xff);
        enclen.* = 2;
    } else if (v >= minInt(i16) and v <= maxInt(i16)) {
        intenc[0] = ENCODING_16BIT_INT;
        writeInt(i16, intenc[1..3], @intCast(v), .little);
        enclen.* = 3;
    } else if (v >= minInt(i24) and v <= maxInt(i24)) {
        intenc[0] = ENCODING_24BIT_INT;
        writeInt(i24, intenc[1..4], @intCast(v), .little);
        enclen.* = 4;
    } else if (v >= minInt(i32) and v <= maxInt(i32)) {
        intenc[0] = ENCODING_32BIT_INT;
        writeInt(i32, intenc[1..5], @intCast(v), .little);
        enclen.* = 5;
    } else {
        intenc[0] = ENCODING_64BIT_INT;
        writeInt(i64, intenc[1..9], v, .little);
        enclen.* = 9;
    }
    return ENCODING_INT;
}

/// Encode the string element pointed by 's' in the target buffer 'buf'. The
/// function should be called with 'buf' having always enough space for encoding
/// the string. This is done by calling encodeGetType() before calling this
/// function.
fn encodeString(buf: [*]u8, s: []const u8) void {
    const len = s.len;
    if (len < 64) {
        // 6bit string
        buf[0] = @intCast(ENCODING_6BIT_STR | len);
        memcpy(buf + 1, s, len);
    } else if (len < 4096) {
        // 12bit string
        buf[0] = @intCast(ENCODING_12BIT_STR | (len >> 8));
        buf[1] = @intCast(len & 0xff);
        memcpy(buf + 2, s, len);
    } else {
        // 32bit string
        buf[0] = ENCODING_32BIT_STR;
        writeInt(u32, buf[1..5], @intCast(len), .little);
        memcpy(buf + 5, s, len);
    }
}

const std = @import("std");
const allocator = @import("allocator.zig");
const LittleEndian = @import("endian.zig").LittleEndian;
const memlib = @import("mem.zig");
const memmove = memlib.memmove;
const memcpy = memlib.memcpy;
const maxInt = std.math.maxInt;
const minInt = std.math.minInt;
const readInt = std.mem.readInt;
const writeInt = std.mem.writeInt;
const util = @import("util.zig");
