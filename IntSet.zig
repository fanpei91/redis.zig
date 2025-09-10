const ENC_INT16 = @sizeOf(i16);
const ENC_INT32 = @sizeOf(i32);
const ENC_INT64 = @sizeOf(i64);

encoding: u32,
length: u32,
numbers: [0]u8,

const IntSet = @This();
pub const Value = i64;

pub fn new(allocator: Allocator) Allocator.Error!*IntSet {
    const s = try allocator.create(IntSet);
    s.encoding = nativeToLittle(u32, ENC_INT16);
    s.length = 0;
    return s;
}

pub const Result = struct {
    set: *IntSet,
    success: bool,
};
pub fn add(
    s: *IntSet,
    allocator: Allocator,
    value: Value,
) Allocator.Error!Result {
    const enc = valueEncoding(value);
    if (enc > s.getEncoding()) {
        const ns = try s.upgradeAdd(allocator, value);
        return .{
            .set = ns,
            .success = true,
        };
    }

    const res = s.search(value);
    if (res.found) {
        return .{
            .set = s,
            .success = false,
        };
    }

    const cur_len = s.getLength();
    const new_length = cur_len + 1;
    const ns = try s.resize(allocator, new_length, null);
    const pos = res.pos;
    if (pos < cur_len) {
        ns.moveTail(pos, pos + 1);
    }

    ns.setAt(pos, value);
    return .{
        .set = ns,
        .success = true,
    };
}

pub fn remove(
    s: *IntSet,
    allocator: Allocator,
    value: Value,
) Allocator.Error!Result {
    const enc = valueEncoding(value);
    if (enc > s.getEncoding()) {
        return .{
            .set = s,
            .success = false,
        };
    }

    const res = s.search(value);
    if (!res.found) {
        return .{
            .set = s,
            .success = false,
        };
    }

    const len = s.getLength();
    const pos = res.pos;
    if (pos < len - 1) s.moveTail(pos + 1, pos);
    const ns = try s.resize(allocator, len - 1, null);
    return .{
        .set = ns,
        .success = true,
    };
}

pub fn find(s: *IntSet, value: Value) bool {
    const enc = valueEncoding(value);
    if (enc > s.getEncoding()) {
        return false;
    }
    const res = s.search(value);
    return res.found;
}

pub fn get(s: *IntSet, pos: u32) ?Value {
    if (pos < s.getLength()) {
        return s.getAt(pos);
    }
    return null;
}

pub fn getLength(s: *IntSet) u32 {
    return littleToNative(u32, s.length);
}

pub fn random(s: *IntSet) Value {
    const seed = std.time.microTimestamp();
    var prng = std.Random.DefaultPrng.init(@bitCast(seed));
    const rand = prng.random();
    const pos = rand.int(u32) % s.getLength();
    return s.getAt(pos);
}

pub fn blobLen(s: *IntSet) usize {
    const length = s.getLength();
    const encoding = s.getEncoding();
    return @sizeOf(IntSet) + length * encoding;
}

pub fn free(s: *IntSet, allocator: Allocator) void {
    allocator.free(s.asBytes());
}

pub fn asBytes(s: *IntSet) []align(@alignOf(IntSet)) u8 {
    const mem: [*]align(@alignOf(IntSet)) u8 = @ptrCast(s);
    return mem[0..s.blobLen()];
}

fn moveTail(s: *IntSet, from: u32, to: u32) void {
    const encoding = s.getEncoding();
    const len = s.getLength() - from;
    if (encoding == ENC_INT64) {
        const ptr = s.numbersPtr(i64);
        @memmove(ptr[to .. to + len], ptr[from .. from + len]);
        return;
    }
    if (encoding == ENC_INT32) {
        const ptr = s.numbersPtr(i32);
        @memmove(ptr[to .. to + len], ptr[from .. from + len]);
        return;
    }
    const ptr = s.numbersPtr(i16);
    @memmove(ptr[to .. to + len], ptr[from .. from + len]);
}

const SearchResult = struct {
    /// The position of the value in the list if found, or the potision where
    /// the value can be inserted if not found.
    pos: u32,
    found: bool,
};
fn search(s: *IntSet, value: Value) SearchResult {
    if (s.getLength() == 0) {
        return .{
            .found = false,
            .pos = 0,
        };
    }
    if (value > s.getAt(s.getLength() - 1)) {
        return .{
            .found = false,
            .pos = s.getLength(),
        };
    }
    if (value < s.getAt(0)) {
        return .{
            .found = false,
            .pos = 0,
        };
    }

    var min: u32 = 0;
    var max = s.getLength() - 1;
    var mid: u32 = 0;
    var cur: Value = -1;

    while (max >= min) {
        mid = (min + max) >> 1;
        cur = s.getAt(mid);
        if (value > cur) {
            min = mid + 1;
        } else if (value < cur) {
            max = mid - 1;
        } else {
            break;
        }
    }
    if (value == cur) {
        return .{
            .found = true,
            .pos = mid,
        };
    }
    return .{
        .found = false,
        .pos = min,
    };
}

fn upgradeAdd(
    s: *IntSet,
    allocator: Allocator,
    value: Value,
) Allocator.Error!*IntSet {
    const curr_enc = s.getEncoding();
    const new_enc = valueEncoding(value);
    std.debug.assert(new_enc > curr_enc);

    const old_length = s.getLength();
    const new_length = old_length + 1;
    const ns = try s.resize(allocator, new_length, new_enc);

    const prepend: u32 = if (value < 0) 1 else 0;
    var length = old_length;
    while (length > 0) {
        length -= 1;
        ns.setAt(length + prepend, ns.getAtEncoded(length, curr_enc));
    }
    if (prepend == 1) {
        ns.setAt(0, value);
    } else {
        ns.setAt(old_length, value);
    }
    return ns;
}

fn setAt(s: *IntSet, pos: u32, value: Value) void {
    const encoding = s.getEncoding();
    if (encoding == ENC_INT64) {
        s.numbersPtr(i64)[pos] = nativeToLittle(i64, value);
        return;
    }
    if (encoding == ENC_INT32) {
        s.numbersPtr(i32)[pos] = nativeToLittle(i32, @intCast(value));
        return;
    }
    s.numbersPtr(i16)[pos] = nativeToLittle(i16, @intCast(value));
}

fn getAt(s: *IntSet, pos: u32) Value {
    return s.getAtEncoded(pos, s.getEncoding());
}

fn getAtEncoded(s: *IntSet, pos: u32, encoding: u32) Value {
    if (encoding == ENC_INT64) {
        return littleToNative(i64, s.numbersPtr(i64)[pos]);
    }
    if (encoding == ENC_INT32) {
        return littleToNative(i32, s.numbersPtr(i32)[pos]);
    }
    return littleToNative(i16, s.numbersPtr(i16)[pos]);
}

fn numbersPtr(s: *IntSet, comptime T: type) [*]T {
    const ptr: [*]u8 = @ptrFromInt(@intFromPtr(s) + @sizeOf(IntSet));
    return @ptrCast(@alignCast(ptr));
}

fn getEncoding(s: *IntSet) u32 {
    return littleToNative(u32, s.encoding);
}

fn resize(
    s: *IntSet,
    allocator: Allocator,
    new_len: u32,
    new_enc: ?u32,
) Allocator.Error!*IntSet {
    const encoding = new_enc orelse s.getEncoding();
    const new_mem_size = @sizeOf(IntSet) + new_len * encoding;
    const new_mem = try allocator.realloc(
        s.asBytes(),
        new_mem_size,
    );
    const ns: *IntSet = @ptrCast(@alignCast(new_mem.ptr));
    ns.length = nativeToLittle(u32, new_len);
    ns.encoding = nativeToLittle(u32, encoding);
    return ns;
}

fn valueEncoding(value: Value) u32 {
    if (value < minInt(i32) or value > maxInt(i32)) {
        return ENC_INT64;
    }
    if (value < minInt(i16) or value > maxInt(i16)) {
        return ENC_INT32;
    }
    return ENC_INT16;
}

test IntSet {
    const allocator = testing.allocator;
    var s = try new(allocator);
    defer s.free(allocator);

    var res = try s.add(allocator, 1);
    try testing.expect(res.success);
    s = res.set;

    res = try s.add(allocator, 1);
    try testing.expect(res.success == false);
    s = res.set;

    try testing.expect(s.find(1));

    const max32 = std.math.maxInt(i32);
    res = try s.add(allocator, max32);
    try testing.expect(res.success);
    s = res.set;
    try testing.expect(s.find(max32));
    try testing.expect(s.find(1));
    try testing.expectEqual(2, s.getLength());

    const min64 = std.math.minInt(i64);
    res = try s.add(allocator, min64);
    try testing.expect(res.success);
    s = res.set;
    try testing.expect(s.find(min64));
    try testing.expect(s.find(max32));
    try testing.expect(s.find(1));
    try testing.expectEqual(3, s.getLength());

    res = try s.remove(allocator, 1);
    try testing.expect(res.success);
    s = res.set;
    try testing.expectEqual(2, s.getLength());

    const first = s.get(0);
    try testing.expect(first != null and first.? == min64);

    const r = s.random();
    try testing.expect(r == min64 or r == max32);

    const bytes = s.blobLen();
    try testing.expectEqual(24, bytes);

    const max64 = std.math.maxInt(i64);
    res = try s.add(allocator, max64);
    s = res.set;
    const min32 = std.math.minInt(i32);
    res = try s.add(allocator, min32);
    s = res.set;
    try testing.expectEqual(4, s.getLength());
    try testing.expectEqualSlices(
        i64,
        &.{ min64, min32, max32, max64 },
        s.numbersPtr(i64)[0..s.getLength()],
    );
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const maxInt = std.math.maxInt;
const minInt = std.math.minInt;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
