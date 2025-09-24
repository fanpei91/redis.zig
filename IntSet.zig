const ENC_INT16 = @sizeOf(i16);
const ENC_INT32 = @sizeOf(i32);
const ENC_INT64 = @sizeOf(i64);

encoding: LittleEndian(u32, @alignOf(u32)),
length: LittleEndian(u32, @alignOf(u32)),
numbers: [0]u8, // Little Endian.

const IntSet = @This();
pub const Value = i64;

pub fn new(allocator: Allocator) Allocator.Error!*IntSet {
    const s = try allocator.create(IntSet);
    s.encoding.set(ENC_INT16);
    s.length.set(0);
    return s;
}

pub fn add(
    s: *IntSet,
    allocator: Allocator,
    value: Value,
) Allocator.Error!struct { set: *IntSet, success: bool } {
    const enc = valueEncoding(value);
    if (enc > s.encoding.get()) {
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

    const cur_len = s.length.get();
    const new_length = cur_len + 1;
    const ns = try s.resize(allocator, new_length, null);
    const pos = res.pos;
    if (pos < cur_len) ns.moveTail(pos, pos + 1, cur_len - pos);
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
) Allocator.Error!struct { set: *IntSet, success: bool } {
    const enc = valueEncoding(value);
    if (enc > s.encoding.get()) {
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

    const len = s.length.get();
    const pos = res.pos;
    if (pos < len - 1) {
        const from = pos + 1;
        s.moveTail(from, pos, len - from);
    }
    const ns = try s.resize(allocator, len - 1, null);
    return .{
        .set = ns,
        .success = true,
    };
}

pub fn find(s: *IntSet, value: Value) bool {
    const enc = valueEncoding(value);
    if (enc > s.encoding.get()) {
        return false;
    }
    const res = s.search(value);
    return res.found;
}

pub fn get(s: *IntSet, pos: u32) ?Value {
    if (pos < s.length.get()) {
        return s.getAt(pos);
    }
    return null;
}

pub fn random(s: *IntSet) Value {
    const pos = rand.int(u32) % s.length.get();
    return s.getAt(pos);
}

pub fn blobLen(s: *IntSet) usize {
    const length = s.length.get();
    const encoding = s.encoding.get();
    return @sizeOf(IntSet) + length * encoding;
}

pub fn free(s: *IntSet, allocator: Allocator) void {
    allocator.free(s.asBytes());
}

fn moveTail(s: *IntSet, from: u32, to: u32, len: usize) void {
    const encoding = s.encoding.get();
    if (encoding == ENC_INT64) {
        const ptr = s.numbersPtr(i64);
        memmove(ptr + to, ptr + from, len);
        return;
    }
    if (encoding == ENC_INT32) {
        const ptr = s.numbersPtr(i32);
        memmove(ptr + to, ptr + from, len);
        return;
    }
    const ptr = s.numbersPtr(i16);
    memmove(ptr + to, ptr + from, len);
}

/// The position of the value in the list if found, or the potision where
/// the value can be inserted if not found.
fn search(s: *IntSet, value: Value) struct { pos: u32, found: bool } {
    if (s.length.get() == 0) {
        return .{
            .found = false,
            .pos = 0,
        };
    }
    if (value > s.getAt(s.length.get() - 1)) {
        return .{
            .found = false,
            .pos = s.length.get(),
        };
    }
    if (value < s.getAt(0)) {
        return .{
            .found = false,
            .pos = 0,
        };
    }

    var min: u32 = 0;
    var max = s.length.get() - 1;
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
    const curr_enc = s.encoding.get();
    const new_enc = valueEncoding(value);
    std.debug.assert(new_enc > curr_enc);

    const old_length = s.length.get();
    const new_length = old_length + 1;
    const ns = try s.resize(allocator, new_length, new_enc);

    const prepend = value < 0;
    var length = old_length;
    while (length > 0) {
        length -= 1;
        ns.setAt(
            length + @intFromBool(prepend),
            ns.getAtEncoded(length, curr_enc),
        );
    }
    if (prepend) {
        ns.setAt(0, value);
    } else {
        ns.setAt(old_length, value);
    }
    return ns;
}

fn setAt(s: *IntSet, pos: u32, value: Value) void {
    const encoding = s.encoding.get();
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
    return s.getAtEncoded(pos, s.encoding.get());
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

fn resize(
    s: *IntSet,
    allocator: Allocator,
    new_len: u32,
    new_enc: ?u32,
) Allocator.Error!*IntSet {
    const encoding = new_enc orelse s.encoding.get();
    const new_mem_size = @sizeOf(IntSet) + new_len * encoding;
    const new_mem = try allocator.realloc(
        s.asBytes(),
        new_mem_size,
    );
    const ns: *IntSet = @ptrCast(@alignCast(new_mem.ptr));
    ns.length.set(new_len);
    ns.encoding.set(encoding);
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

inline fn numbersPtr(s: *IntSet, comptime T: type) [*]T {
    const ptr: [*]u8 = @ptrFromInt(@intFromPtr(s) + @sizeOf(IntSet));
    return @ptrCast(@alignCast(ptr));
}

inline fn asBytes(s: *IntSet) []align(@alignOf(IntSet)) u8 {
    const mem: [*]align(@alignOf(IntSet)) u8 = @ptrCast(s);
    return mem[0..s.blobLen()];
}

test IntSet {
    rand.seed(@bitCast(std.time.microTimestamp()));

    const allocator = testing.allocator;
    var s = try new(allocator);
    defer s.free(allocator);

    var add_res = try s.add(allocator, 1);
    s = add_res.set;
    try expect(add_res.success);

    add_res = try s.add(allocator, 1);
    s = add_res.set;
    try expect(add_res.success == false);
    try expect(s.find(1));

    const max32 = std.math.maxInt(i32);
    add_res = try s.add(allocator, max32);
    s = add_res.set;
    try expect(add_res.success);
    try expect(s.find(max32));
    try expect(s.find(1));
    try expectEqual(2, s.length.get());

    const min64 = std.math.minInt(i64);
    add_res = try s.add(allocator, min64);
    s = add_res.set;
    try expect(add_res.success);
    try expect(s.find(min64));
    try expect(s.find(max32));
    try expect(s.find(1));
    try expectEqual(3, s.length.get());

    var remove_res = try s.remove(allocator, 1);
    s = remove_res.set;
    try expect(remove_res.success);
    try expectEqual(2, s.length.get());

    const first = s.get(0);
    try expect(first != null and first.? == min64);

    const r = s.random();
    try expect(r == min64 or r == max32);

    const bytes = s.blobLen();
    try expectEqual(24, bytes);

    const max64 = std.math.maxInt(i64);
    add_res = try s.add(allocator, max64);
    s = add_res.set;
    const min32 = std.math.minInt(i32);
    add_res = try s.add(allocator, min32);
    s = add_res.set;
    try expectEqual(4, s.length.get());
    try expectEqualSlices(
        i64,
        &.{
            nativeToLittle(i64, min64),
            nativeToLittle(i64, min32),
            nativeToLittle(i64, max32),
            nativeToLittle(i64, max64),
        },
        s.numbersPtr(i64)[0..s.length.get()],
    );
    const ptr: [*]u32 = @ptrCast(@alignCast(s));
    try expectEqual(s.encoding.val, ptr[0]);
    try expectEqual(s.length.val, ptr[1]);

    remove_res = try s.remove(allocator, min32);
    s = remove_res.set;
    try expect(remove_res.success);
    try expect(s.find(min32) == false);
    try expect(s.find(min64));
    try expect(s.find(max32));
    try expect(s.find(max64));
    try expect(s.length.get() == 3);
}

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;
const maxInt = std.math.maxInt;
const minInt = std.math.minInt;
const nativeToLittle = std.mem.nativeToLittle;
const littleToNative = std.mem.littleToNative;
const LittleEndian = @import("endian.zig").LittleEndian;
const expectEqualSlices = testing.expectEqualSlices;
const expectEqual = testing.expectEqual;
const expect = testing.expect;
const rand = @import("random.zig");
const memzig = @import("mem.zig");
const memmove = memzig.memmove;
