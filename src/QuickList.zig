/// compression disable
const NOCOMPRESS = false;

/// Maximum size in bytes of any multi-element ziplist.
/// Larger values will live in their own isolated ziplists.
/// This is used only if we're limited by record count. when we're limited by
/// size, the maximum limit is bigger, but still safe.
/// 8k is a recommended / default size limit
const SIZE_SAFETY_LIMIT = 8192;

/// Minimum ziplist size in bytes for attempting compression.
const MIN_COMPRESS_BYTES = 48;

/// Minimum size reduction in bytes to store compressed quicklistNode data.
/// This also prevents us from storing compression if the compression
/// resulted in a larger size than the original data.
const MIN_COMPRESS_IMPROVE = 8;

/// Optimization levels for size-based filling.
/// Note that the largest possible limit is 16k, so even if each record takes
/// just one byte, it still won't overflow the 16 bit count field.
const optimization_level = [_]usize{
    4096,
    8192,
    16384,
    32768,
    65536,
};

/// Node is a 32 byte struct describing a ziplist for a quicklist.
/// We use packed struct keep the Node at 32 bytes.
/// count: 16 bits, max 65536 (max zl bytes is 65k, so max count actually < 32k).
/// encoding: 2 bits, RAW=1, LZF=2.
/// container: 2 bits, NONE=1, ZIPLIST=2.
/// recompress: 1 bit, bool, true if node is temporarry decompressed for usage.
/// attempted_compress: 1 bit, boolean, used for verifying during testing.
/// extra: 10 bits, free for future use; pads out the remainder of 32 bits
const Node = packed struct {
    const Encoding = enum(u2) {
        raw = 1,
        lzf = 2,
    };

    const Container = enum(u2) {
        none = 1,
        ziplist = 2,
    };

    const Recompress = enum(u1) {
        false = 0,
        true = 1,
    };

    const AttemptedCompress = Recompress;

    prev: ?*Node,
    next: ?*Node,
    zl: ?*anyopaque,
    sz: uint, // ziplist size in bytes
    count: u16, // count of items in ziplist
    encoding: Encoding, // RAW==1 or LZF==2
    container: Container, // NONE==1 or ZIPLIST==2
    recompress: Recompress, // was this node previous compressed?
    attempted_compress: AttemptedCompress, // node can't compress; too small
    extra: u10, // more bits to steal for future usage

    fn create(allocator: Allocator) Allocator.Error!*Node {
        const node = try allocator.create(Node);
        node.prev = null;
        node.next = null;
        node.zl = null;
        node.sz = 0;
        node.count = 0;
        node.encoding = .raw;
        node.container = .ziplist;
        node.recompress = .false;
        node.attempted_compress = .false;
        node.extra = 0;
        return node;
    }

    inline fn isCompressed(self: *const Node) bool {
        return self.encoding == .lzf;
    }

    fn allowInsert(self: *const Node, fill: i16, sz: usize) bool {
        var ziplist_overhead: usize = 0;

        // size of previous offset
        if (sz < 254) {
            ziplist_overhead = 1;
        } else {
            ziplist_overhead = 5;
        }

        // size of forward offset
        if (sz < 64) {
            ziplist_overhead += 1;
        } else if (sz < 16384) {
            @branchHint(.likely);
            ziplist_overhead += 2;
        } else {
            ziplist_overhead += 5;
        }

        const new_sz = self.sz + sz + ziplist_overhead;
        if (sizeMeetsOptimizationRequirement(new_sz, fill)) {
            @branchHint(.likely);
            return true;
        } else if (!sizeMeetsSafetyLimit(new_sz)) {
            return false;
        } else if (self.count < fill) {
            return true;
        }
        return false;
    }

    fn sizeMeetsOptimizationRequirement(sz: usize, fill: int) bool {
        if (fill >= 0) return false;
        const offset = @abs(fill) - 1;
        if (offset < optimization_level.len) {
            if (sz <= optimization_level[offset]) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    inline fn sizeMeetsSafetyLimit(sz: usize) bool {
        return sz <= SIZE_SAFETY_LIMIT;
    }

    /// Compress only uncompressed nodes.
    fn compress(self: *Node, allocator: Allocator) Allocator.Error!bool {
        self.attempted_compress = .true;
        if (self.encoding != .raw) return false;
        if (self.sz < MIN_COMPRESS_BYTES) return false;

        var lz = try LZF.create(allocator, self.sz);
        errdefer lz.free(allocator);
        const zl: *ZipList = @ptrCast(@alignCast(self.zl.?));
        const sz = lzf.compress(
            zl.toBytes(),
            lz.data(),
        );
        if (sz == 0 or sz + MIN_COMPRESS_IMPROVE >= self.sz) {
            // lzf.compress() aborts/rejects compression if value not compressable.
            lz.free(allocator);
            return false;
        }

        lz = try lz.resize(allocator, sz);
        zl.free(allocator);
        self.zl = lz;
        self.encoding = .lzf;
        self.recompress = .false;
        return true;
    }

    fn decompress(self: *Node, allocator: Allocator) Allocator.Error!bool {
        self.attempted_compress = .false;
        if (self.encoding != .lzf) return false;

        const decompressed = try allocator.alignedAlloc(
            u8,
            .of(ZipList),
            self.sz,
        );
        const lz: *LZF = @ptrCast(@alignCast(self.zl));
        if (lzf.decompress(lz.data(), decompressed) == 0) {
            allocator.free(decompressed);
            return false;
        }

        lz.free(allocator);
        self.zl = decompressed.ptr;
        self.encoding = .raw;
        return true;
    }

    fn updateSz(self: *Node) void {
        const zl: *ZipList = @ptrCast(@alignCast(self.zl.?));
        self.sz = zl.blobLen();
    }

    fn release(self: *Node, allocator: Allocator) void {
        defer allocator.destroy(self);
        if (self.zl) |container| {
            if (self.container == .ziplist) {
                if (self.encoding == .lzf) {
                    const lz: *LZF = @ptrCast(@alignCast(container));
                    lz.free(allocator);
                } else {
                    const zl: *ZipList = @ptrCast(@alignCast(container));
                    zl.free(allocator);
                }
            } else if (self.container == .none) {
                const raw: [*]u8 = @ptrCast(container);
                allocator.free(raw[0..self.sz]);
            }
        }
    }
};

/// LZF is a 4+N byte struct holding 'sz' followed by 'compressed'.
/// 'sz' is byte length of 'compressed' field.
/// 'compressed' is LZF data with total (compressed) length 'sz'
/// NOTE: uncompressed length is stored in Node.sz.
/// When Node.zl is compressed, node.zl points to a LZF
const LZF = struct {
    sz: uint, // LZF size in bytes
    compressed: [0]u8,

    fn create(allocator: Allocator, sz: uint) Allocator.Error!*LZF {
        const mem_size = @as(usize, @sizeOf(LZF)) + sz;
        const mem = try allocator.alignedAlloc(u8, .of(LZF), mem_size);
        const lz: *LZF = @ptrCast(@alignCast(mem.ptr));
        lz.sz = sz;
        return lz;
    }

    fn resize(
        self: *LZF,
        allocator: Allocator,
        new_sz: uint,
    ) Allocator.Error!*LZF {
        const old_mem = self.addr();
        const old_mem_size = @as(usize, @sizeOf(LZF)) + self.sz;
        const new_mem_size = @as(usize, @sizeOf(LZF)) + new_sz;
        const new_mem = try allocator.realloc(
            old_mem[0..old_mem_size],
            new_mem_size,
        );
        const lz: *LZF = @ptrCast(@alignCast(new_mem.ptr));
        lz.sz = new_sz;
        return lz;
    }

    fn data(self: *LZF) []u8 {
        const mem = self.addr();
        const start: usize = @sizeOf(LZF);
        const end = start + self.sz;
        return mem[start..end];
    }

    fn free(self: *LZF, allocator: Allocator) void {
        const mem = self.addr();
        const mem_size = @as(usize, @sizeOf(LZF)) + self.sz;
        allocator.free(mem[0..mem_size]);
    }

    inline fn addr(self: *LZF) [*]align(@alignOf(LZF)) u8 {
        return @ptrCast(@alignCast(self));
    }
};

const Iterator = struct {
    const Direction = enum(int) {
        head = 0,
        tail = 1,
    };

    quicklist: *QuickList,
    current: *Node,
    zi: [*]u8,
    offset: long, // offset in current ziplist
    direction: Direction,
};

const Entry = struct {
    quicklist: *QuickList,
    node: *Node,
    zi: [*]u8,
    value: [*]u8,
    longval: longlong,
    sz: uint,
    offset: int,
};

const Where = enum {
    head,
    tail,
};

/// quicklist is a 40 byte struct (on 64-bit systems) describing a quicklist.
/// 'count' is the number of total entries.
/// 'len' is the number of quicklist nodes.
/// 'compress' is: -1 if compression disabled, otherwise it's the number
///                of quicklistNodes to leave uncompressed at ends of quicklist.
/// 'fill' is the user-requested (or default) fill factor.
const QuickList = @This();
head: ?*Node,
tail: ?*Node,
count: ulong, // total count of all entries in all ziplists
len: ulong, // number of Nodes
fill: i16, // fill factor for individual nodes
compress: u16, // depth of end nodes not to compress;0=off

pub fn create(allocator: Allocator) Allocator.Error!*QuickList {
    const list = try allocator.create(QuickList);
    list.* = .{
        .head = null,
        .tail = null,
        .count = 0,
        .len = 0,
        .fill = -2,
        .compress = 0,
    };
    return list;
}

pub fn new(
    allocator: Allocator,
    fill: i16,
    compress: u16,
) Allocator.Error!*QuickList {
    const ql = try create(allocator);
    ql.setOptions(fill, compress);
    return ql;
}

pub fn setOptions(self: *QuickList, fill: i16, depth: u16) void {
    self.setFill(fill);
    self.setCompressDepth(depth);
}

pub fn push(
    self: *QuickList,
    allocator: Allocator,
    value: []const u8,
    where: Where,
) Allocator.Error!void {
    if (where == .head) {
        _ = try self.pushHead(allocator, value);
    } else if (where == .tail) {
        _ = try self.pushTail(allocator, value);
    }
}

pub fn release(self: *QuickList, allocator: Allocator) void {
    var current = self.head;
    while (current) |node| {
        current = node.next;
        self.count -= node.count;
        self.len -= 1;
        node.release(allocator);
    }
    allocator.destroy(self);
}

/// Add new entry to head node of quicklist.
///
/// Returns false if used existing head.
/// Returns true if new head created.
fn pushHead(
    self: *QuickList,
    allocator: Allocator,
    value: []const u8,
) Allocator.Error!bool {
    const orig_head = self.head;
    assert(value.len < maxInt(u32));

    if (orig_head != null and orig_head.?.allowInsert(self.fill, value.len)) {
        @branchHint(.likely);
        const zl: *ZipList = @ptrCast(@alignCast(self.head.?.zl.?));
        self.head.?.zl = try zl.push(allocator, value, .head);
        self.head.?.updateSz();
    } else {
        const node = try Node.create(allocator);
        errdefer node.release(allocator);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);
        node.zl = try zl.push(allocator, value, .head);
        node.updateSz();
        errdefer node.zl = null; // Avoiding double-free in Node.release().
        try self.insertNodeBefore(allocator, self.head, node);
    }

    self.count += 1;
    self.head.?.count += 1;
    return orig_head != self.head;
}

/// Add new entry to tail node of quicklist.
///
/// Returns false if used existing tail.
/// Returns true if new tail created.
fn pushTail(
    self: *QuickList,
    allocator: Allocator,
    value: []const u8,
) Allocator.Error!bool {
    const orig_tail = self.tail;
    assert(value.len < maxInt(u32));

    if (orig_tail != null and orig_tail.?.allowInsert(self.fill, value.len)) {
        @branchHint(.likely);
        const zl: *ZipList = @ptrCast(@alignCast(self.tail.?.zl.?));
        self.tail.?.zl = try zl.push(allocator, value, .tail);
        self.tail.?.updateSz();
    } else {
        const node = try Node.create(allocator);
        errdefer node.release(allocator);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);
        node.zl = try zl.push(allocator, value, .tail);
        node.updateSz();
        errdefer node.zl = null; // Avoiding double-free in Node.release().
        try self.insertNodeAfter(allocator, self.tail, node);
    }

    self.count += 1;
    self.tail.?.count += 1;
    return orig_tail != self.tail;
}

fn setFill(self: *QuickList, fill: i16) void {
    self.fill = if (fill < -5) -5 else fill;
}

fn setCompressDepth(self: *QuickList, depth: u16) void {
    self.compress = depth;
}

fn insertNodeBefore(
    self: *QuickList,
    allocator: Allocator,
    old_node: ?*Node,
    new_node: *Node,
) Allocator.Error!void {
    return self.insertNode(allocator, old_node, new_node, false);
}

fn insertNodeAfter(
    self: *QuickList,
    allocator: Allocator,
    old_node: ?*Node,
    new_node: *Node,
) Allocator.Error!void {
    return self.insertNode(allocator, old_node, new_node, true);
}

fn insertNode(
    self: *QuickList,
    allocator: Allocator,
    old_node: ?*Node,
    new_node: *Node,
    after: bool,
) Allocator.Error!void {
    if (after) {
        new_node.prev = old_node;
        if (old_node) |old| {
            new_node.next = old.next;
            if (old.next) |next| {
                next.prev = new_node;
            }
            old.next = new_node;
        }
        if (self.tail == old_node) {
            self.tail = new_node;
        }
    } else {
        new_node.next = old_node;
        if (old_node) |old| {
            new_node.prev = old.prev;
            if (old.prev) |prev| {
                prev.next = new_node;
            }
            old.prev = new_node;
        }
        if (self.head == old_node) {
            self.head = new_node;
        }
    }
    if (self.len == 0) {
        self.head = new_node;
        self.tail = new_node;
    }
    if (old_node) |old| {
        try self.compressNode(allocator, old);
    }
    self.len += 1;
}

fn compressNode(
    self: *QuickList,
    allocator: Allocator,
    node: *Node,
) Allocator.Error!void {
    if (node.recompress == .true) {
        _ = try node.compress(allocator);
        return;
    }
    try self._compress(allocator, node);
}

/// Force 'quicklist' to meet compression guidelines set by compress depth.
/// The only way to guarantee interior nodes get compressed is to iterate
/// to our "interior" compress depth then compress the next node we find.
/// If compress depth is larger than the entire list, we return immediately.
fn _compress(
    self: *QuickList,
    allocator: Allocator,
    node: *Node,
) Allocator.Error!void {
    // If length is less than our compress depth (from both sides),
    // we can't compress anything.
    if (!self.allowsCompression() or self.len < self.compress * 2) {
        return;
    }

    var forward = self.head;
    var reverse = self.tail;
    var depth: usize = 0;
    var in_depth = false;
    while (depth < self.compress) : (depth += 1) {
        if (forward) |f| _ = try f.decompress(allocator);
        if (reverse) |r| _ = try r.decompress(allocator);

        if (forward == node or reverse == node) {
            in_depth = true;
        }

        if (forward == reverse) return;

        if (forward) |f| forward = f.next;
        if (reverse) |r| reverse = r.prev;
    }

    if (!in_depth) {
        _ = try node.compress(allocator);
    }

    if (depth > 2) {
        // At this point, forward and reverse are one node beyond depth.
        if (forward) |f| _ = try f.compress(allocator);
        if (reverse) |r| _ = try r.compress(allocator);
    }
}

inline fn allowsCompression(self: *QuickList) bool {
    return self.compress != 0;
}

test lzf {
    const phrase = "How are you? I am fine! Thank you! And you?" ** 100;
    var compressed: [phrase.len]u8 = undefined;
    var decompressed: [phrase.len]u8 = undefined;
    const csz = lzf.compress(phrase, &compressed);
    try expect(csz != 0);
    try expect(csz < phrase.len);
    const dsz = lzf.decompress(compressed[0..csz], &decompressed);
    try expect(dsz != 0);
    try expect(dsz == phrase.len);
    try expectEqualStrings(phrase, &decompressed);
}

test QuickList {}

const std = @import("std");
const ctypes = @import("ctypes.zig");
const ulong = ctypes.ulong;
const int = ctypes.int;
const uint = ctypes.uint;
const long = ctypes.long;
const longlong = ctypes.longlong;
const Allocator = std.mem.Allocator;
const ZipList = @import("ZipList.zig");
const assert = std.debug.assert;
const maxInt = std.math.maxInt;
const lzf = @import("lzf/lzf.zig");
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
