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
pub const Node = packed struct {
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

    fn allowInsert(node: ?*const Node, fill: i16, sz: usize) bool {
        if (node == null) {
            @branchHint(.unlikely);
            return false;
        }
        const self = node.?;

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
        }
        if (!sizeMeetsSafetyLimit(new_sz)) {
            return false;
        }
        if (self.count < fill) {
            return true;
        }
        return false;
    }

    fn allowMerge(a: ?*const Node, b: ?*const Node, fill: i16) bool {
        if (a == null or b == null) return false;

        // approximate merged ziplist size (- 11 to remove one ziplist
        // header/trailer)
        const merge_sz = a.?.sz + b.?.sz - 11;
        if (sizeMeetsOptimizationRequirement(merge_sz, fill)) {
            @branchHint(.likely);
            return true;
        }
        if (!sizeMeetsSafetyLimit(merge_sz)) {
            return false;
        }
        if (a.?.count + b.?.count <= fill) {
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
        const zl: *ZipList = ZipList.cast(self.zl.?);
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

    fn decompressForUse(
        self: *Node,
        allocator: Allocator,
    ) Allocator.Error!void {
        if (try self.decompress(allocator)) {
            self.recompress = .true;
        }
    }

    /// Split 'node' into two parts, parameterized by 'offset' and 'after'.
    ///
    /// The 'after' argument controls which quicklistNode gets returned.
    /// If 'after'==1, returned node has elements after 'offset'.
    ///                input node keeps elements up to 'offset', including 'offset'.
    /// If 'after'==0, returned node has elements up to 'offset', including 'offset'.
    ///                input node keeps elements after 'offset'.
    ///
    /// If 'after'==1, returned node will have elements _after_ 'offset'.
    ///                The returned node will have elements [OFFSET+1, END].
    ///                The input node keeps elements [0, OFFSET].
    ///
    /// If 'after'==0, returned node will keep elements up to and including 'offset'.
    ///                The returned node will have elements [0, OFFSET].
    ///                The input node keeps elements [OFFSET+1, END].
    ///
    /// The input node keeps all elements not taken by the returned node.
    ///
    /// Returns newly created node or NULL if split not possible.
    fn split(
        self: *Node,
        allocator: Allocator,
        offset: int,
        after: bool,
    ) Allocator.Error!*Node {
        assert(!self.isCompressed());

        const new_node = try Node.create(allocator);
        errdefer allocator.destroy(new_node);

        const zl = try allocator.alignedAlloc(u8, .of(ZipList), self.sz);
        errdefer allocator.free(zl);
        @memcpy(zl, ZipList.cast(self.zl.?).toBytes());
        new_node.zl = zl.ptr;

        const orig_start = if (after) offset + 1 else 0;
        const orig_extent = if (after) -1 else offset;
        var orig_zl: *ZipList = ZipList.cast(self.zl.?);
        orig_zl = try orig_zl.deleteRange(
            allocator,
            orig_start,
            @bitCast(orig_extent), // -1 equals to maxInt(uint)
        );
        self.zl = orig_zl;
        self.count = orig_zl.len.get();
        self.updateSz();

        const new_start = if (after) 0 else offset;
        const new_extent = if (after) offset + 1 else -1;
        var new_zl: *ZipList = ZipList.cast(zl.ptr);
        new_zl = try new_zl.deleteRange(
            allocator,
            new_start,
            @bitCast(new_extent), // -1 equals to maxInt(uint)
        );
        new_node.zl = new_zl;
        new_node.count = new_zl.len.get();
        new_node.updateSz();

        return new_node;
    }

    fn updateSz(self: *Node) void {
        self.sz = ZipList.cast(self.zl.?).blobLen();
    }

    fn release(self: *Node, allocator: Allocator) void {
        defer allocator.destroy(self);
        if (self.zl) |container| {
            if (self.container == .ziplist) {
                if (self.encoding == .lzf) {
                    const lz: *LZF = @ptrCast(@alignCast(container));
                    lz.free(allocator);
                } else {
                    const zl: *ZipList = ZipList.cast(container);
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

pub const Iterator = struct {
    pub const Direction = enum(int) {
        head = 0,
        tail = 1,
    };

    quicklist: *QuickList,
    current: ?*Node,
    zi: ?[*]u8,
    offset: long, // offset in current ziplist
    direction: Direction,

    /// Get next element in iterator.
    ///
    /// Note: You must NOT insert into the list while iterating over it.
    /// You *may* delete from the list while iterating using the
    /// Iterator.del() function.
    /// If you insert into the quicklist while iterating, you should
    /// re-create the iterator after your addition.
    ///
    /// Populates 'entry' with values for this iteration.
    /// Returns false when iteration is complete or if iteration not possible.
    /// If return value is false, the contents of 'entry' are not valid.
    pub fn next(
        self: *Iterator,
        allocator: Allocator,
        entry: *Entry,
    ) Allocator.Error!bool {
        Entry.init(entry);
        entry.quicklist = self.quicklist;
        entry.node = self.current;

        if (self.current == null) return false;

        const current = self.current.?;
        try current.decompressForUse(allocator);
        const zl = ZipList.cast(current.zl.?);
        if (self.zi == null) {
            self.zi = zl.index(@intCast(self.offset));
        } else {
            if (self.direction == .head) {
                self.zi = zl.next(self.zi.?);
                self.offset += 1;
            } else if (self.direction == .tail) {
                self.zi = zl.prev(self.zi.?);
                self.offset += -1;
            }
        }
        entry.zi = self.zi;
        entry.offset = @intCast(self.offset);
        if (self.zi) |zi| {
            switch (zl.get(zi).?) {
                .str => |v| {
                    entry.value = v.ptr;
                    entry.sz = @intCast(v.len);
                },
                .num => |n| entry.longval = n,
            }
            return true;
        }
        // We ran out of ziplist entries.
        // Pick next node, update offset, then re-run retrieval.
        try self.quicklist.compressNode(allocator, current);
        if (self.direction == .head) {
            self.current = current.next;
            self.offset = 0;
        } else if (self.direction == .tail) {
            self.current = current.prev;
            self.offset = -1;
        }
        self.zi = null;
        return self.next(allocator, entry);
    }

    /// Delete one element represented by 'entry'
    ///
    /// 'entry' stores enough metadata to delete the proper position in
    /// the correct ziplist in the correct quicklist node.
    pub fn delEntry(
        self: *Iterator,
        allocator: Allocator,
        entry: *Entry,
    ) Allocator.Error!void {
        assert(entry.quicklist == self.quicklist);
        assert(entry.node == self.current);
        assert(entry.zi == self.zi);
        assert(entry.offset == self.offset);

        const node = entry.node.?;
        const nxt = node.next;
        const prv = node.prev;

        const deleted_node = try entry.quicklist.?.delIndex(
            allocator,
            node,
            &entry.zi.?,
        );
        // After delete, the zi is now invalid for any future usage.
        self.zi = null;

        // If current node is deleted, we must update iterator node and offset.
        if (deleted_node) {
            if (self.direction == .head) {
                self.current = nxt;
                self.offset = 0;
            } else if (self.direction == .tail) {
                self.current = prv;
                self.offset = -1;
            }
        }
        // else if (!deleted_node), no changes needed.
        // we already reset self.zi above, and the existing self.offset
        // doesn't move again because:
        //   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
        //   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
        //  if we deleted the last element at offet N and now
        //  length of this ziplist is N-1, the next call into
        //  self.next() will jump to the next node.
    }

    /// If we still have a valid current node, then re-encode current node.
    pub fn release(self: *Iterator, allocator: Allocator) Allocator.Error!void {
        if (self.current) |curr| {
            try self.quicklist.compressNode(allocator, curr);
        }
    }
};

pub const Entry = struct {
    quicklist: ?*QuickList,
    node: ?*Node,
    zi: ?[*]u8,
    value: ?[*]u8,
    longval: longlong,
    sz: uint,
    offset: int,

    fn init(e: *Entry) void {
        e.quicklist = null;
        e.node = null;
        e.zi = null;
        e.value = null;
        e.longval = -123456789;
        e.sz = 0;
        e.offset = 123456789;
    }
};

pub const Where = enum {
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

/// Create new (potentially multi-node) quicklist from a single existing ziplist.
/// Frees passed-in ziplist 'zl'.
pub fn createFromZiplist(
    allocator: Allocator,
    fill: i16,
    compress: u16,
    zl: *ZipList,
) Allocator.Error!*QuickList {
    defer zl.free(allocator);
    const ql = try new(allocator, fill, compress);
    var p = zl.index(0) orelse {
        return ql;
    };

    var longstr: [32]u8 = undefined;
    while (zl.get(p)) |un| {
        var value: []u8 = undefined;
        switch (un) {
            .str => |v| value = v,
            .num => |n| value = std.fmt.bufPrint(&longstr, "{}", .{n}) catch unreachable,
        }
        _ = try ql.pushTail(allocator, value);
        p = zl.next(p) orelse break;
    }

    return ql;
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

/// Add new entry to head node of quicklist.
///
/// Returns false if used existing head.
/// Returns true if new head created.
pub fn pushHead(
    self: *QuickList,
    allocator: Allocator,
    value: []const u8,
) Allocator.Error!bool {
    const orig_head = self.head;
    assert(value.len < maxInt(u32));

    if (Node.allowInsert(orig_head, self.fill, value.len)) {
        @branchHint(.likely);
        const zl: *ZipList = ZipList.cast(self.head.?.zl.?);
        self.head.?.zl = try zl.push(allocator, value, .head);
        self.head.?.updateSz();
    } else {
        const node = try Node.create(allocator);
        errdefer allocator.destroy(node);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);
        node.zl = try zl.push(allocator, value, .head);
        node.updateSz();
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
pub fn pushTail(
    self: *QuickList,
    allocator: Allocator,
    value: []const u8,
) Allocator.Error!bool {
    const orig_tail = self.tail;
    assert(value.len < maxInt(u32));

    if (Node.allowInsert(orig_tail, self.fill, value.len)) {
        @branchHint(.likely);
        const zl: *ZipList = ZipList.cast(self.tail.?.zl.?);
        self.tail.?.zl = try zl.push(allocator, value, .tail);
        self.tail.?.updateSz();
    } else {
        const node = try Node.create(allocator);
        errdefer allocator.destroy(node);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);
        node.zl = try zl.push(allocator, value, .tail);
        node.updateSz();
        try self.insertNodeAfter(allocator, self.tail, node);
    }

    self.count += 1;
    self.tail.?.count += 1;
    return orig_tail != self.tail;
}

pub fn insertAfter(
    self: *QuickList,
    allocator: Allocator,
    entry: *Entry,
    value: []const u8,
) Allocator.Error!void {
    return self.insert(allocator, entry, value, true);
}

pub fn insertBefore(
    self: *QuickList,
    allocator: Allocator,
    entry: *Entry,
    value: []const u8,
) Allocator.Error!void {
    return self.insert(allocator, entry, value, false);
}

/// Replace quicklist entry at offset 'idx' by 'data'.
///
/// Returns true if replace happened.
/// Returns false if replace failed and no changes happened.
pub fn replaceAtIndex(
    self: *QuickList,
    allocator: Allocator,
    idx: long,
    data: []const u8,
) Allocator.Error!bool {
    var entry: Entry = undefined;
    if (try self.index(allocator, idx, &entry)) {
        @branchHint(.likely);
        const node = entry.node.?;
        var zl = ZipList.cast(node.zl.?);
        zl = try zl.delete(allocator, &entry.zi.?);
        zl = try zl.insert(allocator, entry.zi.?, data);
        node.zl = zl;
        node.updateSz();
        try self.compressNode(allocator, node);
        return true;
    }
    return false;
}

/// Create new node consisting of a pre-formed ziplist.
/// Used for loading RDBs where entire ziplists have been stored
/// to be retrieved later.
pub fn appendZiplist(
    self: *QuickList,
    allocator: Allocator,
    zl: *ZipList,
) Allocator.Error!void {
    const node = try Node.create(allocator);
    errdefer node.release(allocator);

    node.zl = zl;
    node.sz = zl.blobLen();
    node.count = zl.len.get();

    try self.insertNodeAfter(allocator, self.tail, node);
    self.count += node.count;
}

pub fn iterator(
    self: *const QuickList,
    direction: Iterator.Direction,
) Iterator {
    return .{
        .quicklist = @constCast(self),
        .current = if (direction == .head) self.head else self.tail,
        .zi = null,
        .offset = if (direction == .head) 0 else -1,
        .direction = direction,
    };
}

pub fn iteratorAtIndex(
    self: *const QuickList,
    allocator: Allocator,
    direction: Iterator.Direction,
    idx: longlong,
) Allocator.Error!?Iterator {
    var entry: Entry = undefined;
    const ok = try self.index(allocator, idx, &entry);
    if (ok) {
        var it = self.iterator(direction);
        it.current = entry.node;
        it.zi = null;
        it.offset = entry.offset;
        return it;
    }
    return null;
}

/// Populate 'entry' with the element at the specified zero-based index
/// where 0 is the head, 1 is the element next to head
/// and so on. Negative integers are used in order to count
/// from the tail, -1 is the last element, -2 the penultimate
/// and so on. If the index is out of range false is returned.
///
/// Returns true if element found
/// Returns false if element not found
pub fn index(
    self: *const QuickList,
    allocator: Allocator,
    idx: longlong,
    entry: *Entry,
) Allocator.Error!bool {
    Entry.init(entry);
    entry.quicklist = @constCast(self);

    const forward = if (idx < 0) false else true;
    var indix: ulonglong = undefined;
    var n: ?*Node = null;
    if (!forward) {
        indix = @abs(idx) - 1;
        n = self.tail;
    } else {
        indix = @intCast(idx);
        n = self.head;
    }

    if (indix >= self.count) {
        return false;
    }

    var accum: ulonglong = 0;
    while (n) |node| {
        @branchHint(.likely);
        if (accum + node.count > indix) {
            break;
        }
        accum += node.count;
        n = if (forward) node.next else node.prev;
    }

    if (n == null) return false;

    entry.node = n;
    if (forward) {
        entry.offset = @intCast(indix - accum);
    } else {
        // reverse = need negative offset for tail-to-head, so undo
        // the result of the original if (index < 0) above.
        entry.offset = -@as(int, @intCast(indix + 1 - accum));
    }

    try entry.node.?.decompressForUse(allocator);
    const zl = ZipList.cast(entry.node.?.zl.?);
    entry.zi = zl.index(entry.offset);
    const value = zl.get(entry.zi.?).?;
    switch (value) {
        .num => |v| entry.longval = v,
        .str => |v| {
            entry.value = v.ptr;
            entry.sz = @intCast(v.len);
        },
    }
    // The caller will use our result, so we don't re-compress here.
    // The caller can recompress or delete the node as needed.
    return true;
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

///  Insert a new entry before or after existing entry 'entry'.
///
/// If after==true, the new value is inserted after 'entry', otherwise
/// the new value is inserted before 'entry'.
fn insert(
    self: *QuickList,
    allocator: Allocator,
    entry: *Entry,
    value: []const u8,
    after: bool,
) Allocator.Error!void {
    assert(value.len < maxInt(u32));

    const entry_node = entry.node;
    if (entry_node == null) {
        const new_node = try Node.create(allocator);
        errdefer allocator.destroy(new_node);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);

        zl = try zl.push(allocator, value, .head);
        new_node.zl = zl;
        new_node.count += 1;
        new_node.updateSz();

        try self.insertNode(allocator, null, new_node, after);
        self.count += 1;
        return;
    }

    const node = entry_node.?;
    const sz = value.len;
    const fill = self.fill;
    var at_head = false;
    var at_tail = false;
    var full = false;
    var full_next = false;
    var full_prev = false;

    if (!Node.allowInsert(node, fill, sz)) {
        full = true;
    }

    if (after and entry.offset == node.count) {
        at_tail = true;
        if (node.next) |next| {
            if (!Node.allowInsert(next, fill, sz)) {
                full_next = true;
            }
        }
    }

    if (!after and entry.offset == 0) {
        at_head = true;
        if (node.prev) |prev| {
            if (!Node.allowInsert(prev, fill, sz)) {
                full_prev = true;
            }
        }
    }

    defer self.count += 1;

    if (!full and after) {
        try node.decompressForUse(allocator);
        const zl: *ZipList = ZipList.cast(node.zl.?);
        const next = zl.next(entry.zi.?);
        if (next == null) {
            node.zl = try zl.push(allocator, value, .tail);
        } else {
            node.zl = try zl.insert(allocator, next.?, value);
        }
        node.count += 1;
        node.updateSz();
        try self.recompressOnly(allocator, node);
        return;
    }

    if (!full and !after) {
        try node.decompressForUse(allocator);
        const zl: *ZipList = ZipList.cast(node.zl.?);
        node.zl = try zl.insert(allocator, entry.zi.?, value);
        node.count += 1;
        node.updateSz();
        try self.recompressOnly(allocator, node);
        return;
    }

    if (full and at_tail and node.next != null and !full_next) {
        const next = node.next.?;
        try next.decompressForUse(allocator);
        const zl: *ZipList = ZipList.cast(next.zl.?);
        next.zl = try zl.push(allocator, value, .head);
        next.count += 1;
        next.updateSz();
        try self.recompressOnly(allocator, next);
        return;
    }

    if (full and at_head and node.prev != null and !full_prev) {
        const prev = node.prev.?;
        try prev.decompressForUse(allocator);
        const zl: *ZipList = ZipList.cast(prev.zl.?);
        prev.zl = try zl.push(allocator, value, .tail);
        prev.count += 1;
        prev.updateSz();
        try self.recompressOnly(allocator, prev);
        return;
    }

    // If we are: full, and our prev/next is full, then:
    //   - create new node and attach to quicklist
    if (full and
        ((at_tail and node.next != null and full_next) or
            (at_head and node.prev != null and full_prev)))
    {
        const new_node = try Node.create(allocator);
        errdefer allocator.destroy(new_node);

        var zl = try ZipList.new(allocator);
        errdefer zl.free(allocator);

        zl = try zl.push(allocator, value, .head);
        new_node.zl = zl;
        new_node.count += 1;
        new_node.updateSz();
        try self.insertNode(allocator, node, new_node, after);
        return;
    }

    // node is full we need to split it.
    // covers both after and !after cases.
    if (full) {
        try node.decompressForUse(allocator);
        const new_node = try node.split(allocator, entry.offset, after);
        errdefer new_node.release(allocator);
        const zl: *ZipList = ZipList.cast(new_node.zl.?);
        new_node.zl = try zl.push(allocator, value, if (after) .head else .tail);
        new_node.count += 1;
        new_node.updateSz();
        try self.insertNode(allocator, node, new_node, after);
        try self.mergeNodes(allocator, node);
    }
}

/// Attempt to merge ziplists within two nodes on either side of 'center'.
///
/// We attempt to merge:
///   - (center->prev->prev, center->prev)
///   - (center->next, center->next->next)
///   - (center->prev, center)
///   - (center, center->next)
fn mergeNodes(
    self: *QuickList,
    allocator: Allocator,
    center: *Node,
) Allocator.Error!void {
    const fill = self.fill;
    var prev: ?*Node = null;
    var prev_prev: ?*Node = null;
    var next: ?*Node = null;
    var next_next: ?*Node = null;

    if (center.prev != null) {
        prev = center.prev;
        if (prev.?.prev != null) {
            prev_prev = prev.?.prev;
        }
    }
    if (center.next != null) {
        next = center.next;
        if (next.?.next != null) {
            next_next = next.?.next;
        }
    }
    if (Node.allowMerge(prev, prev_prev, fill)) {
        _ = try self.mergeZiplist(allocator, prev_prev.?, prev.?);
        prev_prev = null;
        prev = null;
    }
    if (Node.allowMerge(next, next_next, fill)) {
        _ = try self.mergeZiplist(allocator, next_next.?, next.?);
        next_next = null;
        next = null;
    }

    var target: ?*Node = null;
    if (Node.allowMerge(center, center.prev, fill)) {
        target = try self.mergeZiplist(allocator, center.prev.?, center);
    } else {
        target = center;
    }

    if (Node.allowMerge(
        target,
        if (target != null) target.?.next else null,
        fill,
    )) {
        _ = try self.mergeZiplist(allocator, target.?, target.?.next.?);
    }
}

/// Given two nodes, try to merge their ziplists.
///
/// This helps us not have a quicklist with 3 element ziplists if
/// our fill factor can handle much higher levels.
///
/// Note: 'a' must be to the LEFT of 'b'.
///
/// After calling this function, both 'a' and 'b' should be considered
/// unusable.  The return value from this function must be used
/// instead of re-using any of the quicklistNode input arguments.
///
/// Returns the input node picked to merge against or NULL if
/// merging was not possible.
fn mergeZiplist(
    self: *QuickList,
    allocator: Allocator,
    a: *Node,
    b: *Node,
) Allocator.Error!?*Node {
    _ = try a.decompress(allocator);
    _ = try b.decompress(allocator);

    var azl: ?*ZipList = ZipList.cast(a.zl.?);
    var bzl: ?*ZipList = ZipList.cast(b.zl.?);
    const merged = try ZipList.merge(allocator, &azl, &bzl);
    if (merged != null) {
        var keep: *Node = undefined;
        var nokeep: *Node = undefined;
        if (azl == null) {
            keep = b;
            keep.zl = bzl.?;
            nokeep = a;
            nokeep.zl = null;
        } else if (bzl == null) {
            keep = a;
            keep.zl = azl.?;
            nokeep = b;
            nokeep.zl = null;
        }
        keep.count = ZipList.cast(keep.zl.?).len.get();
        keep.updateSz();
        nokeep.count = 0; // for the following self.delNode(nokeep)
        try self.delNode(allocator, nokeep);
        try self.compressNode(allocator, keep);
        return keep;
    }
    return null;
}

fn delNode(
    self: *QuickList,
    allocator: Allocator,
    node: *Node,
) Allocator.Error!void {
    if (node.next) |next| {
        next.prev = node.prev;
    }
    if (node.prev) |prev| {
        prev.next = node.next;
    }

    if (node == self.tail) {
        self.tail = node.prev;
    }
    if (node == self.head) {
        self.head = node.next;
    }

    // If we deleted a node within our compress depth, we
    // now have compressed nodes needing to be decompressed.
    try self.meetCompression(allocator, null);

    self.count -= node.count;
    node.release(allocator);
    self.len -= 1;
}

/// Delete one entry from list given the node for the entry and a pointer
/// to the entry in the node.
///
/// Note: delIndex() *requires* uncompressed nodes because you
///       already had to get p.* from an uncompressed node somewhere.
///
/// Returns true if the entire node was deleted, false if node still exists.
/// Also updates in/out param 'p' with the next offset in the ziplist.
fn delIndex(
    self: *QuickList,
    allocator: Allocator,
    node: *Node,
    p: *[*]u8,
) Allocator.Error!bool {
    node.zl = try ZipList.cast(node.zl.?).delete(allocator, p);
    node.count -= 1;
    var gone = false;
    if (node.count == 0) {
        gone = true;
        try self.delNode(allocator, node);
    } else {
        node.updateSz();
    }
    self.count -= 1;
    return gone;
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
    try self.meetCompression(allocator, node);
}

fn recompressOnly(
    _: QuickList,
    allocator: Allocator,
    node: *Node,
) Allocator.Error!void {
    if (node.recompress == .true) {
        _ = try node.compress(allocator);
    }
}

/// Force 'quicklist' to meet compression guidelines set by compress depth.
/// The only way to guarantee interior nodes get compressed is to iterate
/// to our "interior" compress depth then compress the next node we find.
/// If compress depth is larger than the entire list, we return immediately.
fn meetCompression(
    self: *QuickList,
    allocator: Allocator,
    node: ?*Node,
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

    if (!in_depth and node != null) {
        _ = try node.?.compress(allocator);
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
    try expect(dsz == phrase.len);
    try expectEqualStrings(phrase, &decompressed);
}

test "push" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    try ql.push(allocator, "hello" ** 100, .tail);
    try expectEqual(1, ql.len);
    try expectEqual(1, ql.count);
    try expect(ql.head == ql.tail);
    try expectEqual(1, ql.head.?.count);

    try ql.push(allocator, "world" ** 100, .head);
    try expectEqual(1, ql.len);
    try expectEqual(2, ql.count);

    for (0..10) |_| {
        try ql.push(allocator, "a" ** (SIZE_SAFETY_LIMIT + 1), .head);
        try ql.push(allocator, "a" ** (SIZE_SAFETY_LIMIT + 1), .tail);
    }
    try expectEqual(21, ql.len);
    try expectEqual(22, ql.count);
    try expect(ql.head.?.isCompressed() == false);
    try expect(ql.tail.?.isCompressed() == false);
    try expect(ql.head.?.next.?.isCompressed());
    try expect(ql.tail.?.prev.?.isCompressed());
}

test "createFromZiplist" {
    const allocator = std.testing.allocator;

    var zl = try ZipList.new(allocator);
    errdefer zl.free(allocator);

    zl = try zl.push(allocator, "hello", .tail);
    zl = try zl.push(allocator, "world", .tail);
    zl = try zl.push(allocator, "1234", .tail);
    try expectEqual(3, zl.len.get());

    const ql = try createFromZiplist(allocator, -2, 2, zl);
    defer ql.release(allocator);
    try expectEqual(3, ql.count);
}

test "insertBefore() | insertAfter() | index()" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "value2");
    _ = try ql.pushTail(allocator, "value3");
    _ = try ql.pushTail(allocator, "value4");

    var first: Entry = undefined;
    var found = try ql.index(allocator, 0, &first);
    try expect(found);
    try expectEqual(0, first.offset);
    try expectEqualStrings("value1", first.value.?[0..first.sz]);
    try ql.insertAfter(allocator, &first, "value1.1");
    try expectEqual(5, ql.count);

    var last: Entry = undefined;
    found = try ql.index(allocator, -1, &last);
    try expect(found);
    try expectEqual(-1, last.offset);
    try expectEqualStrings("value4", last.value.?[0..last.sz]);
    try ql.insertBefore(allocator, &last, "value3.3");
    try expectEqual(6, ql.count);
}

test "replaceAtIndex()" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "22");

    const replaced = try ql.replaceAtIndex(allocator, 1, "22");
    try expect(replaced);
    try expectEqual(2, ql.count);

    var entry: Entry = undefined;
    const found = try ql.index(allocator, 1, &entry);
    try expect(found);
    try expectEqual(22, entry.longval);
}

test "iterator.next(.head|.tail)" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "value2");
    _ = try ql.pushTail(allocator, "value3");
    _ = try ql.pushTail(allocator, "5");

    var entry: Entry = undefined;
    var it = ql.iterator(.head);
    var ok = try it.next(allocator, &entry);
    try expect(ok);
    try expectEqualStrings("value1", entry.value.?[0..entry.sz]);
    try expectEqual(0, entry.offset);
    try expect(try it.next(allocator, &entry));
    try expect(try it.next(allocator, &entry));
    try expect(try it.next(allocator, &entry));
    try expectEqual(5, entry.longval);
    try expectEqual(3, entry.offset);
    ok = try it.next(allocator, &entry);
    try expect(ok == false);
    try it.release(allocator);

    it = ql.iterator(.tail);
    ok = try it.next(allocator, &entry);
    try expect(ok);
    try expectEqual(5, entry.longval);
    try expectEqual(-1, entry.offset);
    try it.release(allocator);
}

test "iterator.delEntry(.head)" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "value2");
    _ = try ql.pushTail(allocator, "value3");
    _ = try ql.pushTail(allocator, "5");

    var entry: Entry = undefined;
    var it = ql.iterator(.head);
    it = ql.iterator(.head);
    var iterations: usize = 0;
    while (try it.next(allocator, &entry)) {
        try it.delEntry(allocator, &entry);
        iterations += 1;
    }
    try expectEqual(4, iterations);
    try expectEqual(0, ql.count);
    try expectEqual(0, ql.len);
    try it.release(allocator);
}

test "iterator.delEntry(.tail)" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "value2");
    _ = try ql.pushTail(allocator, "value3");
    _ = try ql.pushTail(allocator, "5");

    var entry: Entry = undefined;
    var it = ql.iterator(.tail);
    var iterations: usize = 0;
    while (try it.next(allocator, &entry)) {
        try it.delEntry(allocator, &entry);
        iterations += 1;
    }
    try expectEqual(4, iterations);
    try expectEqual(0, ql.count);
    try expectEqual(0, ql.len);
    try it.release(allocator);
}

test "iteratorAtIndex.delEntry(.head)" {
    const allocator = std.testing.allocator;
    var ql = try new(allocator, -2, 1);
    defer ql.release(allocator);

    _ = try ql.pushTail(allocator, "value1");
    _ = try ql.pushTail(allocator, "value2");
    _ = try ql.pushTail(allocator, "value3");
    _ = try ql.pushTail(allocator, "5");

    var entry: Entry = undefined;
    var it = (try ql.iteratorAtIndex(allocator, .head, 1)).?;
    var iterations: usize = 0;
    while (try it.next(allocator, &entry)) {
        iterations += 1;
        try it.delEntry(allocator, &entry);
    }
    try expectEqual(3, iterations);
    try expectEqual(1, ql.count);
    try expectEqual(1, ql.len);
    _ = try ql.index(allocator, 0, &entry);
    try expectEqualStrings("value1", entry.value.?[0..entry.sz]);
    try it.release(allocator);
}

const std = @import("std");
const ctypes = @import("ctypes.zig");
const ulong = ctypes.ulong;
const int = ctypes.int;
const uint = ctypes.uint;
const long = ctypes.long;
const longlong = ctypes.longlong;
const ulonglong = ctypes.ulonglong;
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
const util = @import("util.zig");
