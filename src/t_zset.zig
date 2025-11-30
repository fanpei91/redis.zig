// Input flags.
pub const ZADD_NONE = 0;
pub const ZADD_INCR = (1 << 0); // Increment the score instead of setting it.
pub const ZADD_NX = (1 << 1); // Don't touch elements not already existing.
pub const ZADD_XX = (1 << 2); // Only touch elements already existing.

// Output flags.
pub const ZADD_NOP = (1 << 3); // Operation not performed because of conditionals.
pub const ZADD_NAN = (1 << 4); // Only touch elements already existing.
pub const ZADD_ADDED = (1 << 5); // The element was new and was added.
pub const ZADD_UPDATED = (1 << 6); // The element already existed, score updated.

// Flags only used by the ZADD command but not by Zset.add() API:
pub const ZADD_CH = (1 << 16); // Return num of elements added or updated.

/// ZADD key [NX | XX] [CH] [INCR] score member [score member...]
pub fn zaddCommand(cli: *Client) void {
    zadd(cli, ZADD_NONE);
}

/// ZINCRBY key increment member
pub fn zincrbyCommand(cli: *Client) void {
    zadd(cli, ZADD_INCR);
}

/// Implements both ZADD and ZINCRBY.
fn zadd(cli: *Client, flags: i32) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // zig fmt: off
    var added: i64 = 0;     // Number of new elements added.
    var updated: i64 = 0;   // Number of elements with updated score.
    var processed: i64 = 0; // Number of elements processed, may remain zero
                            // with options like XX.
    // zig fmt: on

    // Parse options. At the end 'scoreidx' is set to the argument position
    // of the score of the first score-element pair.
    var scoreidx: usize = 2;
    var newflags = flags;
    while (scoreidx < cli.argc) {
        const opt = sds.asBytes(sds.cast(argv[scoreidx].v.ptr));
        if (eqlCase(opt, "nx")) {
            newflags |= ZADD_NX;
        } else if (eqlCase(opt, "xx")) {
            newflags |= ZADD_XX;
        } else if (eqlCase(opt, "ch")) {
            newflags |= ZADD_CH;
        } else if (eqlCase(opt, "incr")) {
            newflags |= ZADD_INCR;
        } else {
            break;
        }
        scoreidx += 1;
    }

    // Turn options into simple to check vars.
    const incr = newflags & ZADD_INCR != 0;
    const nx = newflags & ZADD_NX != 0;
    const xx = newflags & ZADD_XX != 0;
    const ch = newflags & ZADD_CH != 0;

    var elements = cli.argc - scoreidx;
    if (elements % 2 != 0 or elements == 0) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }
    // Now this holds the number of score-element pairs.
    elements = @divExact(elements, 2);

    // Check for incompatible options.
    if (nx and xx) {
        cli.addReplyErr("XX and NX options at the same time are not compatible");
        return;
    }

    if (incr and elements > 1) {
        cli.addReplyErr("INCR option supports a single increment-element pair");
        return;
    }

    // Start parsing all the scores, we need to emit any syntax error
    // before executing additions to the sorted set, as the command should
    // either execute fully or nothing at all.
    var scores = allocator.alloc(f64, elements);
    defer allocator.free(scores);
    for (0..elements) |i| {
        scores[i] = argv[scoreidx + i * 2].getDoubleOrReply(cli, null) orelse {
            return;
        };
    }

    var score: f64 = 0;
    biz: {
        const zobj = cli.db.lookupKeyWrite(key) orelse blk: {
            if (xx) break :biz;
            const member = sds.cast(argv[scoreidx + 1].v.ptr);
            var o: *Object = undefined;
            defer o.decrRefCount();
            if (server.zset_max_ziplist_entries == 0 or
                server.zset_max_ziplist_value < sds.getLen(member))
            {
                o = Object.createZset();
            } else {
                o = Object.createZsetZipList();
            }
            cli.db.add(key, o);
            break :blk o;
        };
        if (zobj.checkTypeOrReply(cli, .zset)) {
            return;
        }

        for (0..elements) |i| {
            score = scores[i];
            const ele = sds.cast(argv[scoreidx + i * 2 + 1].v.ptr);
            var retflags = newflags;
            var newscore: f64 = undefined;
            if (!Zset.add(zobj, score, ele, &retflags, &newscore)) {
                cli.addReplyErr("resulting score is not a number (NaN)");
                return;
            }
            if (retflags & ZADD_ADDED != 0) added += 1;
            if (retflags & ZADD_UPDATED != 0) updated += 1;
            if (retflags & ZADD_NOP == 0) processed += 1;
            score = newscore;
        }
    }

    // reply to client
    if (incr) {
        // ZINCRBY or INCR option
        if (processed > 0) {
            cli.addReplyDouble(score);
        } else {
            cli.addReply(Server.shared.nullbulk);
        }
    } else {
        // ZADD
        cli.addReplyLongLong(if (ch) added + updated else added);
    }
}

/// ZCARD key
pub fn zcardCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }
    cli.addReplyLongLong(@intCast(Zset.length(zobj)));
}

/// ZSCORE key member
pub fn zscoreCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    const member = sds.cast(argv[2].v.ptr);
    const score = Zset.scoreOf(zobj, member) orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    cli.addReplyDouble(score);
}

/// ZRANK key member
pub fn zrankCommand(cli: *Client) void {
    zrank(cli, false);
}

/// ZREVRANK key member
pub fn zrevrankCommand(cli: *Client) void {
    zrank(cli, true);
}

/// ZRANK/ZREVRANK key member
fn zrank(cli: *Client, reverse: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    const ele = sds.cast(argv[2].v.ptr);
    const ranking = Zset.rank(zobj, ele, reverse) orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    cli.addReplyLongLong(@intCast(ranking));
}

const Zset = struct {
    /// Add a new element or update the score of an existing element in a sorted
    /// set, regardless of its encoding.
    ///
    /// The set of flags change the command behavior. They are passed with an integer
    /// pointer since the function will clear the flags and populate them with
    /// other flags to indicate different conditions.
    ///
    /// The input flags are the following:
    ///
    /// ZADD_INCR: Increment the current element score by 'score' instead of updating
    ///            the current element score. If the element does not exist, we
    ///            assume 0 as previous score.
    /// ZADD_NX:   Perform the operation only if the element does not exist.
    /// ZADD_XX:   Perform the operation only if the element already exist.
    ///
    /// When ZADD_INCR is used, the new score of the element is stored in
    /// '*newscore' if 'newscore' is not NULL.
    ///
    /// The returned flags are the following:
    ///
    /// ZADD_NAN:     The resulting score is not a number.
    /// ZADD_ADDED:   The element was added (not present before the call).
    /// ZADD_UPDATED: The element score was updated.
    /// ZADD_NOP:     No operation was performed because of NX or XX.
    ///
    /// Return value:
    ///
    /// The function returns TRUE on success, and sets the appropriate flags
    /// ADDED or UPDATED to signal what happened during the operation (note that
    /// none could be set if we re-added an element using the same score it used
    /// to have, or in the case a zero increment is used).
    ///
    /// The function returns FALSE on erorr, currently only when the increment
    /// produces a NAN condition, or when the 'score' value is NAN since the
    /// start.
    ///
    /// The commad as a side effect of adding a new element may convert the sorted
    /// set internal encoding from ziplist to hashtable+skiplist.
    ///
    /// Memory managemnet of 'ele':
    ///
    /// The function does not take ownership of the 'ele' SDS string, but copies
    /// it if needed.
    pub fn add(
        zobj: *Object,
        score: f64,
        ele: sds.String,
        flags: *i32,
        newscore: ?*f64,
    ) bool {
        // Turn options into simple to check vars.
        const incr = flags.* & ZADD_INCR != 0;
        const nx = flags.* & ZADD_NX != 0;
        const xx = flags.* & ZADD_XX != 0;
        flags.* = 0; // We'll return our response flags.
        var inscore = score;

        // NaN as input is an error regardless of all the other parameters.
        if (isNan(score)) {
            flags.* |= ZADD_NAN;
            return false;
        }

        if (zobj.encoding == .ziplist) {
            var curscore: f64 = undefined;
            var zl: *ZipListSet = .cast(zobj.v.ptr);
            if (zl.find(ele, &curscore)) |eptr| {
                // NX? Return, same element already exists.
                if (nx) {
                    flags.* |= ZADD_NOP;
                    return true;
                }
                // Prepare the score for the increment if needed.
                if (incr) {
                    inscore += curscore;
                    if (isNan(inscore)) {
                        flags.* |= ZADD_NAN;
                        return false;
                    }
                    if (newscore) |new| new.* = inscore;
                }
                // Remove and re-insert when score changed.
                if (inscore != curscore) {
                    zl.delete(eptr);
                    zl.insert(ele, inscore);
                    flags.* |= ZADD_UPDATED;
                }
                return true;
            } else if (!xx) {
                // check if the element is too large or the list
                // becomes too long *before* executing insert.
                if (zl.length() + 1 > server.zset_max_ziplist_entries or
                    sds.getLen(ele) > server.zset_max_ziplist_value or
                    !ZipList.safeToAdd(zl.zl, sds.getLen(ele)))
                {
                    convert(zobj, .skiplist);
                } else {
                    zl.insert(ele, score);
                    if (newscore) |new| new.* = score;
                    flags.* |= ZADD_ADDED;
                    return true;
                }
            } else {
                flags.* |= ZADD_NOP;
                return true;
            }
        }

        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            if (sl.dict.find(ele)) |de| {
                // NX? Return, same element already exists.
                if (nx) {
                    flags.* |= ZADD_NOP;
                    return true;
                }
                const curscore = de.val.*;
                if (incr) {
                    inscore += curscore;
                    if (isNan(inscore)) {
                        flags.* |= ZADD_NAN;
                        return false;
                    }
                    if (newscore) |new| new.* = inscore;
                }
                // Remove and re-insert when score changes.
                if (inscore != curscore) {
                    const node = sl.zsl.updateScore(curscore, ele, inscore);
                    // Note that we did not removed the original element from
                    // the hash table representing the sorted set, so we just
                    // update the score.
                    de.val = &node.score;
                    flags.* |= ZADD_UPDATED;
                }
                return true;
            } else if (!xx) {
                const elm = sds.dupe(allocator.child, ele);
                const node = sl.zsl.insert(score, elm);
                const ok = sl.dict.add(elm, &node.score);
                assert(ok);
                flags.* |= ZADD_ADDED;
                if (newscore) |new| new.* = score;
                return true;
            }
            flags.* |= ZADD_NOP;
            return true;
        }

        @panic("Unknown sorted set encoding");
    }

    pub fn convert(zobj: *Object, encoding: Object.Encoding) void {
        if (zobj.encoding == encoding) return;

        if (zobj.encoding == .ziplist) {
            assert(encoding == .skiplist);

            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var eptr = zl.zl.index(0);
            assert(eptr != null);
            var sptr = zl.zl.next(eptr.?);
            assert(sptr != null);

            const sl = SkipListSet.create();
            while (eptr) |ep| {
                const score = ZipListSet.getScore(sptr.?);
                const ele = switch (ZipList.get(ep).?) {
                    .num => |v| sds.fromLonglong(allocator.child, v),
                    .str => |v| sds.new(allocator.child, v),
                };
                sl.insert(score, ele);
                zl.next(&eptr, &sptr);
            }

            zl.destroy();
            zobj.v = .{ .ptr = sl };
            zobj.encoding = .skiplist;
            return;
        }

        if (zobj.encoding == .skiplist) {
            assert(encoding == .ziplist);

            const sl: *SkipListSet = .cast(zobj.v.ptr);
            var node = sl.zsl.header.level(0).forward;
            allocator.destroy(sl.zsl.header);
            allocator.destroy(sl.zsl);
            sl.dict.destroy();
            allocator.destroy(sl);

            const zl = ZipListSet.create();
            while (node) |n| {
                zl.insertAt(null, n.ele.?, n.score);
                node = n.level(0).forward;
                allocator.destroy(n);
            }

            zobj.v = .{ .ptr = zl };
            zobj.encoding = .ziplist;
            return;
        }

        @panic("Unknown sorted set encoding");
    }

    pub fn length(zobj: *Object) u64 {
        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            return zl.length();
        }
        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            return sl.zsl.length;
        }
        @panic("Unknown sorted set encoding");
    }

    /// Given a sorted set object returns the 0-based rank of the object or
    /// NULL if the object does not exist.
    ///
    /// For rank we mean the position of the element in the sorted collection
    /// of elements. So the first element has rank 0, the second rank 1, and so
    /// forth up to length-1 elements.
    ///
    /// If 'reverse' is false, the rank is returned considering as first element
    /// the one with the lowest score. Otherwise if 'reverse' is non-zero
    /// the rank is computed considering as element with rank 0 the one with
    /// the highest score.
    fn rank(zobj: *Object, ele: sds.String, reverse: bool) ?u64 {
        const llen = length(zobj);

        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var eptr = zl.zl.index(0);
            assert(eptr != null);
            var sptr = zl.zl.next(eptr.?);
            assert(sptr != null);

            var ranking: u64 = 1;
            while (eptr) |ep| {
                if (ZipList.eql(ep, sds.asBytes(ele))) {
                    break;
                }
                ranking += 1;
                zl.next(&eptr, &sptr);
            }
            if (eptr != null) {
                if (reverse) {
                    return llen - ranking;
                }
                return ranking - 1;
            }
            return null;
        }

        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            const de = sl.dict.find(ele) orelse {
                return null;
            };
            const score = de.val.*;
            const ranking = sl.zsl.getRank(score, ele);
            // Existing elements always have a rank.
            assert(ranking != 0);
            if (reverse) {
                return llen - ranking;
            }
            return ranking - 1;
        }

        @panic("Unknown sorted set encoding");
    }

    /// Return the score of the specified member of the sorted set. If the
    /// element does not exist NULL is returned.
    pub fn scoreOf(zobj: *Object, member: sds.String) ?f64 {
        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var score: f64 = undefined;
            _ = zl.find(member, &score) orelse {
                return null;
            };
            return score;
        }
        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            const de = sl.dict.find(member) orelse {
                return null;
            };
            return de.val.*;
        }
        @panic("Unknown sorted set encoding");
    }
};

pub const ZipListSet = struct {
    zl: *ZipList,

    pub fn create() *ZipListSet {
        const szl = allocator.create(ZipListSet);
        szl.zl = ZipList.new();
        return szl;
    }

    pub fn cast(ptr: *anyopaque) *ZipListSet {
        return @ptrCast(@alignCast(ptr));
    }

    fn find(self: *ZipListSet, ele: sds.String, score: ?*f64) ?[*]u8 {
        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const sptr = self.zl.next(ep).?;
            if (ZipList.eql(ep, sds.asBytes(ele))) {
                // Matching element, pull out score.
                if (score) |s| s.* = getScore(sptr);
                return ep;
            }
            // Move to next element.
            eptr = self.zl.next(sptr);
        }
        return null;
    }

    pub fn getScore(sptr: [*]u8) f64 {
        const value = ZipList.get(sptr).?;
        return switch (value) {
            .num => |v| @floatFromInt(v),
            .str => |v| std.fmt.parseFloat(f64, v) catch unreachable,
        };
    }

    fn length(self: *ZipListSet) usize {
        return @divExact(self.zl.len.get(), 2);
    }

    /// Move to next entry based on the values in eptr and sptr. Both are set to
    /// NULL when there is no next entry.
    pub fn next(self: *ZipListSet, eptr: *?[*]u8, sptr: *?[*]u8) void {
        assert(eptr.* != null);
        assert(sptr.* != null);

        const ep = self.zl.next(sptr.*.?);
        if (ep) |ptr| {
            const sp = self.zl.next(ptr).?;
            eptr.* = ptr;
            sptr.* = sp;
            return;
        }
        eptr.* = null;
        sptr.* = null;
    }

    /// Delete (element,score) pair from ziplist.
    fn delete(self: *ZipListSet, eptr: [*]u8) void {
        var p = eptr;
        self.zl = self.zl.delete(&p);
        self.zl = self.zl.delete(&p);
    }

    /// Insert (element,score) pair in ziplist. This function assumes the
    /// element is not yet present in the list.
    fn insert(self: *ZipListSet, ele: sds.String, score: f64) void {
        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const sptr = self.zl.next(ep).?;
            const s = getScore(sptr);
            if (s > score) {
                // First element with score larger than score for element to be
                // inserted. This means we should take its spot in the list to
                // maintain ordering.
                self.insertAt(ep, ele, score);
                break;
            } else if (s == score) {
                // Ensure lexicographical ordering for elements.
                if (compareElements(ep, ele) == .gt) {
                    self.insertAt(ep, ele, score);
                    break;
                }
            }
            // Move to next element.
            eptr = self.zl.next(sptr);
        }

        // Push on tail of list when it was not yet inserted.
        if (eptr == null) {
            self.insertAt(null, ele, score);
        }
    }

    fn insertAt(
        self: *ZipListSet,
        eptr: ?[*]u8,
        ele: sds.String,
        score: f64,
    ) void {
        var buf: [128]u8 = undefined;
        const estr = sds.asBytes(ele);
        const sstr = util.d2string(&buf, score);
        if (eptr) |ep| {
            // Keep offset relative to zl, as it might be re-allocated.
            const offset = @intFromPtr(ep) - @intFromPtr(self.zl.addr());
            self.zl = self.zl.insert(ep, estr);
            var ptr = self.zl.addr() + offset;
            ptr = self.zl.next(ptr).?;
            // Insert score after the element.
            self.zl = self.zl.insert(ptr, sstr);
        } else {
            self.zl = self.zl.push(estr, .tail);
            self.zl = self.zl.push(sstr, .tail);
        }
    }

    /// Compare element in sorted set with given element.
    fn compareElements(eptr: [*]u8, ele: sds.String) std.math.Order {
        const value = ZipList.get(eptr).?;
        var buf: [32]u8 = undefined;
        const estr = switch (value) {
            .str => |v| v,
            .num => |v| util.ll2string(&buf, v),
        };
        return std.mem.order(u8, estr, sds.asBytes(ele));
    }

    pub fn destroy(self: *ZipListSet) void {
        self.zl.free();
        allocator.destroy(self);
    }
};

pub const SkipListSet = struct {
    zsl: *SkipList,
    dict: *dict.Dict(sds.String, *f64),

    pub fn create() *SkipListSet {
        const z = allocator.create(SkipListSet);
        z.dict = .create(&.{
            .hash = sds.hash,
            .eql = sds.eql,
        });
        z.zsl = SkipList.create();
        return z;
    }

    /// The inside skiplist takes ownership of the passed SDS string 'ele'.
    fn insert(self: *SkipListSet, score: f64, ele: sds.String) void {
        const node = self.zsl.insert(score, ele);
        const ok = self.dict.add(ele, &node.score);
        assert(ok);
    }

    pub fn cast(ptr: *anyopaque) *SkipListSet {
        return @ptrCast(@alignCast(ptr));
    }

    pub fn destroy(self: *SkipListSet) void {
        self.dict.destroy();
        self.zsl.free();
        allocator.destroy(self);
    }
};

// Should be enough for 2^64 elements.
pub const ZSKIPLIST_MAXLEVEL = 64;

pub const SkipList = struct {
    header: *Node,
    tail: ?*Node,
    length: u64,
    level: u32,

    const SizedNode = struct {
        size: usize,
        node: Node,
    };

    pub const Node = struct {
        ele: ?sds.String,
        score: f64,
        backward: ?*Node,

        const Level = struct {
            forward: ?*Node,
            span: u64,
        };

        pub fn create(num_level: u32, score: f64, ele: ?sds.String) *Node {
            const mem_size: usize = @sizeOf(SizedNode) + num_level * @sizeOf(Level);
            const mem = allocator.alignedAlloc(
                u8,
                .of(SizedNode),
                mem_size,
            );
            const sno: *SizedNode = @ptrCast(@alignCast(mem));
            sno.size = mem_size;
            sno.node = .{
                .ele = ele,
                .score = score,
                .backward = null,
            };
            return &sno.node;
        }

        pub inline fn level(self: *const Node, i: usize) *Level {
            const offset = @sizeOf(Node) + @sizeOf(Level) * i;
            return @ptrFromInt(@intFromPtr(self) + offset);
        }

        pub fn free(self: *Node) void {
            if (self.ele) |ele| {
                sds.free(allocator.child, ele);
            }
            const parent: *SizedNode = @fieldParentPtr("node", self);
            const mem: [*]align(@alignOf(SizedNode)) u8 = @ptrCast(@alignCast(parent));
            allocator.free(mem[0..parent.size]);
        }
    };

    pub fn create() *SkipList {
        const sl = allocator.create(SkipList);
        const header = Node.create(ZSKIPLIST_MAXLEVEL, 0, null);
        for (0..ZSKIPLIST_MAXLEVEL) |i| {
            const lvl = header.level(i);
            lvl.forward = null;
            lvl.span = 0;
        }
        sl.* = .{
            .header = header,
            .tail = null,
            .length = 0,
            .level = 1,
        };
        return sl;
    }

    /// Insert a new node in the skiplist. Assumes the element does not already
    /// exist (up to the caller to enforce that). The skiplist takes ownership
    /// of the passed SDS string 'ele'.
    pub fn insert(self: *SkipList, score: f64, ele: sds.String) *Node {
        assert(!isNan(score));

        var rank: [ZSKIPLIST_MAXLEVEL]u64 = undefined;
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            rank[i] = if (i == self.level - 1) 0 else rank[i + 1];
            while (x.level(i).forward) |forward| {
                if (forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele) == .lt)
                {
                    rank[i] += x.level(i).span;
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        const level = zslRandomLevel();
        if (level > self.level) {
            i = self.level;
            while (i < level) : (i += 1) {
                rank[i] = 0;
                update[i] = self.header;
                update[i].level(i).span = @intCast(self.length);
            }
            self.level = level;
        }

        x = Node.create(level, score, ele);
        i = 0;
        while (i < level) : (i += 1) {
            x.level(i).forward = update[i].level(i).forward;
            update[i].level(i).forward = x;

            x.level(i).span = update[i].level(i).span - (rank[0] - rank[i]);
            update[i].level(i).span = (rank[0] - rank[i]) + 1;
        }

        i = level;
        while (i < self.level) : (i += 1) {
            update[i].level(i).span += 1;
        }

        x.backward = if (update[0] == self.header) null else update[0];
        if (x.level(0).forward) |forward| {
            forward.backward = x;
        } else {
            self.tail = x;
        }
        self.length += 1;
        return x;
    }

    /// Update the score of an elmenent. Note that the element must exist and
    /// must match 'score'.
    ///
    /// Note that this function attempts to just update the node, in case after
    /// the score update, the node would be exactly at the same position.
    /// Otherwise the skiplist is modified by removing and re-adding a new
    /// element, which is more costly.
    ///
    /// The function returns the updated element skiplist node pointer.
    fn updateScore(
        self: *SkipList,
        curscore: f64,
        ele: sds.String,
        newscore: f64,
    ) *Node {
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;

        // We need to seek to element to update to start: this is useful anyway,
        // we'll have to update or remove it.
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (forward.score < curscore or
                    (forward.score == curscore and
                        sds.cmp(forward.ele.?, ele).compare(.lt)))
                {
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // Jump to our element: note that this function assumes that the
        // element with the matching score exists.
        x = x.level(0).forward.?;
        assert(curscore == x.score and sds.eql(x.ele.?, ele));

        // If the node, after the score update, would be still exactly
        // at the same position, we can just update the score without
        // actually removing and re-inserting the element in the skiplist.
        if ((x.backward == null or x.backward.?.score < newscore) and
            (x.level(0).forward == null or x.level(0).forward.?.score > newscore))
        {
            x.score = newscore;
            return x;
        }

        // No way to reuse the old node: we need to remove and insert a new
        // one at a different place.
        self.deleteNode(x, &update);
        const newnode = self.insert(newscore, x.ele.?);

        // We reused the old node x.ele SDS string, free the node now
        // since insert() created a new one.
        x.ele = null;
        x.free();

        return newnode;
    }

    pub fn delete(self: *SkipList, score: f64, ele: sds.String) bool {
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele) == .lt)
                {
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // We may have multiple elements with the same score, what we need
        // is to find the element with both the right score and sds.
        if (x.level(0).forward) |forward| {
            if (score == forward.score and sds.eql(forward.ele.?, ele)) {
                self.deleteNode(forward, &update);
                forward.free();
                return true;
            }
        }
        return false;
    }

    /// Find the rank for an element by both score and key.
    /// Returns 0 when the element cannot be found, rank otherwise.
    /// Note that the rank is 1-based due to the span of SkipList.header
    /// to the first element.
    pub fn getRank(self: *const SkipList, score: f64, ele: sds.String) usize {
        var rank: u64 = 0;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if ((forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele).compare(.lte)))
                {
                    rank += x.level(i).span;
                    x = forward;
                    continue;
                }
                break;
            }

            // x might be equal to self.header, so test if ele is non-NULL
            if (x.ele) |xe| if (sds.eql(xe, ele)) {
                return rank;
            };

            if (i == 0) break;
        }
        return rank;
    }

    pub fn firstInRange(self: *const SkipList, range: *const Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (!range.minLte(forward.score)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        // This is an inner range, so the next node cannot be NULL.
        x = x.level(0).forward.?;
        if (!range.maxGte(x.score)) return null;
        return x;
    }

    pub fn lastInRange(self: *const SkipList, range: *const Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (range.maxGte(forward.score)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        if (!range.minLte(x.score)) return null;
        return x;
    }

    pub fn free(self: *SkipList) void {
        var node = self.header.level(0).forward;
        self.header.free();
        while (node) |n| {
            node = n.level(0).forward;
            n.free();
        }
        allocator.destroy(self);
    }

    /// Returns if there is a part of the skiplist is in range.
    fn isInRange(self: *const SkipList, range: *const Range) bool {
        if (range.min > range.max) return false;
        if (range.min == range.max and (range.minex or range.maxex)) {
            return false;
        }

        const last = self.tail;
        if (last == null or !range.minLte(last.?.score)) return false;

        const first = self.header.level(0).forward;
        if (first == null or !range.maxGte(first.?.score)) return false;

        return true;
    }

    fn deleteNode(self: *SkipList, x: *Node, update: []*Node) void {
        var i: u32 = 0;
        while (i < self.level) : (i += 1) {
            if (update[i].level(i).forward == x) {
                // update[i].level(i).span += x.level(i).span - 1;
                // This is for avoiding integer overflow because `x` maybe is
                // the last node with 0 span.
                update[i].level(i).span += x.level(i).span;
                update[i].level(i).span -= 1;
                update[i].level(i).forward = x.level(i).forward;
            } else {
                update[i].level(i).span -= 1;
            }
        }
        if (x.level(0).forward) |forward| {
            forward.backward = x.backward;
        } else {
            self.tail = x.backward;
        }

        while (self.level > 1 and
            self.header.level(self.level - 1).forward == null)
        {
            self.level -= 1;
        }
        self.length -= 1;
    }
};

fn zslRandomLevel() u32 {
    const P = 0.25;
    var level: u32 = 1;
    while (random.int(u16) < @as(u16, @intFromFloat(P * 0xFFFF))) {
        level += 1;
    }
    return @min(level, ZSKIPLIST_MAXLEVEL);
}

/// Hold a inclusive/exclusive range by score comparison.
pub const Range = struct {
    min: f64,
    max: f64,

    // Are min or max exclusive?
    minex: bool,
    maxex: bool,

    pub fn minLte(range: *const Range, value: f64) bool {
        return if (range.minex)
            range.min < value
        else
            range.min <= value;
    }

    pub fn maxGte(range: *const Range, value: f64) bool {
        return if (range.maxex)
            range.max > value
        else
            range.max >= value;
    }
};

test zslRandomLevel {
    const level = zslRandomLevel();
    try expect(level >= 1);
}

test SkipList {
    var sl = SkipList.create();
    defer sl.free();

    var ele = sds.new(allocator.child, "score 1");
    try expect(sl.getRank(1, ele) == 0);
    const score1 = sl.insert(
        1,
        ele,
    );
    try expect(score1.backward == null);
    try expect(sl.length == 1);
    try expect(sl.getRank(1, ele) == 1);

    ele = sds.new(allocator.child, "score 2");
    var score2 = sl.insert(
        2,
        ele,
    );
    try expect(sl.header.level(0).forward.? == score1);
    try expect(sl.tail == score2);
    try expect(sl.length == 2);

    var range = Range{
        .min = 1,
        .max = 2,
        .minex = false,
        .maxex = false,
    };
    const first = sl.firstInRange(&range);
    try expect(first != null);
    try expect(first.? == score1);
    var last = sl.lastInRange(&range);
    try expect(last != null);
    try expect(last.? == score2);
    range.maxex = true;
    last = sl.lastInRange(&range);
    try expect(last != null);
    try expect(last.? == score1);

    score2 = sl.insert(
        2,
        sds.new(allocator.child, "score 2.0"),
    );
    try expect(sl.tail == score2);
    try expect(sl.length == 3);

    ele = sds.new(allocator.child, "deleted");
    _ = sl.insert(3, ele);
    try expect(sl.length == 4);
    const deleted = sl.delete(3, ele);
    try expect(deleted);
    try expect(sl.length == 3);
    try expect(sl.tail.?.score == 2);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const assert = std.debug.assert;
const random = @import("random.zig");
const isNan = std.math.isNan;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const sds = @import("sds.zig");
const Server = @import("Server.zig");
const dict = @import("dict.zig");
const hasher = @import("hasher.zig");
const Client = @import("networking.zig").Client;
const eqlCase = std.ascii.eqlIgnoreCase;
const Object = @import("Object.zig");
const server = &Server.instance;
const ZipList = @import("ZipList.zig");
const util = @import("util.zig");
