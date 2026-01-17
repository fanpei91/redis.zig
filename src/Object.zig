/// Object command allows to inspect the internals of an Redis Object.
/// OBJECT <refcount|encoding|idletime|freq> key
pub fn objectCommand(cli: *Client) void {
    const help: []const []const u8 = &.{
        "ENCODING <key>",
        "    Return the kind of internal representation used in order to store the value",
        "    associated with a <key>.",
        "FREQ <key>",
        "    Return the access frequency index of the <key>. The returned integer is",
        "    proportional to the logarithm of the recent access frequency of the key.",
        "IDLETIME <key>",
        "    Return the idle time of the <key>, that is the approximated number of",
        "    seconds elapsed since the last access to the key.",
        "REFCOUNT <key>",
        "    Return the number of references of the value associated with the specified",
        "    <key>.",
        "HELP",
        "    Print this help.",
    };

    const argv = cli.argv.?;
    const subcmd = sds.castBytes(argv[1].v.ptr);
    const eqlCase = std.ascii.eqlIgnoreCase;

    if (cli.argc == 2 and eqlCase(subcmd, "help")) {
        cli.addReplyHelp(help);
        return;
    }

    if (cli.argc == 3) {
        const key = argv[2];
        const obj = key.commandLookupOrReply(
            cli,
            Server.shared.nullbulk,
        ) orelse {
            return;
        };

        if (eqlCase(subcmd, "refcount")) {
            cli.addReplyLongLong(obj.refcount);
            return;
        }
        if (eqlCase(subcmd, "encoding")) {
            cli.addReplyBulkString(obj.encoding.toString());
            return;
        }
        if (eqlCase(subcmd, "idletime")) {
            if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
                // zig fmt: off
                cli.addReplyErr(
                    "An LFU maxmemory policy is selected, " ++
                    "idle time not tracked. " ++
                    "Please note that when switching between policies at " ++
                    "runtime LRU and LFU data will take some time to adjust.",
                );
                // zig fmt: on
                return;
            }
            const idle = evict.estimateObjectIdleTime(obj) / std.time.ms_per_s;
            cli.addReplyLongLong(@intCast(idle));
            return;
        }
        if (eqlCase(subcmd, "freq")) {
            if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU == 0) {
                // zig fmt: off
                cli.addReplyErr("An LFU maxmemory policy is not selected, " ++
                    "access frequency not tracked. " ++
                    "Please note that when switching between policies at " ++
                    "runtime LRU and LFU data will take some time to adjust.",
                );
                // zig fmt: on
                return;
            }
            // LFUDecrAndReturn should be called
            // in case of the key has not been accessed for a long time,
            // because we update the access time only
            // when the key is read or overwritten.
            cli.addReplyLongLong(@intCast(evict.LFUDecrAndReturn(obj)));
            return;
        }
    }
    cli.addReplySubcommandSyntaxError();
}

pub const Type = enum(u4) {
    string = 0,
    list = 1,
    set = 2,
    zset = 3,
    hash = 4,
    module = 5,
    stream = 6,
};

pub const Encoding = enum(u4) {
    raw = 0,
    int = 1,
    ht = 2,
    zipmap = 3,
    linkedlist = 4, // No longer used: old list encoding.
    ziplist = 5,
    intset = 6,
    skiplist = 7,
    embstr = 8,
    quicklist = 9,
    stream = 10,

    pub fn toString(self: Encoding) []const u8 {
        return switch (self) {
            .raw => "raw",
            .int => "int",
            .ht => "hashtable",
            .zipmap => "zipmap",
            .linkedlist => "linkedlist",
            .ziplist => "ziplist",
            .intset => "intset",
            .skiplist => "skiplist",
            .embstr => "embstr",
            .quicklist => "quicklist",
            .stream => "stream",
        };
    }
};

pub const Shared = struct {
    crlf: *Object,
    ok: *Object,
    err: *Object,
    emptybulk: *Object,
    czero: *Object,
    cone: *Object,
    cnegone: *Object,
    nullbulk: *Object,
    nullmultibulk: *Object,
    emptymultibulk: *Object,
    pong: *Object,
    queued: *Object,
    emptyscan: *Object,
    wrongtypeerr: *Object,
    nokeyerr: *Object,
    syntaxerr: *Object,
    sameobjecterr: *Object,
    outofrangeerr: *Object,
    noscripterr: *Object,
    loadingerr: *Object,
    noautherr: *Object,
    oomerr: *Object,
    execaborterr: *Object,
    busykeyerr: *Object,
    space: *Object,
    colon: *Object,
    plus: *Object,
    subscribebulk: *Object,
    psubscribebulk: *Object,
    unsubscribebulk: *Object,
    punsubscribebulk: *Object,
    messagebulk: *Object,
    pmessagebulk: *Object,
    set: *Object,
    hset: *Object,
    unlink: *Object,
    del: *Object,
    rpoplpush: *Object,
    lpop: *Object,
    lpush: *Object,
    rpop: *Object,
    srem: *Object,
    zpopmin: *Object,
    zpopmax: *Object,
    multi: *Object,
    integers: [Server.OBJ_SHARED_INTEGERS]*Object,
    bulkhdr: [Server.OBJ_SHARED_BULKHDR_LEN]*Object, // $<value>\r\n
    mbulkhdr: [Server.OBJ_SHARED_BULKHDR_LEN]*Object, // *<value>\r\n
    minstring: sds.String,
    maxstring: sds.String,

    pub fn create() Shared {
        var self: Shared = undefined;
        self.crlf = Object.create(
            .string,
            sds.new(allocator.impl, "\r\n"),
        );
        self.ok = Object.create(
            .string,
            sds.new(allocator.impl, "+OK\r\n"),
        );
        self.err = Object.create(
            .string,
            sds.new(allocator.impl, "-ERR\r\n"),
        );
        self.emptybulk = Object.create(
            .string,
            sds.new(allocator.impl, "$0\r\n\r\n"),
        );
        self.czero = Object.create(
            .string,
            sds.new(allocator.impl, ":0\r\n"),
        );
        self.cone = Object.create(
            .string,
            sds.new(allocator.impl, ":1\r\n"),
        );
        self.cnegone = Object.create(
            .string,
            sds.new(allocator.impl, ":-1\r\n"),
        );
        self.nullbulk = Object.create(
            .string,
            sds.new(allocator.impl, "$-1\r\n"),
        );
        self.nullmultibulk = Object.create(
            .string,
            sds.new(allocator.impl, "*-1\r\n"),
        );
        self.emptymultibulk = Object.create(
            .string,
            sds.new(allocator.impl, "*0\r\n"),
        );
        self.pong = Object.create(
            .string,
            sds.new(allocator.impl, "+PONG\r\n"),
        );
        self.queued = Object.create(
            .string,
            sds.new(allocator.impl, "+QUEUED\r\n"),
        );
        self.emptyscan = Object.create(
            .string,
            sds.new(allocator.impl, "*2\r\n$1\r\n0\r\n*0\r\n"),
        );
        self.wrongtypeerr = Object.create(
            .string,
            sds.new(
                allocator.impl,
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            ),
        );
        self.nokeyerr = Object.create(
            .string,
            sds.new(allocator.impl, "-ERR no such key\r\n"),
        );
        self.syntaxerr = Object.create(
            .string,
            sds.new(allocator.impl, "-ERR syntax error\r\n"),
        );
        self.sameobjecterr = Object.create(
            .string,
            sds.new(
                allocator.impl,
                "-ERR source and destination objects are the same\r\n",
            ),
        );
        self.outofrangeerr = Object.create(
            .string,
            sds.new(allocator.impl, "-ERR index out of range\r\n"),
        );
        self.noscripterr = Object.create(
            .string,
            sds.new(allocator.impl, "-NOSCRIPT No matching script. Please use EVAL.\r\n"),
        );
        self.loadingerr = Object.create(
            .string,
            sds.new(allocator.impl, "-LOADING Redis is loading the dataset in memory\r\n"),
        );
        self.noautherr = Object.create(
            .string,
            sds.new(allocator.impl, "-NOAUTH Authentication required.\r\n"),
        );
        self.oomerr = Object.create(
            .string,
            sds.new(
                allocator.impl,
                "-OOM command not allowed when used memory > 'maxmemory'.\r\n",
            ),
        );
        self.execaborterr = Object.create(
            .string,
            sds.new(
                allocator.impl,
                "-EXECABORT Transaction discarded because of previous errors.\r\n",
            ),
        );
        self.busykeyerr = Object.create(
            .string,
            sds.new(allocator.impl, "-BUSYKEY Target key name already exists.\r\n"),
        );
        self.space = Object.create(
            .string,
            sds.new(allocator.impl, " "),
        );
        self.colon = Object.create(
            .string,
            sds.new(allocator.impl, ":"),
        );
        self.plus = Object.create(
            .string,
            sds.new(allocator.impl, "+"),
        );
        self.subscribebulk = Object.createString("$9\r\nsubscribe\r\n");
        self.psubscribebulk = Object.createString("$10\r\npsubscribe\r\n");
        self.unsubscribebulk = Object.createString("$11\r\nunsubscribe\r\n");
        self.punsubscribebulk = Object.createString("$12\r\npunsubscribe\r\n");
        self.messagebulk = Object.createString("$7\r\nmessage\r\n");
        self.pmessagebulk = Object.createString("$8\r\npmessage\r\n");
        self.set = Object.createString("SET");
        self.hset = Object.createString("HSET");
        self.unlink = Object.createString("UNLINK");
        self.del = Object.createString("DEL");
        self.rpoplpush = Object.createString("RPOPLPUSH");
        self.lpop = Object.createString("LPOP");
        self.lpush = Object.createString("LPUSH");
        self.rpop = Object.createString("RPOP");
        self.srem = Object.createString("SREM");
        self.zpopmin = Object.createString("ZPOPMIN");
        self.zpopmax = Object.createString("ZPOPMAX");
        self.multi = Object.createString("MULTI");
        for (0..Server.OBJ_SHARED_INTEGERS) |i| {
            var obj = Object.createInt(@intCast(i));
            self.integers[i] = obj.makeShared();
        }
        for (0..Server.OBJ_SHARED_BULKHDR_LEN) |i| {
            self.bulkhdr[i] = Object.create(
                .string,
                sds.catPrintf(
                    allocator.impl,
                    sds.empty(allocator.impl),
                    "${}\r\n",
                    .{i},
                ),
            );
            self.mbulkhdr[i] = Object.create(
                .string,
                sds.catPrintf(
                    allocator.impl,
                    sds.empty(allocator.impl),
                    "*{}\r\n",
                    .{i},
                ),
            );
        }
        self.minstring = sds.new(allocator.impl, "minstring");
        self.maxstring = sds.new(allocator.impl, "maxstring");
        return self;
    }
};

const Object = @This();
type: Type,
encoding: Encoding,
lru: std.meta.Int(.unsigned, Server.LRU_BITS),
refcount: i32,
v: union {
    ptr: *anyopaque,
    int: i64,
},

pub fn create(typ: Type, ptr: *anyopaque) *Object {
    const obj = allocator.create(Object);
    obj.type = typ;
    obj.encoding = .raw;
    // Set the LRU to the current lruclock (minutes resolution), or
    // alternatively the LFU counter.
    if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
        obj.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | evict.LFU_INIT_VAL);
    } else {
        obj.lru = @intCast(evict.LRUClock());
    }
    obj.refcount = 1;
    obj.v = .{ .ptr = ptr };

    return obj;
}

pub fn createInt(int: i64) *Object {
    const obj = create(.string, @ptrFromInt(1));
    obj.encoding = .int;
    obj.v = .{ .int = int };
    return obj;
}

/// Initialize a object allocated on the stack.
pub fn initStaticStringObject(obj: *Object, s: sds.String) void {
    obj.refcount = 1;
    obj.type = .string;
    obj.encoding = .raw;
    obj.v = .{ .ptr = s };
}

pub const SHARED_REFCOUNT = maxInt(i32);
/// Set a special refcount in the object to make it "shared":
/// incrRefCount() and decrRefCount() will test for this special refcount
/// and will not touch the object. This way it is free to access shared
/// objects such as small integers from different threads without any
/// mutex.
///
/// A common patter to create shared objects:
///
///  myobject: *Object = makeShared(create(...));
pub fn makeShared(self: *Object) *Object {
    self.refcount = SHARED_REFCOUNT;
    return self;
}

// TODO: This needs to be adjusted according to the allocator used.
// For now, just use the 44 that Redis has chosen for jemalloc.
const ENCODING_EMBSTR_SIZE_LIMIT = 44;
pub fn createString(str: []const u8) *Object {
    if (str.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        return createEmbeddedString(str);
    }
    return createRawString(str);
}

pub fn createRawString(str: []const u8) *Object {
    const s = sds.newLen(allocator.impl, str.ptr, str.len);
    return create(.string, s);
}

fn createEmbeddedString(str: []const u8) *Object {
    const mem_size = @sizeOf(Object) + @sizeOf(sds.Hdr8) + str.len + 1;
    const mem = allocator.alignedAlloc(u8, .of(Object), mem_size);

    const sh: *sds.Hdr8 = @ptrFromInt(@intFromPtr(mem.ptr) + @sizeOf(Object));
    sh.len = @intCast(str.len);
    sh.alloc = @intCast(str.len);
    sh.flags = sds.TYPE_8;
    const s: sds.String = @ptrFromInt(@intFromPtr(sh) + @sizeOf(sds.Hdr8));
    if (@intFromPtr(str.ptr) != @intFromPtr(sds.NOINIT.ptr)) {
        memcpy(s, str, str.len);
    }
    s[str.len] = 0;

    const obj: *Object = @ptrCast(@alignCast(mem));
    obj.type = .string;
    obj.encoding = .embstr;
    if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
        obj.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | evict.LFU_INIT_VAL);
    } else {
        obj.lru = @intCast(evict.LRUClock());
    }
    obj.refcount = 1;
    obj.v = .{ .ptr = s };

    return obj;
}

pub fn createFromStreamID(id: *const Stream.Id) *Object {
    return create(
        .string,
        sds.catPrintf(
            allocator.impl,
            sds.empty(allocator.impl),
            "{}-{}",
            .{ id.ms, id.seq },
        ),
    );
}

/// Always demanding to create a shared object if possible.
pub fn createStringFromLonglong(value: i64) *Object {
    return createStringFromLonglongWithOptions(
        value,
        true,
    );
}

/// Avoiding a shared object when LFU/LRU info are needed, that is, when the
/// object is used as a value in the key space, and Redis is configured to evict
/// based on LFU/LRU.
pub fn createStringFromLonglongForValue(value: i64) *Object {
    return createStringFromLonglongWithOptions(
        value,
        false,
    );
}

/// Create a string object from a long long value. When possible returns a
/// shared integer object, or at least an integer encoded one.
///
/// If `from_shared` is false, the function avoids returning a a shared
/// integer, because the object is going to be used as value in the Redis key
/// space (for instance when the INCR command is used), so we want LFU/LRU
/// values specific for each key.
fn createStringFromLonglongWithOptions(value: i64, from_shared: bool) *Object {
    var enabled = from_shared;
    if (Server.instance.maxmemory == 0 or
        (Server.instance.maxmemory_policy &
            Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0)
    {
        // If the maxmemory policy permits, we can still return shared integers
        // even if `from_shared` is false.
        enabled = true;
    }
    if (value >= 0 and value < Server.OBJ_SHARED_INTEGERS and enabled) {
        const o = Server.shared.integers[@abs(value)];
        return o.incrRefCount();
    }

    if (value >= minInt(i64) and value <= maxInt(i64)) {
        const o = createInt(value);
        return o;
    }

    const s = sds.fromLonglong(allocator.impl, value);
    return create(.string, s);
}

pub fn createStringFromLongDouble(value: f80, humanfriendly: bool) *Object {
    var buf: [util.MAX_LONG_DOUBLE_CHARS]u8 = undefined;
    const str = util.ld2string(&buf, value, humanfriendly);
    return createString(str);
}

pub fn dupeString(self: *const Object) *Object {
    assert(self.type == .string);

    switch (self.encoding) {
        .raw => {
            return createRawString(sds.castBytes(self.v.ptr));
        },
        .embstr => {
            return createEmbeddedString(sds.castBytes(self.v.ptr));
        },
        .int => {
            const o = createInt(self.v.int);
            return o;
        },
        else => @panic("Wrong encoding."),
    }
}

pub fn compareStrings(self: *Object, other: *Object) std.math.Order {
    assert(self.type == .string);
    assert(other.type == .string);

    if (self == other) return .eq;

    if (self.encoding == .int and other.encoding == .int) {
        return std.math.order(
            self.v.int,
            other.v.int,
        );
    }

    assert(self.encoding != .int);
    assert(other.encoding != .int);

    const a: sds.String = @ptrCast(self.v.ptr);
    const b: sds.String = @ptrCast(other.v.ptr);
    return sds.cmp(a, b);
}

pub fn equalStrings(self: *Object, other: *Object) bool {
    if (self.encoding == .int and other.encoding == .int) {
        return self.v.int == other.v.int;
    }
    return self.compareStrings(other) == .eq;
}

pub fn createQuickList() *Object {
    const ql = QuickList.create();
    const obj = create(.list, ql);
    obj.encoding = .quicklist;
    return obj;
}

pub fn createZipList() *Object {
    const zl = ZipList.new();
    const obj = create(.list, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createIntSet() *Object {
    const is = IntSet.new();
    const obj = create(.set, is);
    obj.encoding = .intset;
    return obj;
}

pub fn createSet() *Object {
    const d = set.create();
    const obj = create(.set, d);
    obj.encoding = .ht;
    return obj;
}

pub fn createHash() *Object {
    const zl = ZipList.new();
    const obj = create(.hash, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createZset() *Object {
    const zs = SkipListSet.create();
    const obj = create(.zset, zs);
    obj.encoding = .skiplist;
    return obj;
}

pub fn createZsetZipList() *Object {
    const zl = ZipListSet.create();
    const obj = create(.zset, zl);
    obj.encoding = .ziplist;
    return obj;
}

pub fn createStream() *Object {
    const s = Stream.create();
    const obj = create(.stream, s);
    obj.encoding = .stream;
    return obj;
}

pub fn decrRefCount(self: *Object) void {
    if (self.refcount == 1) {
        switch (self.type) {
            .string => self.freeString(),
            .list => self.freeList(),
            .set => self.freeSet(),
            .zset => self.freeZset(),
            .hash => self.freeHash(),
            .stream => self.freeStream(),
            else => unreachable,
        }
        self.free();
        return;
    }
    if (self.refcount <= 0) @panic("Object.decrRefCount against refcount <= 0");
    if (self.refcount != SHARED_REFCOUNT) self.refcount -= 1;
}

pub fn incrRefCount(self: *Object) *Object {
    if (self.refcount != SHARED_REFCOUNT) self.refcount += 1;
    return self;
}

pub fn resetRefCount(self: *Object) *Object {
    self.refcount = 0;
    return self;
}

/// Set the object LRU/LFU depending on server.maxmemory_policy.
/// The lfu_freq arg is only relevant if policy is MAXMEMORY_FLAG_LFU.
/// The lru_idle and lru_clock args are only relevant if policy
/// is MAXMEMORY_FLAG_LRU.
/// Either or both of them may be <0, in that case, nothing is set.
pub fn setLRUOrLFU(
    val: *Object,
    lfu_freq: i64,
    lru_idle: i64,
    lru_clock: i64,
) void {
    if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
        if (lfu_freq >= 0) {
            assert(lfu_freq <= 255);
            const freq: u8 = @intCast(lfu_freq);
            val.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | freq);
        }
    } else if (lru_idle >= 0) {
        // Provided LRU idle time is in seconds. Scale
        // according to the LRU clock resolution this Redis
        // instance was compiled with (normally 1000 ms, so the
        // below statement will expand to lru_idle*1000/1000.
        const idle = @divFloor(
            lru_idle * std.time.ms_per_s,
            Server.LRU_CLOCK_RESOLUTION,
        );
        var lru_abs = lru_clock - idle;
        // If the LRU field underflows (since LRU it is a wrapping
        // clock), the best we can do is to provide a large enough LRU
        // that is half-way in the circlular LRU clock we use: this way
        // the computed idle time for this object will stay high for quite
        // some time.
        if (lru_abs < 0) {
            lru_abs = @rem(
                (lru_clock + (@divFloor(Server.LRU_CLOCK_MAX, 2))),
                Server.LRU_CLOCK_MAX,
            );
        }
        val.lru = @intCast(lru_abs);
    }
}

pub fn stringLen(self: *Object) usize {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return sds.getLen(sds.cast(self.v.ptr));
    }
    return util.sdigits10(self.v.int);
}

/// Get a decoded version of an encoded object (returned as a new object).
/// If the object is already raw-encoded just increment the ref count.
pub fn getDecoded(self: *Object) *Object {
    if (self.sdsEncoded()) {
        return self.incrRefCount();
    }
    if (self.type == .string and self.encoding == .int) {
        var buf: [20]u8 = undefined;
        const digits = util.ll2string(&buf, self.v.int);
        return createString(digits);
    }
    @panic("Unknown encoding type");
}

/// Try to encode a string object in order to save space.
pub fn tryEncoding(self: *Object) *Object {
    assert(self.type == .string);

    if (!self.sdsEncoded()) return self;
    if (self.refcount > 1) return self;

    const slice = sds.castBytes(self.v.ptr);
    if (slice.len <= 20) if (util.string2ll(slice)) |value| {
        const use_shared_integers = (Server.instance.maxmemory == 0 or
            (Server.instance.maxmemory_policy & Server.MAXMEMORY_FLAG_NO_SHARED_INTEGERS) == 0);
        if (use_shared_integers and value > 0 and value < Server.OBJ_SHARED_INTEGERS) {
            self.decrRefCount();
            const obj = Server.shared.integers[@as(usize, @intCast(value))];
            return obj.incrRefCount();
        } else {
            if (self.encoding == .raw) {
                sds.free(allocator.impl, sds.cast(self.v.ptr));
                self.encoding = .int;
                self.v = .{ .int = value };
                return self;
            } else if (self.encoding == .embstr) {
                self.decrRefCount();
                return createStringFromLonglongForValue(value);
            }
        }
    };
    // If the string is small and is still RAW encoded,
    // try the EMBSTR encoding which is more efficient.
    // In this representation the object and the SDS string are allocated
    // in the same chunk of memory to save space and cache misses.
    if (slice.len <= ENCODING_EMBSTR_SIZE_LIMIT) {
        if (self.encoding == .embstr) return self;
        const emb = createEmbeddedString(slice);
        self.decrRefCount();
        return emb;
    }

    // We can't encode the object...
    //
    // Do the last try, and at least optimize the SDS string inside
    // the string object to require little space, in case there
    // is more than 10% of free space at the end of the SDS string.
    //
    // We do that only for relatively large strings as this branch
    // is only entered if the length of the string is greater than
    // ENCODING_EMBSTR_SIZE_LIMIT.
    self.trimStringIfNeeded();

    return self;
}

/// Optimize the SDS string inside the string object to require little space,
/// in case there is more than 10% of free space at the end of the SDS
/// string. This happens because SDS strings tend to overallocate to avoid
/// wasting too much time in allocations when appending to the string.
pub fn trimStringIfNeeded(self: *Object) void {
    if (self.encoding != .raw) {
        return;
    }
    var s = sds.cast(self.v.ptr);
    if (sds.getAvail(s) > sds.getLen(s) / 10) {
        s = sds.removeAvailSpace(allocator.impl, s);
        self.v = .{ .ptr = s };
    }
}

pub fn sdsEncoded(self: *const Object) bool {
    return self.encoding == .raw or self.encoding == .embstr;
}

/// This is a function for the OBJECT command.
pub fn commandLookupOrReply(
    key: *const Object,
    cli: *Client,
    reply: *const Object,
) ?*Object {
    // We need to lookup keys without any modification of LRU or other parameters.
    const de = cli.db.dict.find(sds.cast(key.v.ptr)) orelse {
        cli.addReply(reply);
        return null;
    };
    return de.val;
}

pub fn getLongLongOrReply(
    self: *const Object,
    cli: *Client,
    msg: ?[]const u8,
) ?i64 {
    return self.getLongLong() orelse {
        if (msg) |m| {
            cli.addReplyErr(m);
        } else {
            cli.addReplyErr("value is not an integer or out of range");
        }
        return null;
    };
}

pub fn getLongLong(self: *const Object) ?i64 {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return util.string2ll(sds.castBytes(self.v.ptr));
    }
    if (self.encoding == .int) {
        return self.v.int;
    }
    @panic("Unknown string encoding");
}

pub fn getLongDoubleOrReply(
    self: *const Object,
    cli: *Client,
    msg: ?[]const u8,
) ?f80 {
    return self.getLongDouble() orelse {
        if (msg) |err| {
            cli.addReplyErr(err);
        } else {
            cli.addReplyErr("value is not a valid float");
        }
        return null;
    };
}

pub fn getLongDouble(self: *const Object) ?f80 {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return std.fmt.parseFloat(f80, sds.castBytes(self.v.ptr)) catch {
            return null;
        };
    }
    if (self.encoding == .int) {
        return @floatFromInt(self.v.int);
    }
    @panic("Unknown string encoding");
}

pub fn getDoubleOrReply(
    self: *const Object,
    cli: *Client,
    msg: ?[]const u8,
) ?f64 {
    return self.getDouble() orelse {
        if (msg) |err| {
            cli.addReplyErr(err);
        } else {
            cli.addReplyErr("value is not a valid float");
        }
        return null;
    };
}

pub fn getDouble(self: *const Object) ?f64 {
    assert(self.type == .string);
    if (self.sdsEncoded()) {
        return std.fmt.parseFloat(f64, sds.castBytes(self.v.ptr)) catch {
            return null;
        };
    }
    if (self.encoding == .int) {
        return @floatFromInt(self.v.int);
    }
    @panic("Unknown string encoding");
}

pub fn hash(self: *Object) hasher.Hash {
    var o = self;
    if (o.sdsEncoded()) {
        return sds.hash(sds.cast(o.v.ptr));
    }
    if (o.encoding == .int) {
        var buf: [32]u8 = undefined;
        const s = util.ll2string(&buf, o.v.int);
        return hasher.hash(s);
    }
    o = o.getDecoded();
    defer o.decrRefCount();
    return sds.hash(sds.cast(o.v.ptr));
}

pub fn eql(self: *Object, other: *Object) bool {
    var o1 = self;
    var o2 = other;

    if (o1.encoding == .int and o2.encoding == .int) {
        return o1.v.int == o2.v.int;
    }

    o1 = o1.getDecoded();
    defer o1.decrRefCount();
    o2 = o2.getDecoded();
    defer o2.decrRefCount();
    return sds.eql(sds.cast(o1.v.ptr), sds.cast(o2.v.ptr));
}

pub fn checkTypeOrReply(self: *const Object, cli: *Client, typ: Type) bool {
    if (self.type != typ) {
        cli.addReply(Server.shared.wrongtypeerr);
        return true;
    }
    return false;
}

pub fn strEncoding(self: *Object) []const u8 {
    return self.encoding.toString();
}

pub fn free(self: *Object) void {
    if (self.type == .string and self.encoding == .embstr) {
        const s: sds.String = sds.cast(self.v.ptr);
        const mem_size = @sizeOf(Object) + sds.allocSize(s);
        const mem: [*]align(@alignOf(Object)) u8 = @ptrCast(@alignCast(self));
        allocator.free(mem[0..mem_size]);
        return;
    }
    allocator.destroy(self);
}

fn freeString(self: *Object) void {
    if (self.encoding == .raw) {
        const s: sds.String = sds.cast(self.v.ptr);
        sds.free(allocator.impl, s);
    }
}

fn freeList(self: *Object) void {
    if (self.encoding == .quicklist) {
        const ql: *QuickList = .cast(self.v.ptr);
        ql.release();
    } else {
        @panic("Unknown list encoding type");
    }
}

fn freeSet(self: *Object) void {
    switch (self.encoding) {
        .intset => {
            const is: *IntSet = .cast(self.v.ptr);
            is.free();
        },
        .ht => {
            const d: *set.Set.Hash = .cast(self.v.ptr);
            d.destroy();
        },
        else => @panic("Unknown set encoding type"),
    }
}

fn freeZset(self: *Object) void {
    switch (self.encoding) {
        .skiplist => {
            const sl: *SkipListSet = .cast(self.v.ptr);
            sl.destroy();
        },
        .ziplist => {
            const zl: *ZipListSet = .cast(self.v.ptr);
            zl.destroy();
        },
        else => @panic("Unknown sorted set encoding type"),
    }
}

fn freeHash(self: *Object) void {
    switch (self.encoding) {
        .ht => {
            const ht: *hashtype.Hash.Map = .cast(self.v.ptr);
            ht.destroy();
        },
        .ziplist => {
            const zl: *ZipList = .cast(self.v.ptr);
            zl.free();
        },
        else => @panic("Unknown hash encoding type"),
    }
}

fn freeStream(self: *Object) void {
    const s: *Stream = .cast(self.v.ptr);
    s.destroy();
}

const std = @import("std");
const meta = std.meta;
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const assert = std.debug.assert;
const Server = @import("Server.zig");
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqual = std.testing.expectEqual;
const minInt = std.math.minInt;
const maxInt = std.math.maxInt;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const util = @import("util.zig");
const ZipList = @import("ZipList.zig");
const IntSet = @import("IntSet.zig");
const QuickList = @import("QuickList.zig");
const Stream = @import("t_stream.zig").Stream;
const zset = @import("t_zset.zig");
const SkipListSet = zset.SkipListSet;
const ZipListSet = zset.ZipListSet;
const Client = @import("networking.zig").Client;
const evict = @import("evict.zig");
const server = &Server.instance;
const dict = @import("dict.zig");
const set = @import("t_set.zig");
const hashtype = @import("t_hash.zig");
const hasher = @import("hasher.zig");
