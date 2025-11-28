/// SELECT index
pub fn selectCommand(cli: *Client) void {
    const obj = cli.argv.?[1];
    const id = obj.getLongLongOrReply(
        cli,
        "invalid DB index",
    ) orelse return;

    if (!select(cli, id)) {
        cli.addReplyErr("DB index is out of range");
        return;
    }
    cli.addReply(Server.shared.ok);
}

/// EXISTS key [key ...]
pub fn existsCommand(cli: *Client) void {
    const argv = cli.argv.?;
    var count: i64 = 0;
    for (argv[1..]) |key| {
        if (cli.db.lookupKeyRead(key) != null) {
            count += 1;
        }
    }
    cli.addReplyLongLong(count);
}

/// DEL key [key ...]
pub fn delCommand(cli: *Client) void {
    del(cli, false);
}

/// UNLINK key [key ...]
pub fn unlinkCommand(cli: *Client) void {
    del(cli, true);
}

/// RENAME key newkey
pub fn renameCommand(cli: *Client) void {
    rename(cli, false);
}

/// RENAMENX key newkey
pub fn renamenxCommand(cli: *Client) void {
    rename(cli, true);
}

/// DBSIZE
pub fn dbsizeCommand(cli: *Client) void {
    cli.addReplyLongLong(@intCast(cli.db.dict.size()));
}

/// RANDOMKEY
pub fn randomkeyCommand(cli: *Client) void {
    const key = cli.db.randomKey() orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    defer key.decrRefCount();
    cli.addReplyBulk(key);
}

/// MOVE key db
pub fn moveCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const src = cli.db;

    const dbid = argv[2].getLongLongOrReply(cli, null) orelse return;
    if (!select(cli, dbid)) {
        cli.addReply(Server.shared.outofrangeerr);
        return;
    }
    const dst = cli.db;

    // Back to the source DB
    _ = select(cli, @intCast(src.id));

    // If the user is moving using as target the same
    // DB as the source DB it is probably an error.
    if (src == dst) {
        cli.addReply(Server.shared.sameobjecterr);
        return;
    }

    // Check if the element exists and get a reference
    const key = argv[1];
    const obj = src.lookupKeyWrite(key) orelse {
        cli.addReply(Server.shared.czero);
        return;
    };
    const expire = src.getExpire(key);

    // Return zero if the key already exists in the target DB
    if (dst.lookupKeyWrite(key) != null) {
        cli.addReply(Server.shared.czero);
        return;
    }

    dst.add(key, obj);
    if (expire != -1) dst.setExpire(cli, key, expire);

    // OK! key moved, free the entry in the source DB
    _ = src.delete(key);
    cli.addReply(Server.shared.cone);
}

/// SCAN cursor [MATCH pattern] [COUNT count]
pub fn scanCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const cursor = Scan.parseCursorOrReply(argv[1], cli) orelse {
        return;
    };
    Scan.scan(cli, null, cursor, Database.Hash.Map, scanCallback);
}

fn scanCallback(
    privdata: ?*anyopaque,
    entry: *const Database.Hash.Map.Entry,
) void {
    const keys: *Scan.Keys = @ptrCast(@alignCast(privdata.?));
    keys.append(Object.createString(sds.asBytes(entry.key)));
}

/// DEL/UNLINK key [key ...]
fn del(cli: *Client, lazy: bool) void {
    const argv = cli.argv.?;
    var numdel: i64 = 0;
    for (argv[1..]) |key| {
        _ = cli.db.expireIfNeeded(key);
        const deleted = if (lazy)
            lazyfree.asyncDelete(cli.db, key)
        else
            cli.db.syncDelete(key);
        if (deleted) numdel += 1;
    }
    cli.addReplyLongLong(numdel);
}

/// RENAME/RENAMENX key newkey
fn rename(cli: *Client, nx: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const newkey = argv[2];

    const obj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nokeyerr,
    ) orelse return;
    // When source and dest key is the same, no operation is performed,
    // if the key exists, however we still return an error on unexisting key.
    const samekey = sds.cmp(
        sds.cast(key.v.ptr),
        sds.cast(newkey.v.ptr),
    ) == .eq;
    if (samekey) {
        cli.addReply(if (nx) Server.shared.czero else Server.shared.ok);
        return;
    }

    if (cli.db.lookupKeyWrite(newkey) != null) {
        if (nx) {
            cli.addReply(Server.shared.czero);
            return;
        }
        // Overwrite: delete the old key before creating the new one
        // with the same name.
        _ = cli.db.delete(newkey);
    }

    cli.db.add(newkey, obj);
    const expire = cli.db.getExpire(key);
    if (expire != -1) {
        cli.db.setExpire(cli, newkey, expire);
    }
    _ = cli.db.delete(key);

    cli.addReply(if (nx) Server.shared.cone else Server.shared.ok);
}

pub fn select(cli: *Client, id: i64) bool {
    if (id < 0 or id >= server.dbnum) {
        return false;
    }
    cli.db = &server.db[@intCast(id)];
    return true;
}

pub const Database = struct {
    pub const Hash = struct {
        pub const Map = dict.Dict(sds.String, ?*Object);

        const vtable: *const Map.VTable = &.{
            .hash = hash,
            .eql = eql,
            .dupeKey = dupeKey,
            .dupeVal = dupeVal,
            .freeKey = freeKey,
            .freeVal = freeVal,
        };

        fn hash(key: sds.String) dict.Hash {
            return dict.genHash(sds.asBytes(key));
        }

        fn eql(k1: sds.String, k2: sds.String) bool {
            return sds.cmp(k1, k2) == .eq;
        }

        fn dupeKey(key: sds.String) sds.String {
            return sds.dupe(allocator.child, key);
        }

        fn dupeVal(val: ?*Object) ?*Object {
            // Lazy freeing will set value to null.
            if (val) |v| {
                v.incrRefCount();
            }
            return val;
        }

        fn freeKey(key: sds.String) void {
            sds.free(allocator.child, key);
        }

        fn freeVal(val: ?*Object) void {
            // Lazy freeing will set value to null.
            if (val) |v| v.decrRefCount();
        }
    };

    const Expires = struct {
        const HashMap = dict.Dict(sds.String, i64);

        const vtable: *const HashMap.VTable = &.{
            .hash = hash,
            .eql = eql,
        };

        fn hash(key: sds.String) dict.Hash {
            return dict.genHash(sds.asBytes(key));
        }

        fn eql(k1: sds.String, k2: sds.String) bool {
            return sds.cmp(k1, k2) == .eq;
        }
    };

    const BlockingKeys = struct {
        const HashMap = dict.Dict(*Object, *Server.ClientList);

        const vtable: *const HashMap.VTable = &.{
            .hash = Object.hash,
            .eql = Object.eql,
            .dupeKey = dupeKey,
            .freeKey = Object.decrRefCount,
            .freeVal = Server.ClientList.release,
        };

        fn dupeKey(key: *Object) *Object {
            key.incrRefCount();
            return key;
        }
    };

    const ReadyKeys = struct {
        const HashMap = dict.Dict(*Object, void);

        const vtable: *const HashMap.VTable = &.{
            .hash = Object.hash,
            .eql = Object.eql,
            .dupeKey = dupeKey,
            .freeKey = Object.decrRefCount,
        };

        fn dupeKey(key: *Object) *Object {
            key.incrRefCount();
            return key;
        }
    };

    id: usize, // Database ID
    dict: *Hash.Map, // The keyspace for this DB
    expires: *Expires.HashMap, // Timeout of keys with a timeout set
    blocking_keys: *BlockingKeys.HashMap, // Keys with clients waiting for data (BLPOP)
    ready_keys: *ReadyKeys.HashMap, // Blocked keys that received a PUSH

    pub fn create(id: usize) Database {
        return .{
            .id = id,
            .dict = .create(Hash.vtable),
            .expires = .create(Expires.vtable),
            .blocking_keys = .create(BlockingKeys.vtable),
            .ready_keys = .create(ReadyKeys.vtable),
        };
    }

    /// Update LFU when an object is accessed.
    /// Firstly, decrement the counter if the decrement time is reached.
    /// Then logarithmically increment the counter, and update the access time.
    /// High level Set operation. This function can be used in order to set
    /// a key, whatever it was existing or not, to a new object.
    ///
    /// All the new keys in the database should be created via this interface.
    pub fn setKey(self: *Database, key: *Object, val: *Object) void {
        if (self.lookupKeyWrite(key) == null) {
            self.add(key, val);
        } else {
            self.overwrite(key, val);
        }
        _ = self.removeExpire(key);
    }

    /// Add the key to the DB. The program is aborted if the key already exists.
    pub fn add(self: *Database, key: *Object, val: *Object) void {
        const added = self.dict.add(sds.cast(key.v.ptr), val);
        assert(added);
        if (val.type == .list) {
            blocked.signalKeyAsReady(self, key);
        }
    }

    /// This is a wrapper whose behavior depends on the Redis lazy free
    /// configuration. Deletes the key synchronously or asynchronously.
    pub fn delete(self: *Database, key: *Object) bool {
        if (server.lazyfree_lazy_expire) {
            return lazyfree.asyncDelete(self, key);
        }
        return self.syncDelete(key);
    }

    /// Overwrite an existing key with a new value. Incrementing the reference
    /// count of the new value is up to the caller.
    /// This function does not modify the expire time of the existing key.
    ///
    /// The program is aborted if the key was not already present.
    pub fn overwrite(self: *Database, key: *Object, val: *Object) void {
        const entry = self.dict.find(sds.cast(key.v.ptr)).?;
        var auxentry = entry.*;
        const old: *Object = entry.val.?;
        if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
            val.lru = old.lru;
        }
        self.dict.setVal(entry, val);
        self.dict.freeVal(&auxentry);
    }

    pub fn removeExpire(self: *Database, key: *Object) bool {
        // An expire may only be removed if there is a corresponding entry in
        // the main dict. Otherwise, the key will never be freed.
        const skey = sds.cast(key.v.ptr);
        assert(self.dict.find(skey) != null);
        return self.expires.delete(skey);
    }

    /// Set an expire to the specified key. The 'when' parameter is the absolute
    /// unix time in milliseconds after which the key will no longer be
    /// considered valid.
    pub fn setExpire(
        self: *Database,
        cli: *Client,
        key: *Object,
        when: i64,
    ) void {
        _ = cli;
        const de = self.dict.find(sds.cast(key.v.ptr)).?;
        // Reuse the sds from the main dict in the expire dict
        const ee = self.expires.addOrFind(de.key);
        self.expires.setVal(ee, when);
    }

    /// Return the expire time(absolute unix time in ms) of the specified key,
    /// or -1 if no expire is associated with this key.
    pub fn getExpire(self: *Database, key: *Object) i64 {
        if (self.expires.size() == 0) {
            return -1;
        }
        const skey = sds.cast(key.v.ptr);
        const entry = self.expires.find(skey) orelse {
            return -1;
        };
        // The entry was found in the expire dict, this means it should also
        // be present in the main dict (safety check).
        assert(self.dict.find(skey) != null);
        return entry.val;
    }

    /// Prepare the string object stored at 'key' to be modified destructively
    /// to implement commands like SETBIT or APPEND.
    ///
    /// An object is usually ready to be modified unless one of the two conditions
    /// are true:
    ///
    /// 1) The object 'o' is shared (refcount > 1), we don't want to affect
    ///    other users.
    /// 2) The object encoding is not "RAW".
    ///
    /// If the object is found in one of the above conditions (or both) by the
    /// function, an unshared / not-encoded copy of the string object is stored
    /// at 'key' in the specified 'db'. Otherwise the object 'o' itself is
    /// returned.
    ///
    /// USAGE:
    ///
    /// The object 'o' is what the caller already obtained by looking up 'key'
    /// in 'db', the usage pattern looks like this:
    ///
    /// o = db.lookupKeyWrite(key);
    /// if (o.checkTypeOrReply(cli, .string)) return;
    /// o = db.unshareStringValue(key,o);
    ///
    /// At this point the caller is ready to modify the object, for example
    /// using an sds.cat() call to append some data, or anything else.
    pub fn unshareStringValue(
        self: *Database,
        key: *Object,
        o: *Object,
    ) *Object {
        assert(o.type == .string);
        if (o.refcount != 1 or o.encoding != .raw) {
            const decoded = o.getDecoded();
            const new = Object.createRawString(
                sds.asBytes(sds.cast(decoded.v.ptr)),
            );
            decoded.decrRefCount();
            self.overwrite(key, new);
            return new;
        }
        return o;
    }

    /// Lookup a key for write operations, and as a side effect, if needed,
    /// expires the key if its TTL is reached.
    ///
    /// Returns the linked value object if the key exists or null if the key
    /// does not exist in the specified DB.
    pub fn lookupKeyWrite(self: *Database, key: *Object) ?*Object {
        _ = self.expireIfNeeded(key);
        return self.lookupKey(key, Server.LOOKUP_NONE);
    }

    pub fn lookupKeyWriteOrReply(
        self: *Database,
        cli: *Client,
        key: *Object,
        reply: *Object,
    ) ?*Object {
        const obj = self.lookupKeyWrite(key) orelse {
            cli.addReply(reply);
            return null;
        };
        return obj;
    }

    pub fn lookupKeyReadOrReply(
        self: *Database,
        cli: *Client,
        key: *Object,
        reply: *Object,
    ) ?*Object {
        const obj = self.lookupKeyRead(key) orelse {
            cli.addReply(reply);
            return null;
        };
        return obj;
    }

    /// Like lookupKeyReadWithFlags(), but does not use any flag, which is the
    /// common case.
    pub fn lookupKeyRead(self: *Database, key: *Object) ?*Object {
        return self.lookupKeyReadWithFlags(
            key,
            Server.LOOKUP_NONE,
        );
    }

    /// Lookup a key for read operations, or return null if the key is not found
    /// in the specified DB.
    ///
    /// As a side effect of calling this function:
    /// 1. A key gets expired if it reached it's TTL.
    /// 2. The key last access time is updated.
    ///
    /// This API should not be used when we write to the key after obtaining
    /// the object linked to the key, but only for read only operations.
    ///
    /// Flags change the behavior of this command:
    ///
    ///  LOOKUP_NONE (or zero): no special flags are passed.
    ///  LOOKUP_NOTOUCH: don't alter the last access time of the key.
    pub fn lookupKeyReadWithFlags(
        self: *Database,
        key: *Object,
        flags: u32,
    ) ?*Object {
        if (self.expireIfNeeded(key)) {
            return null;
        }
        return self.lookupKey(key, flags);
    }

    /// Low level key lookup API, not actually called directly from commands
    /// implementations that should instead rely on lookupKeyRead(),
    /// lookupKeyWrite() and lookupKeyReadWithFlags().
    fn lookupKey(self: *Database, key: *Object, flags: u32) ?*Object {
        const entry = self.dict.find(sds.cast(key.v.ptr)) orelse {
            return null;
        };
        const val: *Object = entry.val.?;
        if (flags & Server.LOOKUP_NOTOUCH == 0) {
            if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
                updateLFU(val);
            } else {
                val.lru = @intCast(evict.LRUClock());
            }
        }
        return val;
    }

    fn updateLFU(obj: *Object) void {
        var counter: u8 = evict.LFUDecrAndReturn(obj);
        counter = evict.LFULogIncr(counter);
        obj.lru = @intCast((evict.LFUGetTimeInMinutes() << 8) | counter);
    }

    /// This function is called when we are going to perform some operation
    /// in a given key, but such key may be already logically expired even if
    /// it still exists in the database. The main way this function is called
    /// is via lookupKey*() family of functions.
    ///
    /// The return value of the function is false if the key is still valid,
    /// otherwise the function returns true if the key is expired.
    fn expireIfNeeded(self: *Database, key: *Object) bool {
        if (!self.keyIsExpired(key)) return false;
        if (server.lazyfree_lazy_expire) {
            return lazyfree.asyncDelete(self, key);
        }
        return self.syncDelete(key);
    }

    /// Delete a key, value, and associated expiration entry if any,
    /// from the DB.
    fn syncDelete(self: *Database, key: *Object) bool {
        const skey = sds.cast(key.v.ptr);
        if (self.expires.size() > 0) {
            _ = self.expires.delete(skey);
        }
        return self.dict.delete(skey);
    }

    /// Check if the key is expired.
    fn keyIsExpired(self: *Database, key: *Object) bool {
        const when = self.getExpire(key);

        // No expire for this key
        if (when < 0) return false;

        var now: i64 = undefined;
        // If we are in the middle of a command execution, we still want to use
        // a reference time that does not change: in that case we just use the
        // cached time, that we update before each call in the call() function.
        // This way we avoid that commands such as RPOPLPUSH or similar, that
        // may re-open the same key multiple times, can invalidate an already
        // open object in a next call, if the next call will see the key expired,
        // while the first did not.
        if (server.fixed_time_expire > 0) {
            now = server.mstime;
        }
        // For the other cases, we want to use the most fresh time we have.
        else {
            now = std.time.milliTimestamp();
        }

        // The key expired if the current (virtual or real) time is greater
        // than the expire time of the key.
        return now > when;
    }

    /// Return a random key, in form of a Object.
    /// If there are no keys, null is returned.
    ///
    /// The function makes sure to return keys not already expired.
    fn randomKey(self: *Database) ?*Object {
        while (true) {
            const entry = self.dict.getRandom() orelse {
                return null;
            };
            const key: sds.String = entry.key;
            const keyobj = Object.createString(sds.asBytes(key));
            if (self.expires.find(key) != null) {
                if (self.expireIfNeeded(keyobj)) {
                    keyobj.decrRefCount();
                    continue;
                }
            }
            return keyobj;
        }
    }

    pub fn destroy(self: *Database) void {
        self.dict.destroy();
        self.expires.destroy();
        self.blocking_keys.destroy();
        self.ready_keys.destroy();
        self.* = undefined;
    }
};

pub const Scan = struct {
    pub const Keys = list.List(void, *Object);

    const vtable: *const Keys.VTable = &.{
        .freeVal = Object.decrRefCount,
    };

    /// This function implements SCAN, HSCAN and SSCAN commands.
    /// If object 'o' is passed, then it must be a Hash or Set object, otherwise
    /// if 'o' is NULL the command will operate on the dictionary associated with
    /// the current database.
    ///
    /// When 'o' is not NULL the function assumes that the first argument in
    /// the client arguments vector is a key so it skips it before iterating
    /// in order to parse options.
    ///
    /// In the case of a Hash object the function returns both the field and value
    /// of every element on the Hash.
    pub fn scan(
        cli: *Client,
        o: ?*Object,
        cursor: u64,
        comptime HashMap: type,
        callback: *const fn (
            privtata: ?*anyopaque, // Keys
            entry: *const HashMap.Entry,
        ) void,
    ) void {
        // zig fmt: off
        assert(
            o == null or
            o.?.type == .hash or
            o.?.type == .set or
            o.?.type == .zset,
        );
        // zig fmt: on

        const argv = cli.argv.?;
        var keys: *Keys = Keys.create(vtable);
        defer keys.release();
        var count: i64 = 10;
        var pat: ?sds.String = null;
        var use_pattern = false;

        // Set i to the first option argument. The previous one is the cursor.
        // Skip the key argument if needed.
        var i: usize = if (o == null) 2 else 3;

        // Step 1: Parse options.
        while (i < cli.argc) {
            const j = cli.argc - i;
            const option = sds.asBytes(sds.cast(argv[i].v.ptr));
            if (caseEql(option, "count") and j >= 2) {
                const value = argv[i + 1];
                count = value.getLongLongOrReply(cli, null) orelse return;
                if (count < 1) {
                    cli.addReply(Server.shared.syntaxerr);
                    return;
                }
                i += 2;
            } else if (caseEql(option, "match") and j >= 2) {
                const value = argv[i + 1];
                pat = sds.cast(value.v.ptr);
                const patlen = sds.getLen(pat.?);
                use_pattern = !(patlen == 1 and pat.?[0] == '*');
                i += 2;
            } else {
                cli.addReply(Server.shared.syntaxerr);
                return;
            }
        }

        // Step 2: Iterate the collection.
        //
        // Note that if the object is encoded with a ziplist, intset, or any other
        // representation that is not a hash table, we are sure that it is also
        // composed of a small number of elements. So to avoid taking state we
        // just return everything inside the object in a single call, setting the
        // cursor to zero to signal the end of the iteration.

        // Handle the case of a hash table.
        var map: ?*HashMap = null;
        if (o == null) {
            map = @ptrCast(@alignCast(cli.db.dict));
        } else if (o.?.type == .hash and o.?.encoding == .ht) {
            map = @ptrCast(@alignCast(o.?.v.ptr));
            count *= 2; // We return key / value for this type.
        } else if (o.?.type == .set and o.?.encoding == .ht) {
            map = @ptrCast(@alignCast(o.?.v.ptr));
        }
        // TODO: ZSET

        var cur = cursor;
        if (map) |ht| {
            // We set the max number of iterations to ten times the specified
            // COUNT, so if the hash table is in a pathological state (very
            // sparsely populated) we avoid to block too much time at the cost
            // of returning no or very few elements.
            var maxiterations = count * 10;

            while (true) {
                cur = ht.scan(cur, callback, null, keys);
                if (cur > 0 and maxiterations > 0 and keys.len < count) {
                    maxiterations -= 1;
                    continue;
                }
                break;
            }
        } else if (o.?.type == .set) {
            var pos: u32 = 0;
            const is: *IntSet = @ptrCast(@alignCast(o.?.v.ptr));
            while (is.get(pos)) |value| {
                keys.append(Object.createStringFromLonglong(value));
                pos += 1;
            }
            cur = 0;
        } else if (o.?.type == .hash or o.?.type == .zset) {
            const zl = ZipList.cast(o.?.v.ptr);
            var p = zl.index(ZipList.HEAD);
            while (p) |entry| {
                const value = ZipList.get(entry).?;
                const obj = switch (value) {
                    .num => |v| Object.createStringFromLonglong(v),
                    .str => |v| Object.createString(v),
                };
                keys.append(obj);
                p = zl.next(entry);
            }
            cur = 0;
        } else {
            @panic("Not handled encoding in SCAN.");
        }

        // Step 3: Filter elements.
        var node: ?*Keys.Node = keys.first;
        var nextnode: ?*Keys.Node = null;
        while (node) |n| {
            const kboj = n.value;
            nextnode = n.next;
            var filter: bool = false;

            // Filter element if it does not match the pattern.
            if (!filter and use_pattern) {
                const pattern = sds.asSentinelBytes(pat.?);
                if (kboj.sdsEncoded()) {
                    filter = !util.stringmatch(
                        pattern,
                        sds.asSentinelBytes(sds.cast(kboj.v.ptr)),
                        false,
                    );
                } else {
                    var buf: [util.MAX_LONG_DOUBLE_CHARS]u8 = undefined;
                    assert(kboj.encoding == .int);
                    const string = util.ll2string(&buf, kboj.v.int);
                    buf[string.len] = 0;
                    filter = !util.stringmatch(
                        pattern,
                        buf[0..string.len :0],
                        false,
                    );
                }
            }

            // Filter element if it is an expired key.
            if (!filter and o == null and cli.db.expireIfNeeded(kboj)) {
                filter = true;
            }

            // Remove the element and its associted value if needed.
            if (filter) {
                keys.removeNode(n);
            }

            // If this is a hash or a sorted set, we have a flat list of
            // key-value elements, so if this element was filtered, remove the
            // value, or skip it if it was not filtered: we only match keys.
            if (o != null and (o.?.type == .zset or o.?.type == .hash)) {
                node = nextnode;
                nextnode = node.?.next;
                if (filter) {
                    keys.removeNode(node.?);
                }
            }
            node = nextnode;
        }

        // Step 4: Reply to the client.
        cli.addReplyMultiBulkLen(2);
        cli.addReplyLongLong(@intCast(cur));

        cli.addReplyMultiBulkLen(@intCast(keys.len));
        while (keys.first) |n| {
            cli.addReplyBulk(n.value);
            keys.removeNode(n);
        }
    }

    /// Try to parse a SCAN cursor stored at object 'o'. If the cursor is valid,
    /// return it, otherwise return null and send an error to the client.
    pub fn parseCursorOrReply(o: *const Object, cli: *Client) ?u64 {
        const cursor = std.fmt.parseInt(
            u64,
            sds.asBytes(sds.cast(o.v.ptr)),
            10,
        ) catch {
            cli.addReplyErr("invalid cursor");
            return null;
        };
        return cursor;
    }
};

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const allocator = @import("allocator.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const evict = @import("evict.zig");
const sds = @import("sds.zig");
const log = std.log.scoped(.db);
const lazyfree = @import("lazyfree.zig");
const blocked = @import("blocked.zig");
const dict = @import("dict.zig");
const util = @import("util.zig");
const assert = std.debug.assert;
const list = @import("list.zig");
const caseEql = std.ascii.eqlIgnoreCase;
const ZipList = @import("ZipList.zig");
const IntSet = @import("IntSet.zig");
