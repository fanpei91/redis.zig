/// SELECT index
pub fn selectCommand(cli: *Client) void {
    var id: i64 = undefined;
    const obj = cli.argv.?[1];
    const ok = obj.getLongLongOrReply(
        cli,
        &id,
        "invalid DB index",
    );
    if (!ok) return;

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

    var dbid: i64 = undefined;
    if (!argv[2].getLongLong(&dbid) or !select(cli, dbid)) {
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
    obj.incrRefCount();

    // OK! key moved, free the entry in the source DB
    _ = src.delete(key);
    cli.addReply(Server.shared.cone);
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
    obj.incrRefCount();
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
    pub const Dict = struct {
        pub const HashMap = dict.Dict(sds.String, ?*Object);

        const vtable: *const HashMap.VTable = &.{
            .hash = hash,
            .eql = eql,
            .freeKey = freeKey,
            .freeVal = freeVal,
        };

        fn hash(key: sds.String) dict.Hash {
            return dict.genHash(sds.asBytes(key));
        }

        fn eql(k1: sds.String, k2: sds.String) bool {
            return sds.cmp(k1, k2) == .eq;
        }

        fn freeKey(key: sds.String) void {
            sds.free(key);
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
            .hash = hash,
            .eql = eql,
            .freeKey = freeKey,
            .freeVal = freeVal,
        };

        fn hash(key: *Object) dict.Hash {
            return dict.genHash(sds.asBytes(sds.cast(key.v.ptr)));
        }

        fn eql(k1: *Object, k2: *Object) bool {
            return sds.cmp(sds.cast(k1.v.ptr), sds.cast(k2.v.ptr)) == .eq;
        }

        fn freeKey(key: *Object) void {
            key.decrRefCount();
        }

        fn freeVal(val: *Server.ClientList) void {
            val.release();
        }
    };

    const ReadyKeys = struct {
        const HashMap = dict.Dict(*Object, void);

        const vtable: *const HashMap.VTable = &.{
            .hash = hash,
            .eql = eql,
            .freeKey = freeKey,
        };

        fn hash(key: *Object) dict.Hash {
            var o = key;
            if (o.sdsEncoded()) {
                return dict.genHash(sds.asBytes(sds.cast(o.v.ptr)));
            }
            if (o.encoding == .int) {
                var buf: [32]u8 = undefined;
                const s = util.ll2string(&buf, o.v.int);
                return dict.genHash(s);
            }
            o = o.getDecoded();
            defer o.decrRefCount();
            return dict.genHash(sds.asBytes(sds.cast(o.v.ptr)));
        }

        fn eql(k1: *Object, k2: *Object) bool {
            var o1 = k1;
            var o2 = k2;

            if (o1.encoding == .int and o2.encoding == .int) {
                return o1.v.int == o2.v.int;
            }

            o1 = o1.getDecoded();
            defer o1.decrRefCount();
            o2 = o2.getDecoded();
            defer o2.decrRefCount();

            return sds.cmp(sds.cast(o1.v.ptr), sds.cast(o2.v.ptr)) == .eq;
        }

        fn freeKey(key: *Object) void {
            key.decrRefCount();
        }
    };

    id: usize, // Database ID
    dict: *Dict.HashMap, // The keyspace for this DB
    expires: *Expires.HashMap, // Timeout of keys with a timeout set
    blocking_keys: *BlockingKeys.HashMap, // Keys with clients waiting for data (BLPOP)
    ready_keys: *ReadyKeys.HashMap, // Blocked keys that received a PUSH

    pub fn create(id: usize) Database {
        return .{
            .id = id,
            .dict = .create(Dict.vtable),
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
        val.incrRefCount();
        _ = self.removeExpire(key);
    }

    /// Add the key to the DB. It's up to the caller to increment the reference
    /// counter of the value if needed.
    ///
    /// The program is aborted if the key already exists.
    pub fn add(self: *Database, key: *Object, val: *Object) void {
        const copy = sds.dupe(sds.cast(key.v.ptr));
        const ok = self.dict.add(copy, val);
        std.debug.assert(ok);
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
        const entry = self.dict.find(sds.cast(key.v.ptr));
        var auxentry = entry.?.*;
        std.debug.assert(entry != null);
        const old: *Object = entry.?.val.?;
        if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
            val.lru = old.lru;
        }
        self.dict.setVal(entry.?, val);
        self.dict.freeVal(&auxentry);
    }

    pub fn removeExpire(self: *Database, key: *Object) bool {
        // An expire may only be removed if there is a corresponding entry in
        // the main dict. Otherwise, the key will never be freed.
        const skey = sds.cast(key.v.ptr);
        std.debug.assert(self.dict.find(skey) != null);
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
        const de = self.dict.find(sds.cast(key.v.ptr));
        std.debug.assert(de != null);
        // Reuse the sds from the main dict in the expire dict
        const ee = self.expires.addOrFind(de.?.key);
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
        std.debug.assert(self.dict.find(skey) != null);
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
        std.debug.assert(o.type == .string);
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
        if (self.dict.delete(skey)) {
            return true;
        }
        return false;
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
