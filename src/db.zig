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
    id: usize, // Database ID
    dict: *Dict, // The keyspace for this DB
    expires: *Dict, // Timeout of keys with a timeout set

    pub fn create(id: usize) Database {
        return .{
            .id = id,
            .dict = Dict.create(
                Server.dbDictVTable,
                null,
            ),
            .expires = Dict.create(
                Server.expireDictVTable,
                null,
            ),
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
        const entry = self.dict.find(key.v.ptr);
        var auxentry = entry.?.*;
        std.debug.assert(entry != null);
        const old: *Object = @ptrCast(@alignCast(entry.?.v.val.?));
        if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
            val.lru = old.lru;
        }
        self.dict.setVal(entry.?, val);
        self.dict.freeVal(&auxentry);
    }

    pub fn removeExpire(self: *Database, key: *Object) bool {
        // An expire may only be removed if there is a corresponding entry in
        // the main dict. Otherwise, the key will never be freed.
        std.debug.assert(self.dict.find(key.v.ptr) != null);
        return self.expires.delete(key.v.ptr);
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
        const de = self.dict.find(key.v.ptr);
        std.debug.assert(de != null);
        // Reuse the sds from the main dict in the expire dict
        const ee = self.expires.addOrFind(
            de.?.key,
        );
        ee.v = .{ .s64 = when };
    }

    /// Return the expire time(absolute unix time in ms) of the specified key,
    /// or -1 if no expire is associated with this key.
    pub fn getExpire(self: *Database, key: *Object) i64 {
        if (self.expires.size() == 0) {
            return -1;
        }
        const entry = self.expires.find(
            key.v.ptr,
        ) orelse {
            return -1;
        };
        // The entry was found in the expire dict, this means it should also
        // be present in the main dict (safety check).
        std.debug.assert(self.dict.find(key.v.ptr) != null);
        return entry.v.s64;
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
        const entry = self.dict.find(key.v.ptr) orelse {
            return null;
        };
        const val: *Object = @ptrCast(@alignCast(entry.v.val.?));
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
        if (self.expires.size() > 0) {
            _ = self.expires.delete(key.v.ptr);
        }
        if (self.dict.delete(key.v.ptr)) {
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
            const key: sds.String = sds.cast(entry.key);
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
        self.* = undefined;
    }
};

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const allocator = @import("allocator.zig");
const server = &Server.instance;
const Dict = @import("Dict.zig");
const Object = @import("Object.zig");
const evict = @import("evict.zig");
const sds = @import("sds.zig");
const log = std.log.scoped(.db);
const lazyfree = @import("lazyfree.zig");
