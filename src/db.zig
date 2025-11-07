pub const Database = struct {
    allocator: Allocator,
    id: usize, // Database ID
    dict: *Dict, // The keyspace for this DB
    expires: *Dict, // Timeout of keys with a timeout set

    pub fn create(allocator: Allocator, id: usize) Allocator.Error!Database {
        var db: Database = undefined;
        db.allocator = allocator;
        db.id = id;
        db.dict = try Dict.create(
            allocator,
            Server.dbDictVTable,
            null,
        );
        errdefer db.dict.destroy(allocator);
        db.expires = try Dict.create(
            allocator,
            Server.expireDictVTable,
            null,
        );
        errdefer db.expires.destroy(allocator);
        return db;
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

    fn lookupKey(
        self: *Database,
        key: *Object,
        flags: u32,
    ) ?*Object {
        const entry = self.dict.find(self.allocator, key.data.ptr) orelse {
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

    /// Update LFU when an object is accessed.
    /// Firstly, decrement the counter if the decrement time is reached.
    /// Then logarithmically increment the counter, and update the access time.
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
        return self.syncDelete(key);
    }

    /// Delete a key, value, and associated expiration entry if any,
    /// from the DB.
    fn syncDelete(self: *Database, key: *Object) bool {
        if (self.expires.size() > 0) {
            _ = self.expires.delete(self.allocator, key.data.ptr);
        }
        if (self.dict.delete(self.allocator, key.data.ptr)) {
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

    /// Return the expire time of the specified key, or -1 if no expire
    /// is associated with this key (i.e. the key is non volatile)
    fn getExpire(self: *Database, key: *Object) i64 {
        if (self.expires.size() == 0) {
            return -1;
        }
        const entry = self.expires.find(
            self.allocator,
            key.data.ptr,
        ) orelse {
            return -1;
        };
        // The entry was found in the expire dict, this means it should also
        // be present in the main dict (safety check).
        std.debug.assert(self.dict.find(
            self.allocator,
            key.data.ptr,
        ) != null);
        return entry.v.s64;
    }

    /// High level Set operation. This function can be used in order to set
    /// a key, whatever it was existing or not, to a new object.
    ///
    /// All the new keys in the database should be created via this interface.
    pub fn setKey(
        self: *Database,
        key: *Object,
        val: *Object,
    ) Allocator.Error!void {
        if (self.lookupKeyWrite(key) == null) {
            try self.add(key, val);
        } else {
            try self.overwrite(key, val);
        }
        val.incrRefCount();
        _ = self.removeExpire(key);
    }

    /// Add the key to the DB. It's up to the caller to increment the reference
    /// counter of the value if needed.
    ///
    /// The program is aborted if the key already exists.
    pub fn add(
        self: *Database,
        key: *Object,
        val: *Object,
    ) Allocator.Error!void {
        const copy = try sds.dupe(self.allocator, sds.cast(key.data.ptr));
        errdefer sds.free(self.allocator, copy);
        const ok = try self.dict.add(self.allocator, copy, val);
        std.debug.assert(ok);
    }

    /// Overwrite an existing key with a new value. Incrementing the reference
    /// count of the new value is up to the caller.
    /// This function does not modify the expire time of the existing key.
    ///
    /// The program is aborted if the key was not already present.
    pub fn overwrite(
        self: *Database,
        key: *Object,
        val: *Object,
    ) Allocator.Error!void {
        const entry = self.dict.find(self.allocator, key.data.ptr);
        var auxentry = entry.?.*;
        std.debug.assert(entry != null);
        const old: *Object = @ptrCast(@alignCast(entry.?.v.val.?));
        if (server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0) {
            val.lru = old.lru;
        }
        try self.dict.setVal(self.allocator, entry.?, val);
        self.dict.freeVal(self.allocator, &auxentry);
    }

    pub fn removeExpire(
        self: *Database,
        key: *Object,
    ) bool {
        // An expire may only be removed if there is a corresponding entry in
        // the main dict. Otherwise, the key will never be freed.
        std.debug.assert(self.dict.find(self.allocator, key.data.ptr) != null);
        return self.expires.delete(self.allocator, key.data.ptr);
    }

    /// Set an expire to the specified key. The 'when' parameter is the absolute
    /// unix time in milliseconds after which the key will no longer be
    /// considered valid.
    pub fn setExpire(
        self: *Database,
        cli: *Client,
        key: *Object,
        when: i64,
    ) Allocator.Error!void {
        _ = cli;
        const de = self.dict.find(self.allocator, key.data.ptr);
        std.debug.assert(de != null);
        // Reuse the sds from the main dict in the expire dict
        const ee = try self.expires.addOrFind(
            self.allocator,
            de.?.key,
        );
        ee.v.s64 = when;
    }

    pub fn destroy(self: *Database) void {
        self.dict.destroy(self.allocator);
        self.expires.destroy(self.allocator);
        self.* = undefined;
    }
};

pub fn select(cli: *Client, id: i64) bool {
    if (id < 0 or id >= server.dbnum) {
        return false;
    }
    cli.db = &server.db[@intCast(id)];
    return true;
}

pub fn selectCommand(cli: *Client) Allocator.Error!void {
    var id: i64 = undefined;
    const obj = cli.argv.?[1];
    const ok = try obj.getLongLongOrReply(
        cli,
        &id,
        "invalid DB index",
    );
    if (!ok) return;

    if (!select(cli, id)) {
        try cli.addReplyErr("DB index is out of range");
        return;
    }
    try cli.addReply(Server.shared.ok);
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Allocator = std.mem.Allocator;
const server = &Server.instance;
const Dict = @import("Dict.zig");
const Object = @import("Object.zig");
const evict = @import("evict.zig");
const sds = @import("sds.zig");
