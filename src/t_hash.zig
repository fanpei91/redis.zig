/// HSET/HMSET key field value [field value ...]
pub fn hsetCommand(cli: *Client) void {
    const argv = cli.argv orelse unreachable;

    if (cli.argc % 2 == 1) {
        cli.addReplyErr("wrong number of arguments for HMSET");
        return;
    }

    const hobj = Hash.lookupCreateOrReply(argv[1], cli) orelse {
        return;
    };
    Hash.tryConversion(hobj, argv[2..]);

    var created: i64 = 0;
    var i: usize = 2;
    while (i < cli.argc) : (i += 2) {
        const key = sds.cast(argv[i].v.ptr);
        const val = sds.cast(argv[i + 1].v.ptr);
        const ret = Hash.set(hobj, key, val, Hash.SET_COPY);
        if (ret == .insert) {
            created += 1;
        }
    }

    // HMSET (deprecated) and HSET return value is different.
    const command = sds.asBytes(sds.cast(argv[0].v.ptr));
    if (command[1] == 's' or command[1] == 'S') {
        // HSET
        cli.addReplyLongLong(created);
    } else {
        // HMSET
        cli.addReply(Server.shared.ok);
    }
}

/// HGET key field
pub fn hgetCommand(cli: *Client) void {
    const argv = cli.argv orelse unreachable;
    const key = argv[1];
    const hobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (hobj.checkTypeOrReply(cli, .hash)) {
        return;
    }

    const field = argv[2];
    Hash.fieldToReply(hobj, sds.cast(field.v.ptr), cli);
}

/// HDEL key field [field ...]
pub fn hdelCommand(cli: *Client) void {
    const argv = cli.argv orelse unreachable;
    const key = argv[1];
    const hobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (hobj.checkTypeOrReply(cli, .hash)) {
        return;
    }

    var deleted: i64 = 0;
    for (argv[2..]) |field| {
        if (Hash.delete(hobj, sds.cast(field.v.ptr))) {
            deleted += 1;
            if (Hash.length(hobj) == 0) {
                const ok = cli.db.delete(key);
                assert(ok);
                break;
            }
        }
    }
    cli.addReplyLongLong(deleted);
}

pub const Hash = struct {
    fn lookupCreateOrReply(key: *Object, cli: *Client) ?*Object {
        const o = cli.db.lookupKeyWrite(key) orelse {
            const o = Object.createHash();
            defer o.decrRefCount();
            cli.db.add(key, o);
            return o;
        };
        if (o.checkTypeOrReply(cli, .hash)) {
            return null;
        }
        return o;
    }

    /// Check the length of a number of objects to see if we need to convert a
    /// ziplist to a real hash. Note that we only check string encoded objects
    /// as their string length can be queried in constant time.
    fn tryConversion(hobj: *Object, argv: []*Object) void {
        if (hobj.encoding != .ziplist) {
            return;
        }
        var sum: usize = 0;
        for (argv) |arg| {
            if (!Object.sdsEncoded(arg)) {
                continue;
            }
            const len = sds.getLen(sds.cast(arg.v.ptr));
            if (len > server.hash_max_ziplist_value) {
                convert(hobj, .ht);
                return;
            }
            sum += len;
        }
        const zl: *ZipList = ZipList.cast(hobj.v.ptr);
        if (!ZipList.safeToAdd(zl, sum)) {
            convert(hobj, .ht);
        }
    }

    fn convert(hobj: *Object, enc: Object.Encoding) void {
        if (hobj.encoding == .ziplist) {
            convertZipList(hobj, enc);
            return;
        }
        if (hobj.encoding == .ht) {
            @panic("Not implemented");
        }
        @panic("Unknown hash encoding");
    }

    fn convertZipList(hobj: *Object, enc: Object.Encoding) void {
        assert(hobj.encoding == .ziplist);

        if (enc == .ziplist) {
            return;
        }

        if (enc == .ht) {
            const map = Map.create(vtable);
            var it = iterator(hobj);
            while (it.next()) {
                const key = it.currentKey();
                const val = it.currentVal();
                const ok = map.add(key, val);
                if (!ok) @panic("ZipList corruption detected");
            }
            it.release();

            const zl: *ZipList = ZipList.cast(hobj.v.ptr);
            zl.free();

            hobj.v = .{ .ptr = map };
            hobj.encoding = .ht;
            return;
        }

        @panic("Unknown hash encoding");
    }

    pub const Iterator = struct {
        subject: *Object,
        encoding: Object.Encoding,

        di: ?Map.Iterator = null,
        de: ?*Map.Entry = null,

        // ZipList key/value pair
        field: ?[*]u8 = null,
        value: ?[*]u8 = null,

        pub fn currentKey(self: *const Iterator) sds.String {
            if (self.encoding == .ziplist) {
                return switch (self.currentKeyFromZipList()) {
                    .num => |v| sds.fromLonglong(v),
                    .str => |v| sds.new(v),
                };
            }
            if (self.encoding == .ht) {
                return self.currentKeyFromHashMap();
            }
            @panic("Unknown hash encoding");
        }

        pub fn currentKeyFromZipList(self: *const Iterator) ZipList.Value {
            assert(self.encoding == .ziplist);
            assert(self.field != null);
            return ZipList.get(self.field.?) orelse unreachable;
        }

        pub fn currentKeyFromHashMap(self: *const Iterator) sds.String {
            assert(self.encoding == .ht);
            assert(self.de != null);
            return self.de.?.key;
        }

        pub fn currentVal(self: *const Iterator) sds.String {
            if (self.encoding == .ziplist) {
                return switch (self.currentValFromZipList()) {
                    .num => |v| sds.fromLonglong(v),
                    .str => |v| sds.new(v),
                };
            }
            if (self.encoding == .ht) {
                return self.currentValFromHashMap();
            }
            @panic("Unknown hash encoding");
        }

        pub fn currentValFromZipList(self: *const Iterator) ZipList.Value {
            assert(self.encoding == .ziplist);
            assert(self.value != null);
            return ZipList.get(self.value.?) orelse unreachable;
        }

        pub fn currentValFromHashMap(self: *const Iterator) sds.String {
            assert(self.encoding == .ht);
            assert(self.de != null);
            return self.de.?.val;
        }

        pub fn next(self: *Iterator) bool {
            if (self.encoding == .ziplist) {
                const zl: *ZipList = ZipList.cast(self.subject.v.ptr);
                var field = self.field;
                var value = self.value;

                if (field == null) {
                    assert(value == null);
                    field = zl.index(ZipList.HEAD);
                } else {
                    assert(value != null);
                    field = zl.next(value.?);
                }
                if (field == null) {
                    return false;
                }
                value = zl.next(field.?);
                assert(value != null);

                self.field = field;
                self.value = value;
                return true;
            }
            if (self.encoding == .ht) {
                assert(self.di != null);
                self.de = self.di.?.next() orelse {
                    return false;
                };
                return true;
            }
            @panic("Unknown hash encoding");
        }

        pub fn release(self: *Iterator) void {
            if (self.encoding == .ht) {
                self.di.?.release();
            }
            self.* = undefined;
        }
    };

    pub fn iterator(hoj: *Object) Iterator {
        var it: Iterator = .{
            .subject = hoj,
            .encoding = hoj.encoding,
        };
        if (it.encoding == .ht) {
            const m: *Map = @ptrCast(@alignCast(hoj.v.ptr));
            it.di = m.iterator(false);
        }
        return it;
    }

    /// Delete an element from a hash.
    /// Return TRUE on deleted and FALSE on not found.
    pub fn delete(hobj: *Object, field: sds.String) bool {
        if (hobj.encoding == .ziplist) {
            var zl = ZipList.cast(hobj.v.ptr);
            if (zl.index(ZipList.HEAD)) |head| {
                if (ZipList.find(head, sds.asBytes(field), 1)) |e| {
                    var entry = e;
                    zl = zl.delete(&entry); // Delete the key
                    zl = zl.delete(&entry); // Delete the value
                    hobj.v = .{ .ptr = zl };
                    return true;
                }
            }
            return false;
        }
        if (hobj.encoding == .ht) {
            const m: *Map = @ptrCast(@alignCast(hobj.v.ptr));
            // Always check if the dictionary needs a shrink after a delete.
            defer if (Server.needShrinkDictToFit(m.size(), m.slots())) {
                _ = m.shrinkToFit();
            };
            return m.delete(field);
        }
        @panic("Unknown hash encoding");
    }

    /// Return the number of elements in a hash.
    pub fn length(hobj: *Object) u64 {
        if (hobj.encoding == .ziplist) {
            const zl = ZipList.cast(hobj.v.ptr);
            return @divExact(zl.len.get(), 2);
        }
        if (hobj.encoding == .ht) {
            const m: *Map = @ptrCast(@alignCast(hobj.v.ptr));
            return m.size();
        }
        @panic("Unknown hash encoding");
    }

    pub const SET_TAKE_FIELD = (1 << 0);
    pub const SET_TAKE_VALUE = (1 << 1);
    pub const SET_COPY = 0;
    /// Add a new field, overwrite the old with the new value if it already exists.
    /// Return .insert on insert and .update on update.
    ///
    /// By default, the key and value SDS strings are copied if needed, so the
    /// caller retains ownership of the strings passed. However this behavior
    /// can be effected by passing appropriate flags (possibly bitwise OR-ed):
    ///
    /// SET_TAKE_FIELD -- The SDS field ownership passes to the function.
    /// SET_TAKE_VALUE -- The SDS value ownership passes to the function.
    ///
    /// When the flags are used the caller does not need to release the passed
    /// SDS string(s). It's up to the function to use the string to create a new
    /// entry or to free the SDS string before returning to the caller.
    ///
    /// SET_COPY corresponds to no flags passed, and means the default
    /// semantics of copying the values if needed.
    pub fn set(hobj: *Object, key: sds.String, val: sds.String, flags: i32) enum {
        insert,
        update,
    } {
        var field: ?sds.String = key;
        var value: ?sds.String = val;
        // Free SDS strings we did not referenced elsewhere if the flags
        // want this function to be responsible.
        defer if (field) |f| if (flags & SET_TAKE_FIELD != 0) {
            sds.free(f);
        };
        defer if (value) |v| if (flags & SET_TAKE_VALUE != 0) {
            sds.free(v);
        };

        if (hobj.encoding == .ziplist) {
            // Check if the ziplist needs to be converted to a hash table
            defer if (length(hobj) > server.hash_max_ziplist_entries) {
                convert(hobj, .ht);
            };

            var zl: *ZipList = ZipList.cast(hobj.v.ptr);
            if (zl.index(ZipList.HEAD)) |head| {
                if (ZipList.find(head, sds.asBytes(key), 1)) |f| {
                    var v = zl.next(f) orelse unreachable;

                    zl = zl.delete(&v);
                    zl = zl.insert(v, sds.asBytes(val));

                    hobj.v = .{ .ptr = zl };
                    return .update;
                }
            }

            zl = zl.push(sds.asBytes(key), .tail);
            zl = zl.push(sds.asBytes(val), .tail);
            hobj.v = .{ .ptr = zl };
            return .insert;
        }

        if (hobj.encoding == .ht) {
            const map: *Map = @ptrCast(@alignCast(hobj.v.ptr));
            if (map.find(key)) |de| {
                sds.free(de.val);
                if (flags & SET_TAKE_VALUE != 0) {
                    de.val = val;
                    value = null;
                } else {
                    de.val = sds.dupe(val);
                }
                return .update;
            }

            var f: sds.String = undefined;
            var v: sds.String = undefined;
            if (flags & SET_TAKE_FIELD != 0) {
                f = key;
                field = null;
            } else {
                f = sds.dupe(key);
            }
            if (flags & SET_TAKE_VALUE != 0) {
                v = val;
                value = null;
            } else {
                v = sds.dupe(key);
            }
            const ok = map.add(f, v);
            assert(ok);
            return .insert;
        }

        @panic("Unknown hash encoding");
    }

    fn fieldToReply(hobj: *Object, field: sds.String, cli: *Client) void {
        if (hobj.encoding == .ziplist) {
            const value = Hash.getFromZipList(hobj, field) orelse {
                cli.addReply(Server.shared.nullbulk);
                return;
            };
            switch (value) {
                .str => |v| cli.addReplyBulkString(v),
                .num => |v| cli.addReplyLongLong(v),
            }
            return;
        }
        if (hobj.encoding == .ht) {
            const value = getFromHashMap(hobj, field) orelse {
                cli.addReply(Server.shared.nullbulk);
                return;
            };
            cli.addReplyBulkString(sds.asBytes(value));
            return;
        }
        @panic("Unknown hash encoding");
    }

    /// Get the value from a ziplist encoded hash, identified by field.
    fn getFromZipList(hobj: *Object, field: sds.String) ?ZipList.Value {
        assert(hobj.encoding == .ziplist);

        const zl = ZipList.cast(hobj.v.ptr);
        if (zl.index(ZipList.HEAD)) |head| {
            if (ZipList.find(head, sds.asBytes(field), 1)) |f| {
                const value = zl.next(f) orelse unreachable;
                return ZipList.get(value);
            }
        }
        return null;
    }

    /// Get the value from a hash table encoded hash, identified by field.
    /// Returns null when the field cannot be found, otherwise the SDS value
    /// is returned.
    fn getFromHashMap(hobj: *Object, field: sds.String) ?sds.String {
        assert(hobj.encoding == .ht);

        const map: *Map = @ptrCast(@alignCast(hobj.v.ptr));
        const de = map.find(field) orelse {
            return null;
        };
        return de.val;
    }

    pub const Map = dict.Dict(sds.String, sds.String);

    const vtable: *const Map.VTable = &.{
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

    fn freeVal(val: sds.String) void {
        sds.free(val);
    }
};

const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Object = @import("Object.zig");
const std = @import("std");
const assert = std.debug.assert;
const sds = @import("sds.zig");
const server = &Server.instance;
const ZipList = @import("ZipList.zig");
const dict = @import("dict.zig");
