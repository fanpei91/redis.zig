/// SADD key member [member ...]
pub fn saddCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const sobj = cli.db.lookupKeyWrite(key) orelse blk: {
        const o = Set.create(sds.cast(argv[2].v.ptr));
        defer o.decrRefCount();
        cli.db.add(key, o);
        break :blk o;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    var added: i64 = 0;
    for (argv[2..]) |member| {
        if (Set.add(sobj, sds.cast(member.v.ptr))) {
            added += 1;
        }
    }
    cli.addReplyLongLong(added);
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
pub fn sscanCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const cursor = db.Scan.parseCursorOrReply(argv[2], cli) orelse {
        return;
    };

    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptyscan,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    db.Scan.scan(cli, sobj, cursor, Set.Hash, sscanCallback);
}

/// SREM key member [member ...]
pub fn sremCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    var deleted: i64 = 0;
    for (argv[2..]) |member| {
        if (Set.remove(sobj, sds.cast(member.v.ptr))) {
            deleted += 1;
            if (Set.size(sobj) == 0) {
                _ = cli.db.delete(key);
                break;
            }
        }
    }
    cli.addReplyLongLong(deleted);
}

/// SISMEMBER key member
pub fn sismemberCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    if (Set.isMember(sobj, sds.cast(argv[2].v.ptr))) {
        cli.addReply(Server.shared.cone);
        return;
    }
    cli.addReply(Server.shared.czero);
}

/// SCARD key
pub fn scardCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    cli.addReplyLongLong(@intCast(Set.size(sobj)));
}

fn sscanCallback(privdata: ?*anyopaque, entry: *const Set.Hash.Entry) void {
    const keys: *db.Scan.Keys = @ptrCast(@alignCast(privdata.?));
    keys.append(Object.createString(sds.asBytes(entry.key)));
}

pub const Set = struct {
    /// Return a set that *can* hold "value". When the object has
    /// an integer-encodable value, an intset will be returned.
    /// Otherwise a regular hash table.
    fn create(value: sds.String) *Object {
        _ = sds.asLongLong(value) orelse {
            return Object.createSet();
        };
        return Object.createIntSet();
    }

    /// Add the specified value into a set. If the value was already member of
    /// the set, nothing is done and FALSE is returned, otherwise the new
    /// element is added and TRUE is returned.
    fn add(sobj: *Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
            return h.add(sds.dupe(value), {});
        }
        if (sobj.encoding == .intset) {
            const llval = sds.asLongLong(value) orelse {
                convert(sobj, .ht);
                // The set *was* an intset and this value is not integer
                // encodable, so dict.add should always work.
                const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
                const ok = h.add(sds.dupe(value), {});
                assert(ok);
                return true;
            };
            const is: *IntSet = @ptrCast(@alignCast(sobj.v.ptr));
            const ret = is.add(llval);
            sobj.v = .{ .ptr = ret.set };
            if (ret.success) {
                // Convert to regular set when the intset contains
                // too many entries.
                var max_entries = server.set_max_intset_entries;
                // limit to 1G entries due to intset internals.
                if (max_entries >= (1 << 30)) max_entries = (1 << 30);
                if (ret.set.length.get() > max_entries) {
                    convert(sobj, .ht);
                }
                return true;
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    /// Convert the set to specified encoding. The resulting dict (when converting
    /// to a hash table) is presized to hold the number of elements in the original
    /// set.
    fn convert(sobj: *Object, enc: Object.Encoding) void {
        assert(sobj.type == .set and sobj.encoding == .intset);

        const is: *IntSet = @ptrCast(@alignCast(sobj.v.ptr));
        defer is.free();

        if (enc == .ht) {
            const h = Hash.create(vtable);
            // Presize the dict to avoid rehashing
            _ = h.expand(is.length.get());
            var it = Iterator.create(sobj);
            defer it.release();
            while (it.next()) |value| switch (value) {
                .num => |v| {
                    const s = sds.fromLonglong(v);
                    const ok = h.add(s, {});
                    assert(ok);
                },
                .s => unreachable,
            };
            sobj.v = .{ .ptr = h };
            sobj.encoding = .ht;
            return;
        }
        @panic("Unsupported set conversion");
    }

    fn remove(sobj: *Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
            if (h.delete(value)) {
                if (Server.needShrinkDictToFit(h.size(), h.slots())) {
                    _ = h.shrinkToFit();
                }
                return true;
            }
            return false;
        }
        if (sobj.encoding == .intset) {
            if (sds.asLongLong(value)) |num| {
                const is: *IntSet = @ptrCast(@alignCast(sobj.v.ptr));
                const ret = is.remove(num);
                sobj.v = .{ .ptr = ret.set };
                return ret.success;
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    fn isMember(sobj: *const Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
            return h.find(value) != null;
        }
        if (sobj.encoding == .intset) {
            if (sds.asLongLong(value)) |num| {
                const is: *IntSet = @ptrCast(@alignCast(sobj.v.ptr));
                return is.find(num);
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    fn size(sobj: *const Object) u64 {
        if (sobj.encoding == .ht) {
            const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
            return h.size();
        }
        if (sobj.encoding == .intset) {
            const is: *IntSet = @ptrCast(@alignCast(sobj.v.ptr));
            return is.length.get();
        }
        @panic("Unknown set encoding");
    }

    const Iterator = struct {
        subject: *Object,
        encoding: Object.Encoding,
        ii: ?u32 = null, // intset iterator
        di: ?Hash.Iterator = null,

        const Value = union(enum) {
            num: i64,
            s: sds.String,
        };

        pub fn create(sobj: *Object) Iterator {
            var it: Iterator = .{
                .subject = sobj,
                .encoding = sobj.encoding,
            };
            if (it.encoding == .ht) {
                const h: *Hash = @ptrCast(@alignCast(sobj.v.ptr));
                it.di = h.iterator(false);
            } else if (it.encoding == .intset) {
                it.ii = 0;
            } else {
                @panic("Unknown set encoding");
            }
            return it;
        }

        pub fn next(self: *Iterator) ?Value {
            if (self.encoding == .ht) {
                const entry = self.di.?.next() orelse {
                    return null;
                };
                return .{ .s = entry.key };
            }
            if (self.encoding == .intset) {
                const is: *IntSet = @ptrCast(@alignCast(self.subject.v.ptr));
                const v = is.get(self.ii.?) orelse {
                    return null;
                };
                self.ii.? += 1;
                return .{ .num = v };
            }
            @panic("Wrong set encoding in Iterator.next");
        }

        pub fn release(self: *Iterator) void {
            if (self.encoding == .ht) {
                self.di.?.release();
            }
        }
    };

    pub const Hash = dict.Dict(sds.String, void);

    const vtable: *const Hash.VTable = &.{
        .hash = hash,
        .eql = eql,
        .freeKey = sds.free,
    };

    fn hash(key: sds.String) dict.Hash {
        return dict.genHash(sds.asBytes(key));
    }

    fn eql(k1: sds.String, k2: sds.String) bool {
        return sds.cmp(k1, k2) == .eq;
    }
};

pub fn create() *Set.Hash {
    return Set.Hash.create(Set.vtable);
}

const Client = @import("networking.zig").Client;
const Object = @import("Object.zig");
const dict = @import("dict.zig");
const sds = @import("sds.zig");
const IntSet = @import("IntSet.zig");
const std = @import("std");
const assert = std.debug.assert;
const Server = @import("Server.zig");
const server = &Server.instance;
const db = @import("db.zig");
