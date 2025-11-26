/// LPUSH key element [element ...]
pub fn lpushCommand(cli: *Client) void {
    push(cli, .head);
}

/// RPUSH key element [element ...]
pub fn rpushCommand(cli: *Client) void {
    push(cli, .tail);
}

/// LPUSHX key element [element ...]
pub fn lpushxCommand(cli: *Client) void {
    pushx(cli, .head);
}

/// RPUSHX key element [element ...]
pub fn rpushxCommand(cli: *Client) void {
    pushx(cli, .tail);
}

/// LPOP key
pub fn lpopCommand(cli: *Client) void {
    pop(cli, .head);
}

/// RPOP key
pub fn rpopCommand(cli: *Client) void {
    pop(cli, .tail);
}

/// RPOPLPUSH source destination
pub fn rpoplpushCommand(cli: *Client) void {
    const argv = cli.argv.?;

    const srckey = argv[1];
    const srcobj = cli.db.lookupKeyWriteOrReply(
        cli,
        srckey,
        Server.shared.nullbulk,
    ) orelse return;
    if (srcobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const dstkey = argv[2];
    const dstobj = cli.db.lookupKeyWrite(dstkey);
    if (dstobj) |dst| if (dst.checkTypeOrReply(cli, .list)) {
        return;
    };

    const value = List.pop(srcobj, .tail).?;
    defer value.decrRefCount();

    rpoplpush(cli, dstkey, dstobj, value);

    if (List.length(srcobj) == 0) {
        _ = cli.db.delete(srckey);
    }
}

/// LINSERT key <BEFORE | AFTER> pivot element
pub fn linsertCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var where: Where = undefined;
    const arg2 = sds.asBytes(sds.cast(argv[2].v.ptr));
    if (std.ascii.eqlIgnoreCase(arg2, "after")) {
        where = .tail;
    } else if (std.ascii.eqlIgnoreCase(arg2, "before")) {
        where = .head;
    } else {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    const key = argv[1];
    const lobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    // Seek pivot from head to tail
    var iter = List.Iterator.create(lobj, 0, .tail);
    const pivot = argv[3];
    const element = argv[4];
    var inserted = false;
    var entry: List.Entry = undefined;
    while (iter.next(&entry)) {
        if (entry.eql(pivot)) {
            entry.insert(element, where);
            inserted = true;
            break;
        }
    }
    if (inserted) {
        cli.addReplyLongLong(List.length(lobj));
        return;
    }
    cli.addReply(Server.shared.cnegone);
}

/// LINDEX key index
pub fn lindexCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const lobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const index = argv[2].getLongLongOrReply(cli, null) orelse return;

    if (lobj.encoding != .quicklist) {
        @branchHint(.unlikely);
        @panic("Unknown list encoding");
    }

    const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
    var entry: QuickList.Entry = undefined;
    if (ql.index(index, &entry)) {
        var obj: *Object = undefined;
        defer obj.decrRefCount();
        if (entry.value) |value| {
            obj = Object.createString(value[0..entry.sz]);
        } else {
            obj = Object.createStringFromLonglong(entry.longval);
        }
        cli.addReplyBulk(obj);
        return;
    }
    cli.addReply(Server.shared.nullbulk);
}

/// LSET key index element
pub fn lsetCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const lobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nokeyerr,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const index = argv[2].getLongLongOrReply(cli, null) orelse return;

    if (lobj.encoding != .quicklist) {
        @branchHint(.unlikely);
        @panic("Unknown list encoding");
    }

    const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
    const element = sds.asBytes(sds.cast(argv[3].v.ptr));
    if (ql.replaceAtIndex(index, element)) {
        cli.addReply(Server.shared.ok);
        return;
    }
    cli.addReply(Server.shared.outofrangeerr);
}

/// LLEN key
pub fn llenCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const lobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }
    cli.addReplyLongLong(List.length(lobj));
}

/// LRANGE key start end
pub fn lrangeCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var start = argv[2].getLongLongOrReply(cli, null) orelse return;
    var end = argv[3].getLongLongOrReply(cli, null) orelse return;

    const key = argv[1];
    const lobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const llen: i64 = List.length(lobj);
    // convert negative indexes
    if (start < 0) start = llen +% start;
    if (end < 0) end = llen +% end;
    if (start < 0) start = 0;

    // Invariant: start >= 0, so this test will be true when end < 0.
    // The range is empty when start > end or start >= length.
    if (start > end or start >= llen) {
        cli.addReply(Server.shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen - 1;

    // Return the result in form of a multi-bulk reply
    var rangelen = end - start + 1;
    cli.addReplyMultiBulkLen(@intCast(rangelen));
    var iter = List.Iterator.create(lobj, start, .tail);
    while (rangelen > 0) : (rangelen -= 1) {
        var entry: List.Entry = undefined;
        _ = iter.next(&entry);
        if (entry.entry.value) |value| {
            cli.addReplyBulkString(value[0..entry.entry.sz]);
        } else {
            cli.addReplyBulkLongLong(entry.entry.longval);
        }
    }
}

/// LTRIM key start end
pub fn ltrimCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var start = argv[2].getLongLongOrReply(cli, null) orelse return;
    var end = argv[3].getLongLongOrReply(cli, null) orelse return;

    const key = argv[1];
    const lobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.ok,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const llen: i64 = List.length(lobj);
    // convert negative indexes
    if (start < 0) start = llen +% start;
    if (end < 0) end = llen +% end;
    if (start < 0) start = 0;

    var ltrim: i64 = undefined;
    var rtrim: i64 = undefined;

    // Invariant: start >= 0, so this test will be true when end < 0.
    // The range is empty when start > end or start >= length.
    if (start > end or start >= llen) {
        // Out of range start or start > end result in empty list
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen - 1;
        ltrim = start;
        rtrim = llen - end - 1;
    }

    if (lobj.encoding != .quicklist) {
        @branchHint(.unlikely);
        @panic("Unknown list encoding");
    }

    const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
    _ = ql.delRange(0, ltrim);
    _ = ql.delRange(-rtrim, rtrim);
    if (List.length(lobj) == 0) {
        _ = cli.db.delete(key);
    }
    cli.addReply(Server.shared.ok);
}

/// LREM key count element
pub fn lremCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var toremove = argv[2].getLongLongOrReply(cli, null) orelse return;
    const key = argv[1];
    const lobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        cli.addReply(Server.shared.czero);
        return;
    };
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    const element = argv[3];
    var iter: List.Iterator = blk: {
        if (toremove < 0) {
            toremove = -toremove;
            break :blk List.Iterator.create(lobj, -1, .head);
        }
        break :blk List.Iterator.create(lobj, 0, .tail);
    };
    var removed: i64 = 0;
    var entry: List.Entry = undefined;
    while (iter.next(&entry)) {
        if (entry.eql(element)) {
            entry.delete();
            removed += 1;
            if (toremove > 0 and removed == toremove) {
                break;
            }
        }
    }

    if (List.length(lobj) == 0) {
        _ = cli.db.delete(key);
    }

    cli.addReplyLongLong(removed);
}

/// BLPOP key [key ...] timeout
pub fn blpopCommand(cli: *Client) void {
    bpop(cli, .head);
}

/// BRPOP key [key ...] timeout
pub fn brpopCommand(cli: *Client) void {
    bpop(cli, .tail);
}

/// BRPOPLPUSH source destination timeout
pub fn brpoplpushCommand(cli: *Client) void {
    const argv = cli.argv.?;

    const timeout = blocked.getTimeoutFromObjectOrReply(
        cli,
        argv[3],
        Server.UNIT_SECONDS,
    ) orelse return;

    const srcobj = cli.db.lookupKeyWrite(argv[1]) orelse {
        const keys = argv[1..2];
        const target = argv[2];
        blocked.blockForKeys(
            cli,
            Server.BLOCKED_LIST,
            keys,
            timeout,
            target,
        );
        return;
    };
    if (srcobj.checkTypeOrReply(cli, .list)) {
        return;
    }

    // The list exists and has elements, so
    // the regular rpoplpushCommand is executed.
    assert(List.length(srcobj) > 0);
    rpoplpushCommand(cli);
}

/// BLPOP/BRPOP key [key ...] timeout
fn bpop(cli: *Client, where: Where) void {
    const argv = cli.argv.?;

    const timeout = blocked.getTimeoutFromObjectOrReply(
        cli,
        argv[cli.argc - 1],
        Server.UNIT_SECONDS,
    ) orelse return;

    const keys = argv[1 .. cli.argc - 1];
    for (keys) |key| {
        const lobj = cli.db.lookupKeyWrite(key) orelse continue;
        if (lobj.checkTypeOrReply(cli, .list)) {
            return;
        }
        const value = List.pop(lobj, where).?;
        defer value.decrRefCount();

        cli.addReplyMultiBulkLen(2);
        cli.addReplyBulk(key);
        cli.addReplyBulk(value);

        if (List.length(lobj) == 0) {
            _ = cli.db.delete(key);
        }
        return;
    }

    // If the keys does not exists we must block
    blocked.blockForKeys(cli, Server.BLOCKED_LIST, keys, timeout, null);
}

///  This is the semantic of this command:
///  RPOPLPUSH srclist dstlist:
///    IF LLEN(srclist) > 0
///      element = RPOP srclist
///      LPUSH dstlist element
///      RETURN element
///    ELSE
///      RETURN nil
///    END
///  END
///
/// The idea is to be able to get an element from a list in a reliable way
/// since the element is not just returned but pushed against another list
/// as well.
fn rpoplpush(
    cli: *Client,
    dstkey: *Object,
    dstobj: ?*Object,
    value: *Object,
) void {
    const destlist = dstobj orelse blk: {
        const l = List.create();
        defer l.decrRefCount();
        cli.db.add(dstkey, l);
        break :blk l;
    };
    List.push(destlist, value, .head);
    // Always send the pushed value to the client.
    cli.addReplyBulk(value);
}

/// LPUSH/RPUSH key element [element ...]
fn push(cli: *Client, where: Where) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const list = cli.db.lookupKeyWrite(key) orelse blk: {
        const obj = List.create();
        defer obj.decrRefCount();
        cli.db.add(key, obj);
        break :blk obj;
    };
    if (list.checkTypeOrReply(cli, .list)) {
        return;
    }

    for (argv[2..cli.argc]) |element| {
        List.push(list, element, where);
    }
    cli.addReplyLongLong(List.length(list));
}

/// LPUSHX/RPUSHX key element [element ...]
fn pushx(cli: *Client, where: Where) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const list = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (list.checkTypeOrReply(cli, .list)) {
        return;
    }

    for (argv[2..cli.argc]) |element| {
        List.push(list, element, where);
    }
    cli.addReplyLongLong(List.length(list));
}

/// LPOP/RPOP key
fn pop(cli: *Client, where: Where) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const lobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse return;
    if (lobj.checkTypeOrReply(cli, .list)) {
        return;
    }
    const value = List.pop(lobj, where) orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    defer value.decrRefCount();
    cli.addReplyBulk(value);

    if (List.length(lobj) == 0) {
        _ = cli.db.delete(key);
    }
}

/// This is a helper function for blocked.handleClientsBlockedOnLists().
/// It's work is to serve a specific client (receiver) that is blocked on 'key'
/// in the context of the specified 'db', doing the following:
///
/// 1) Provide the client with the 'value' element.
/// 2) If the dstkey is not null (we are serving a BRPOPLPUSH) also push the
///    'value' element on the destination list (the LPUSH side of the command).
/// 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
///    the AOF and replication channel.
///
/// The argument 'where' is .tail or .head, and indicates if the
/// 'value' element was popped from the head (BLPOP) or tail (BRPOP) so that
/// we can propagate the command properly.
///
/// The function returns true if we are able to serve the client, otherwise
/// false is returned to signal the caller that the list POP operation
/// should be undone as the client was not served: This only happens for
/// BRPOPLPUSH that fails to push the value to the destination key as it is
/// of the wrong type.
pub fn serveClientBlockedOnList(
    receiver: *Client,
    key: *Object,
    dstkey: ?*Object,
    db: *Database,
    value: *Object,
    where: Where,
) bool {
    _ = db;
    _ = where;

    // BRPOPLPUSH
    if (dstkey) |dst| {
        const dstobj = receiver.db.lookupKeyWrite(dst);
        if (dstobj) |obj| if (obj.checkTypeOrReply(receiver, .list)) {
            @branchHint(.unlikely);
            return false;
        };
        rpoplpush(receiver, dst, dstobj, value);
        return true;
    }

    // BLPOP/BRPOP
    receiver.addReplyMultiBulkLen(2);
    receiver.addReplyBulk(key);
    receiver.addReplyBulk(value);
    return true;
}

pub const Where = enum {
    head,
    tail,
};

pub const List = struct {
    pub const Entry = struct {
        iter: *Iterator,
        entry: QuickList.Entry,

        pub fn eql(self: *Entry, obj: *Object) bool {
            if (self.iter.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            return QuickList.eql(
                self.entry.zi.?,
                sds.asBytes(sds.cast(obj.v.ptr)),
            );
        }

        pub fn delete(self: *Entry) void {
            if (self.iter.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            self.iter.iter.?.delEntry(&self.entry);
        }

        pub fn insert(self: *Entry, obj: *Object, where: Where) void {
            if (self.iter.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            const value = obj.getDecoded();
            defer value.decrRefCount();
            const ql = self.entry.quicklist.?;
            const str = sds.asBytes(sds.cast(value.v.ptr));
            if (where == .tail) {
                ql.insertAfter(&self.entry, str);
            } else {
                ql.insertBefore(&self.entry, str);
            }
        }
    };

    pub const Iterator = struct {
        subject: *Object,
        encoding: Object.Encoding,
        direction: Where,
        iter: ?QuickList.Iterator,

        /// Create an iterator at the specified index.
        pub fn create(lobj: *Object, index: i64, direction: Where) Iterator {
            if (lobj.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
            return .{
                .subject = lobj,
                .encoding = lobj.encoding,
                .direction = direction,
                .iter = ql.iteratorAtIndex(
                    // .head means start at TAIL and move *towards* head.
                    // .tail means start at HEAD and move *towards tail.
                    if (direction == .head) .tail else .head,
                    index,
                ),
            };
        }

        /// Stores pointer to current the entry in the provided entry structure
        /// and advances the position of the iterator.
        /// Returns true when the current entry is in fact an entry,
        /// false otherwise.
        pub fn next(self: *Iterator, entry: *Entry) bool {
            assert(self.encoding == self.subject.encoding);
            if (self.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            entry.iter = self;
            return self.iter.?.next(&entry.entry);
        }
    };

    pub fn create() *Object {
        const obj = Object.createQuickList();
        const ql: *QuickList = @ptrCast(@alignCast(obj.v.ptr));
        ql.setOptions(
            server.list_max_ziplist_size,
            server.list_compress_depth,
        );
        return obj;
    }

    pub fn length(lobj: *Object) i64 {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
        return @intCast(ql.count);
    }

    pub fn push(lobj: *Object, element: *Object, where: Where) void {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
        const value = element.getDecoded();
        defer value.decrRefCount();
        ql.push(
            sds.asBytes(sds.cast(value.v.ptr)),
            if (where == .head) .head else .tail,
        );
    }

    pub fn pop(lobj: *Object, where: Where) ?*Object {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.v.ptr));
        const obj = ql.pop(
            if (where == .head) .head else .tail,
            popSaver,
        ) orelse return null;
        return @ptrCast(@alignCast(obj));
    }

    fn popSaver(value: QuickList.popSaverValue) *anyopaque {
        return switch (value) {
            .num => |v| Object.createStringFromLonglong(v),
            .str => |v| Object.createString(v),
        };
    }
};

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const QuickList = @import("QuickList.zig");
const allocator = @import("allocator.zig");
const assert = std.debug.assert;
const blocked = @import("blocked.zig");
const Database = @import("db.zig").Database;
