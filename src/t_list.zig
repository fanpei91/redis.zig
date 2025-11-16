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
                sds.asBytes(sds.cast(obj.data.ptr)),
            );
        }

        pub fn insert(self: *Entry, obj: *Object, where: Where) void {
            if (self.iter.encoding != .quicklist) {
                @branchHint(.unlikely);
                @panic("Unknown list encoding");
            }
            const value = obj.getDecoded();
            defer value.decrRefCount();
            const ql = self.entry.quicklist.?;
            const str = sds.asBytes(sds.cast(value.data.ptr));
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

        /// Stores pointer to current the entry in the provided entry structure
        /// and advances the position of the iterator. Returns true when the current
        /// entry is in fact an entry, false otherwise.
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

    /// Create an iterator at the specified index.
    pub fn iterator(lobj: *Object, index: i64, direction: Where) Iterator {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
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

    pub fn length(lobj: *Object) i64 {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
        return @intCast(ql.count);
    }

    pub fn push(lobj: *Object, element: *Object, where: Where) void {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
        const value = element.getDecoded();
        defer value.decrRefCount();
        ql.push(
            sds.asBytes(sds.cast(value.data.ptr)),
            if (where == .head) .head else .tail,
        );
    }

    pub fn pop(lobj: *Object, where: Where) ?*Object {
        if (lobj.encoding != .quicklist) {
            @branchHint(.unlikely);
            @panic("Unknown list encoding");
        }
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
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

/// LINSERT key <BEFORE | AFTER> pivot element
pub fn linsertCommand(cli: *Client) void {
    const argv = cli.argv.?;

    var where: Where = undefined;
    const arg2 = sds.asBytes(sds.cast(argv[2].data.ptr));
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
    var iter = List.iterator(lobj, 0, .tail);
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

/// LLEN key
pub fn llenCommand(cli: *Client) void {
    const key = cli.argv.?[1];
    const lobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    );
    if (lobj == null or lobj.?.checkTypeOrReply(cli, .list)) {
        return;
    }
    cli.addReplyLongLong(List.length(lobj.?));
}

/// LPUSH/RPUSH key element [element ...]
fn push(cli: *Client, where: Where) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const list = cli.db.lookupKeyWrite(key) orelse blk: {
        const obj = Object.createQuickList();
        const ql: *QuickList = @ptrCast(@alignCast(obj.data.ptr));
        ql.setOptions(
            server.list_max_ziplist_size,
            server.list_compress_depth,
        );
        cli.db.add(key, obj);
        break :blk obj;
    };
    if (list.checkTypeOrReply(cli, .list)) {
        return;
    }

    for (argv[2..]) |element| {
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

    for (argv[2..]) |element| {
        List.push(list, element, where);
    }
    cli.addReplyLongLong(List.length(list));
}

/// LPOP/RPOP key
fn pop(cli: *Client, where: Where) void {
    const key = cli.argv.?[1];
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

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const QuickList = @import("QuickList.zig");
const allocator = @import("allocator.zig");
const assert = std.debug.assert;
