/// LPUSH key element [element ...]
pub fn lpushCommand(cli: *Client) void {
    push(cli, Server.LIST_HEAD);
}

/// RPUSH key element [element ...]
pub fn rpushCommand(cli: *Client) void {
    push(cli, Server.LIST_TAIL);
}

/// LPUSHX key element [element ...]
pub fn lpushxCommand(cli: *Client) void {
    pushx(cli, Server.LIST_HEAD);
}

/// RPUSHX key element [element ...]
pub fn rpushxCommand(cli: *Client) void {
    pushx(cli, Server.LIST_TAIL);
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
    cli.addReplyLongLong(listLength(lobj.?));
}

/// LPUSH/RPUSH key element [element ...]
fn push(cli: *Client, where: comptime_int) void {
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
        listPush(list, element, where);
    }
    cli.addReplyLongLong(listLength(list));
}

/// LPUSHX/RPUSHX key element [element ...]
fn pushx(cli: *Client, where: comptime_int) void {
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
        listPush(list, element, where);
    }
    cli.addReplyLongLong(listLength(list));
}

fn listPush(lobj: *Object, element: *Object, where: comptime_int) void {
    if (lobj.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
        const value = element.getDecoded();
        ql.push(
            sds.asBytes(sds.cast(value.data.ptr)),
            if (where == Server.LIST_HEAD) .head else .tail,
        );
        value.decrRefCount();
    } else {
        @panic("Unknown list encoding");
    }
}

fn listLength(lobj: *Object) i64 {
    if (lobj.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
        return @intCast(ql.count);
    } else {
        @panic("Unknown list encoding");
    }
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const server = &Server.instance;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const QuickList = @import("QuickList.zig");
