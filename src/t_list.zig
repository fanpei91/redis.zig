const LIST_MAX_ITEM_SIZE = ((1 << 32) - 1024);

/// LPUSH key element [element ...]
pub fn lpushCommand(cli: *Client) void {
    push(cli, Server.LIST_HEAD);
}

/// RPUSH key element [element ...]
pub fn rpushCommand(cli: *Client) void {
    push(cli, Server.LIST_TAIL);
}

fn push(cli: *Client, where: comptime_int) void {
    const argv = cli.argv.?;
    for (argv[2..]) |arg| {
        if (sds.getLen(sds.cast(arg.data.ptr)) > LIST_MAX_ITEM_SIZE) {
            cli.addReplyErr("Element too large");
            return;
        }
    }

    const key = argv[1];
    const lobj = cli.db.lookupKeyWrite(key);
    if (lobj != null and lobj.?.type != .list) {
        cli.addReply(Server.shared.wrongtypeerr);
        return;
    }

    const list = lobj orelse blk: {
        const obj = Object.createQuickList();
        const ql: *QuickList = @ptrCast(@alignCast(obj.data.ptr));
        ql.setOptions(
            server.list_max_ziplist_size,
            server.list_compress_depth,
        );
        cli.db.add(key, obj);
        break :blk obj;
    };

    for (argv[2..]) |element| {
        listPush(list, element, where);
    }
    cli.addReplyLongLong(@intCast(listLength(list)));
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

fn listLength(lobj: *Object) u64 {
    if (lobj.encoding == .quicklist) {
        const ql: *QuickList = @ptrCast(@alignCast(lobj.data.ptr));
        return ql.count;
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
