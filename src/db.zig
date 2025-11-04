pub const Database = struct {
    allocator: Allocator,
    dict: *Dict, // The keyspace for this DB
    id: usize, // Database ID

    pub fn create(allocator: Allocator, id: usize) Allocator.Error!Database {
        return .{
            .allocator = allocator,
            .dict = try Dict.create(allocator, Server.dbDictVTable, null),
            .id = id,
        };
    }

    pub fn destroy(self: *Database) void {
        self.dict.destroy(self.allocator);
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
