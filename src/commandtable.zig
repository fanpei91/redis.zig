/// Our command table.
///
/// Every entry is composed of the following fields:
///
/// name: a string representing the command name.
/// proc: pointer to the function implementing the command.
/// arity: number of arguments, it is possible to use -N to say >= N.
pub const table = [_]Command{
    .{ .name = "ping", .proc = Server.pingCommand, .arity = -1 },
    .{ .name = "echo", .proc = Server.echoCommand, .arity = 2 },
    .{ .name = "auth", .proc = Server.authCommand, .arity = 2 },
    .{ .name = "time", .proc = Server.timeCommand, .arity = 1 },
    .{ .name = "select", .proc = dbx.selectCommand, .arity = 2 },
    .{ .name = "exists", .proc = dbx.existsCommand, .arity = -2 },
    .{ .name = "del", .proc = dbx.delCommand, .arity = -2 },
    .{ .name = "unlink", .proc = dbx.unlinkCommand, .arity = -2 },
    .{ .name = "rename", .proc = dbx.renameCommand, .arity = 3 },
    .{ .name = "renamenx", .proc = dbx.renamenxCommand, .arity = 3 },
    .{ .name = "dbsize", .proc = dbx.dbsizeCommand, .arity = 1 },
    .{ .name = "ttl", .proc = expire.ttlCommand, .arity = 2 },
    .{ .name = "pttl", .proc = expire.pttlCommand, .arity = 2 },
    .{ .name = "persist", .proc = expire.persistCommand, .arity = 2 },
    .{ .name = "touch", .proc = expire.touchCommand, .arity = -2 },
    .{ .name = "expire", .proc = expire.expireCommand, .arity = 3 },
    .{ .name = "expireat", .proc = expire.expireatCommand, .arity = 3 },
    .{ .name = "pexpire", .proc = expire.pexpireCommand, .arity = 3 },
    .{ .name = "pexpireat", .proc = expire.pexpireatCommand, .arity = 3 },
    .{ .name = "set", .proc = stringx.setCommand, .arity = -3 },
    .{ .name = "setnx", .proc = stringx.setnxCommand, .arity = 3 },
    .{ .name = "setex", .proc = stringx.setexCommand, .arity = 4 },
    .{ .name = "psetex", .proc = stringx.psetexCommand, .arity = 4 },
    .{ .name = "mset", .proc = stringx.msetCommand, .arity = -3 },
    .{ .name = "msetnx", .proc = stringx.msetnxCommand, .arity = -3 },
    .{ .name = "get", .proc = stringx.getCommand, .arity = 2 },
    .{ .name = "mget", .proc = stringx.mgetCommand, .arity = -2 },
    .{ .name = "getset", .proc = stringx.getsetCommand, .arity = 3 },
    .{ .name = "incr", .proc = stringx.incrCommand, .arity = 2 },
    .{ .name = "decr", .proc = stringx.decrCommand, .arity = 2 },
    .{ .name = "incrby", .proc = stringx.incrbyCommand, .arity = 3 },
    .{ .name = "decrby", .proc = stringx.decrbyCommand, .arity = 3 },
    .{ .name = "incrbyfloat", .proc = stringx.incrbyfloatCommand, .arity = 3 },
    .{ .name = "strlen", .proc = stringx.strlenCommand, .arity = 2 },
    .{ .name = "append", .proc = stringx.appendCommand, .arity = 3 },
    .{ .name = "setrange", .proc = stringx.setrangeCommand, .arity = 4 },
    .{ .name = "getrange", .proc = stringx.getrangeCommand, .arity = 4 },
    .{ .name = "lpush", .proc = listx.lpushCommand, .arity = -3 },
    .{ .name = "rpush", .proc = listx.rpushCommand, .arity = -3 },
    .{ .name = "lpushx", .proc = listx.lpushxCommand, .arity = -3 },
    .{ .name = "rpushx", .proc = listx.rpushxCommand, .arity = -3 },
    .{ .name = "lpop", .proc = listx.lpopCommand, .arity = 2 },
    .{ .name = "rpop", .proc = listx.rpopCommand, .arity = 2 },
    .{ .name = "linsert", .proc = listx.linsertCommand, .arity = 5 },
    .{ .name = "lindex", .proc = listx.lindexCommand, .arity = 3 },
    .{ .name = "lset", .proc = listx.lsetCommand, .arity = 4 },
    .{ .name = "llen", .proc = listx.llenCommand, .arity = 2 },
    .{ .name = "lrange", .proc = listx.lrangeCommand, .arity = 4 },
    .{ .name = "ltrim", .proc = listx.ltrimCommand, .arity = 4 },
};

pub const Command = struct {
    pub const Proc = *const fn (cli: *Client) void;

    name: []const u8,
    proc: Proc,
    arity: i32,
};

const Client = @import("networking.zig").Client;
const stringx = @import("t_string.zig");
const listx = @import("t_list.zig");
const dbx = @import("db.zig");
const expire = @import("expire.zig");
const Server = @import("Server.zig");
const sds = @import("sds.zig");
