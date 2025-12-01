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
    .{ .name = "object", .proc = Object.objectCommand, .arity = -2 },
    .{ .name = "select", .proc = db.selectCommand, .arity = 2 },
    .{ .name = "exists", .proc = db.existsCommand, .arity = -2 },
    .{ .name = "del", .proc = db.delCommand, .arity = -2 },
    .{ .name = "unlink", .proc = db.unlinkCommand, .arity = -2 },
    .{ .name = "rename", .proc = db.renameCommand, .arity = 3 },
    .{ .name = "renamenx", .proc = db.renamenxCommand, .arity = 3 },
    .{ .name = "dbsize", .proc = db.dbsizeCommand, .arity = 1 },
    .{ .name = "randomkey", .proc = db.randomkeyCommand, .arity = 1 },
    .{ .name = "move", .proc = db.moveCommand, .arity = 3 },
    .{ .name = "scan", .proc = db.scanCommand, .arity = -2 },
    .{ .name = "ttl", .proc = expire.ttlCommand, .arity = 2 },
    .{ .name = "pttl", .proc = expire.pttlCommand, .arity = 2 },
    .{ .name = "persist", .proc = expire.persistCommand, .arity = 2 },
    .{ .name = "touch", .proc = expire.touchCommand, .arity = -2 },
    .{ .name = "expire", .proc = expire.expireCommand, .arity = 3 },
    .{ .name = "expireat", .proc = expire.expireatCommand, .arity = 3 },
    .{ .name = "pexpire", .proc = expire.pexpireCommand, .arity = 3 },
    .{ .name = "pexpireat", .proc = expire.pexpireatCommand, .arity = 3 },
    .{ .name = "set", .proc = string.setCommand, .arity = -3 },
    .{ .name = "setnx", .proc = string.setnxCommand, .arity = 3 },
    .{ .name = "setex", .proc = string.setexCommand, .arity = 4 },
    .{ .name = "psetex", .proc = string.psetexCommand, .arity = 4 },
    .{ .name = "mset", .proc = string.msetCommand, .arity = -3 },
    .{ .name = "msetnx", .proc = string.msetnxCommand, .arity = -3 },
    .{ .name = "get", .proc = string.getCommand, .arity = 2 },
    .{ .name = "mget", .proc = string.mgetCommand, .arity = -2 },
    .{ .name = "getset", .proc = string.getsetCommand, .arity = 3 },
    .{ .name = "incr", .proc = string.incrCommand, .arity = 2 },
    .{ .name = "decr", .proc = string.decrCommand, .arity = 2 },
    .{ .name = "incrby", .proc = string.incrbyCommand, .arity = 3 },
    .{ .name = "decrby", .proc = string.decrbyCommand, .arity = 3 },
    .{ .name = "incrbyfloat", .proc = string.incrbyfloatCommand, .arity = 3 },
    .{ .name = "strlen", .proc = string.strlenCommand, .arity = 2 },
    .{ .name = "append", .proc = string.appendCommand, .arity = 3 },
    .{ .name = "setrange", .proc = string.setrangeCommand, .arity = 4 },
    .{ .name = "getrange", .proc = string.getrangeCommand, .arity = 4 },
    .{ .name = "lpush", .proc = list.lpushCommand, .arity = -3 },
    .{ .name = "rpush", .proc = list.rpushCommand, .arity = -3 },
    .{ .name = "lpushx", .proc = list.lpushxCommand, .arity = -3 },
    .{ .name = "rpushx", .proc = list.rpushxCommand, .arity = -3 },
    .{ .name = "lpop", .proc = list.lpopCommand, .arity = 2 },
    .{ .name = "rpop", .proc = list.rpopCommand, .arity = 2 },
    .{ .name = "rpoplpush", .proc = list.rpoplpushCommand, .arity = 3 },
    .{ .name = "blpop", .proc = list.blpopCommand, .arity = -3 },
    .{ .name = "brpop", .proc = list.brpopCommand, .arity = -3 },
    .{ .name = "brpoplpush", .proc = list.brpoplpushCommand, .arity = 4 },
    .{ .name = "linsert", .proc = list.linsertCommand, .arity = 5 },
    .{ .name = "lindex", .proc = list.lindexCommand, .arity = 3 },
    .{ .name = "lset", .proc = list.lsetCommand, .arity = 4 },
    .{ .name = "llen", .proc = list.llenCommand, .arity = 2 },
    .{ .name = "lrange", .proc = list.lrangeCommand, .arity = 4 },
    .{ .name = "ltrim", .proc = list.ltrimCommand, .arity = 4 },
    .{ .name = "lrem", .proc = list.lremCommand, .arity = 4 },
    .{ .name = "hset", .proc = hash.hsetCommand, .arity = -4 },
    .{ .name = "hmset", .proc = hash.hsetCommand, .arity = -4 },
    .{ .name = "hincrby", .proc = hash.hincrbyCommand, .arity = 4 },
    .{ .name = "hincrbyfloat", .proc = hash.hincrbyfloatCommand, .arity = 4 },
    .{ .name = "hsetnx", .proc = hash.hsetnxCommand, .arity = 4 },
    .{ .name = "hget", .proc = hash.hgetCommand, .arity = 3 },
    .{ .name = "hmget", .proc = hash.hmgetCommand, .arity = -3 },
    .{ .name = "hdel", .proc = hash.hdelCommand, .arity = -3 },
    .{ .name = "hlen", .proc = hash.hlenCommand, .arity = 2 },
    .{ .name = "hstrlen", .proc = hash.hstrlenCommand, .arity = 3 },
    .{ .name = "hscan", .proc = hash.hscanCommand, .arity = -3 },
    .{ .name = "hexists", .proc = hash.hexistsCommand, .arity = 3 },
    .{ .name = "hkeys", .proc = hash.hkeysCommand, .arity = 2 },
    .{ .name = "hvals", .proc = hash.hvalsCommand, .arity = 2 },
    .{ .name = "hgetall", .proc = hash.hgetallCommand, .arity = 2 },
    .{ .name = "sadd", .proc = set.saddCommand, .arity = -3 },
    .{ .name = "sscan", .proc = set.sscanCommand, .arity = -3 },
    .{ .name = "srem", .proc = set.sremCommand, .arity = -3 },
    .{ .name = "sismember", .proc = set.sismemberCommand, .arity = 3 },
    .{ .name = "scard", .proc = set.scardCommand, .arity = 2 },
    .{ .name = "smove", .proc = set.smoveCommand, .arity = 4 },
    .{ .name = "sunion", .proc = set.sunionCommand, .arity = -2 },
    .{ .name = "sunionstore", .proc = set.sunionstoreCommand, .arity = -3 },
    .{ .name = "sdiff", .proc = set.sdiffCommand, .arity = -2 },
    .{ .name = "sdiffstore", .proc = set.sdiffstoreCommand, .arity = -3 },
    .{ .name = "sinter", .proc = set.sinterCommand, .arity = -2 },
    .{ .name = "smembers", .proc = set.sinterCommand, .arity = 2 },
    .{ .name = "sinterstore", .proc = set.sinterstoreCommand, .arity = -3 },
    .{ .name = "spop", .proc = set.spopCommand, .arity = -2 },
    .{ .name = "srandmember", .proc = set.srandmemberCommand, .arity = -2 },
    .{ .name = "zadd", .proc = zset.zaddCommand, .arity = -4 },
    .{ .name = "zincrby", .proc = zset.zincrbyCommand, .arity = 4 },
    .{ .name = "zcard", .proc = zset.zcardCommand, .arity = 2 },
    .{ .name = "zscore", .proc = zset.zscoreCommand, .arity = 3 },
    .{ .name = "zrank", .proc = zset.zrankCommand, .arity = 3 },
    .{ .name = "zrevrank", .proc = zset.zrevrankCommand, .arity = 3 },
    .{ .name = "zrem", .proc = zset.zremCommand, .arity = -3 },
};

pub const Command = struct {
    pub const Proc = *const fn (cli: *Client) void;

    name: []const u8,
    proc: Proc,
    arity: i32,
};

const Client = @import("networking.zig").Client;
const string = @import("t_string.zig");
const list = @import("t_list.zig");
const hash = @import("t_hash.zig");
const set = @import("t_set.zig");
const zset = @import("t_zset.zig");
const db = @import("db.zig");
const expire = @import("expire.zig");
const Server = @import("Server.zig");
const sds = @import("sds.zig");
const Object = @import("Object.zig");
