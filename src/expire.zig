/// TTL key
pub fn ttlCommand(cli: *Client) void {
    ttl(cli, false);
}

/// PTTL key
pub fn pttlCommand(cli: *Client) void {
    ttl(cli, true);
}

/// PERSIST key
pub fn persistCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    if (cli.db.lookupKeyWrite(key) != null) {
        if (cli.db.removeExpire(key)) {
            cli.addReply(Server.shared.cone);
            server.dirty +%= 1;
        } else {
            cli.addReply(Server.shared.czero);
        }
    } else {
        cli.addReply(Server.shared.czero);
    }
}

/// TOUCH key [key ...]
pub fn touchCommand(cli: *Client) void {
    const argv = cli.argv.?;
    var touched: i64 = 0;
    for (argv[1..]) |key| {
        if (cli.db.lookupKeyRead(key) != null) {
            touched += 1;
        }
    }
    cli.addReplyLongLong(touched);
}

/// EXPIRE key seconds
pub fn expireCommand(cli: *Client) void {
    expireAt(cli, std.time.milliTimestamp(), Server.UNIT_SECONDS);
}

/// EXPIREAT key sec_time
pub fn expireatCommand(cli: *Client) void {
    expireAt(cli, 0, Server.UNIT_SECONDS);
}

/// PEXPIRE key milliseconds
pub fn pexpireCommand(cli: *Client) void {
    expireAt(cli, std.time.milliTimestamp(), Server.UNIT_MILLISECONDS);
}

/// PEXPIREAT key ms_time
pub fn pexpireatCommand(cli: *Client) void {
    expireAt(cli, 0, Server.UNIT_MILLISECONDS);
}

/// This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
/// and PEXPIREAT. Because the commad second argument may be relative or absolute
/// the "basetime" argument is used to signal what the base time is (either 0
/// for *AT variants of the command, or the current time for relative expires).
///
/// unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
/// the argv[2] parameter. The basetime is always specified in milliseconds.
fn expireAt(cli: *Client, basetime: i64, unit: u32) void {
    const argv = cli.argv.?;

    // unix time in milliseconds when the key will expire.
    var when = argv[2].getLongLongOrReply(cli, null) orelse return;

    if (unit == Server.UNIT_SECONDS) {
        when *= std.time.ms_per_s;
    }
    when += basetime;

    // No key, return zero.
    const key = argv[1];
    if (cli.db.lookupKeyWrite(key) == null) {
        cli.addReply(Server.shared.czero);
        return;
    }

    if (checkAlreadyExpired(when)) {
        assert(cli.db.delete(key));
        server.dirty +%= 1;

        //Replicate/AOF this as an explicit DEL or UNLINK.
        const cmd = if (server.lazyfree_lazy_expire)
            Server.shared.unlink
        else
            Server.shared.del;
        cli.rewriteCommandVector(&.{ cmd, key });
        cli.db.signalModifiedKey(key);
        cli.addReply(Server.shared.cone);
        return;
    }
    cli.db.setExpire(cli, key, when);
    cli.db.signalModifiedKey(key);
    cli.addReply(Server.shared.cone);
    server.dirty +%= 1;
}

fn checkAlreadyExpired(when: i64) bool {
    // EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
    // should never be executed as a DEL when load the AOF or in the context
    // of a slave instance.
    //
    // Instead we add the already expired key to the database with expire time
    // (possibly in the past) and wait for an explicit DEL from the master.
    return std.time.milliTimestamp() >= when and !server.loading;
}

/// Implements TTL and PTTL
fn ttl(cli: *Client, output_ms: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // If the key does not exist at all, return -2
    _ = cli.db.lookupKeyReadWithFlags(key, Server.LOOKUP_NOTOUCH) orelse {
        cli.addReplyLongLong(-2);
        return;
    };

    // The key exists. Return -1 if it has no expire, or the actual
    // TTL value otherwise.
    var t: i64 = -1;
    const expire = cli.db.getExpire(key);
    if (expire != -1) {
        t = expire - std.time.milliTimestamp();
        if (t < 0) t = 0;
    }
    if (t == -1) {
        cli.addReplyLongLong(-1);
    } else {
        cli.addReplyLongLong(
            if (output_ms)
                t
            else
                @divFloor(t, std.time.ms_per_s),
        );
    }
}

/// Try to expire a few timed out keys. The algorithm used is adaptive and
/// will use few CPU cycles if there are few expiring keys, otherwise
/// it will get more aggressive to avoid that too much memory is used by
/// keys that can be removed from the keyspace.
///
/// No more than CRON_DBS_PER_CALL databases are tested at every
/// iteration.
///
/// This kind of call is used when Redis detects that timelimit_exit is
/// true, so there is more work to do, and we do it more incrementally from
/// the beforeSleep() function of the event loop.
///
/// Expire cycle type:
///
/// If type is ACTIVE_EXPIRE_CYCLE_FAST the function will try to run a
/// "fast" expire cycle that takes no longer than EXPIRE_FAST_CYCLE_DURATION
/// microseconds, and is not repeated again before the same amount of time.
///
/// If type is ACTIVE_EXPIRE_CYCLE_SLOW, that normal expire cycle is
/// executed, where the time limit is a percentage of the REDIS_HZ period
/// as specified by the ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC define.
pub fn activeExpireCycle(@"type": i32) void {
    _ = @"type";
    // TODO:
}

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Object = @import("Object.zig");
const server = &Server.instance;
const assert = std.debug.assert;
const networking = @import("networking.zig");
