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
    const argv = cli.argv orelse unreachable;
    const key = argv[1];
    if (cli.db.lookupKeyWrite(key) != null) {
        if (cli.db.removeExpire(key)) {
            cli.addReply(Server.shared.cone);
        } else {
            cli.addReply(Server.shared.czero);
        }
    } else {
        cli.addReply(Server.shared.czero);
    }
}

/// TOUCH key [key ...]
pub fn touchCommand(cli: *Client) void {
    const argv = cli.argv orelse unreachable;
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
    const argv = cli.argv orelse unreachable;

    // unix time in milliseconds when the key will expire.
    var when: i64 = undefined;
    if (!argv[2].getLongLongOrReply(cli, &when, null)) {
        return;
    }

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
        _ = cli.db.delete(key) or unreachable;
        cli.addReply(Server.shared.cone);
        return;
    }
    cli.db.setExpire(cli, key, when);
    cli.addReply(Server.shared.cone);
}

fn checkAlreadyExpired(when: i64) bool {
    return std.time.milliTimestamp() >= when;
}

/// Implements TTL and PTTL
fn ttl(cli: *Client, output_ms: bool) void {
    const argv = cli.argv orelse unreachable;
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

const std = @import("std");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const Object = @import("Object.zig");
const server = &Server.instance;
