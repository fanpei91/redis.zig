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
    const key = cli.argv.?[1];
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

/// Implements TTL and PTTL
fn ttl(cli: *Client, output_ms: bool) void {
    const key = cli.argv.?[1];

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
const sds = @import("sds.zig");
