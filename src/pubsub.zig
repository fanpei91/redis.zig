/// SUBSCRIBE channel [channel ...]
pub fn subscribeCommand(cli: *Client) void {
    const argv = cli.argv.?;
    for (argv[1..cli.argc]) |channel| {
        _ = subscribeChannel(cli, channel);
    }
    cli.flags |= Server.CLIENT_PUBSUB;
}

/// PSUBSCRIBE pattern [pattern ...]
pub fn psubscribeCommand(cli: *Client) void {
    const argv = cli.argv.?;
    for (argv[1..cli.argc]) |channel| {
        _ = subscribePattern(cli, channel);
    }
    cli.flags |= Server.CLIENT_PUBSUB;
}

/// UNSUBSCRIBE [channel [channel ...]]
pub fn unsubscribeCommand(cli: *Client) void {
    const argv = cli.argv.?;
    if (cli.argc == 1) {
        _ = unsubscribeAllChannels(cli, true);
    } else {
        for (argv[1..cli.argc]) |channel| {
            _ = unsubscribeChannel(cli, channel, true);
        }
    }
    if (clientSubscriptionsCount(cli) == 0) {
        cli.flags &= ~@as(i32, Server.CLIENT_PUBSUB);
    }
}

/// PUNSUBSCRIBE [pattern [pattern ...]]
pub fn punsubscribeCommand(cli: *Client) void {
    const argv = cli.argv.?;
    if (cli.argc == 1) {
        _ = unsubscribeAllPatterns(cli, true);
    } else {
        for (argv[1..cli.argc]) |pattern| {
            _ = unsubscribePattern(cli, pattern, true);
        }
    }
    if (clientSubscriptionsCount(cli) == 0) {
        cli.flags &= ~@as(i32, Server.CLIENT_PUBSUB);
    }
}

/// PUBSUB CHANNELS [pattern]
///
/// PUBSUB NUMPAT
///
/// PUBSUB NUMSUB [channel [channel ...]]
pub fn pubsubCommand(cli: *Client) void {
    const help: []const []const u8 = &.{
        "CHANNELS [<pattern>] -- Return the currently active channels" ++
            " matching a pattern (default: all).",
        "NUMPAT -- Return number of subscriptions to patterns.",
        "NUMSUB [channel-1 .. channel-N] -- Returns the number of subscribers " ++
            "for the specified channels (excluding patterns, default: none).",
        "HELP -- Prints this help.",
    };

    const argv = cli.argv.?;
    const opt = sds.castBytes(argv[1].v.ptr);
    if (cli.argc == 2 and eqlCase(opt, "help")) {
        cli.addReplyHelp(help);
    } else if (eqlCase(opt, "channels") and (cli.argc == 2 or cli.argc == 3)) {
        const pat: ?sds.String = if (cli.argc == 2)
            null
        else
            sds.cast(argv[2].v.ptr);
        var di = server.pubsub_channels.iterator(false);
        defer di.release();
        const replylen = cli.addDeferredMultiBulkLength();
        var mblen: i64 = 0;
        while (di.next()) |de| {
            const cobj = de.key;
            const channel = sds.asSentinelBytes(sds.cast(cobj.v.ptr));
            if (pat == null or util.stringmatch(
                sds.asSentinelBytes(pat.?),
                channel,
                false,
            )) {
                cli.addReplyBulk(cobj);
                mblen += 1;
            }
        }
        cli.setDeferredMultiBulkLength(replylen, mblen);
    } else if (eqlCase(opt, "numsub") and cli.argc >= 2) {
        cli.addReplyMultiBulkLen(@intCast((cli.argc - 2) * 2));
        for (argv[2..cli.argc]) |channel| {
            const numclients = blk: {
                if (server.pubsub_channels.find(channel)) |l| {
                    break :blk l.val.len;
                }
                break :blk 0;
            };
            cli.addReplyBulk(channel);
            cli.addReplyLongLong(@intCast(numclients));
        }
    } else if (eqlCase(opt, "numpat") and cli.argc == 2) {
        cli.addReplyLongLong(@intCast(server.pubsub_patterns.len));
    } else {
        cli.addReplySubcommandSyntaxError();
    }
}

/// PUBLISH channel message
pub fn publishCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const receivers = publishMessage(argv[1], argv[2]);
    cli.addReplyLongLong(@intCast(receivers));
}

/// Publish a message
pub fn publishMessage(channel: *Object, message: *Object) usize {
    var receivers: usize = 0;

    // Send to clients listening for that channel
    if (server.pubsub_channels.find(channel)) |de| {
        const clients = de.val;
        var li = clients.iterator(.forward);
        while (li.next()) |ln| {
            const cli = ln.value;
            cli.addReply(Server.shared.mbulkhdr[3]);
            cli.addReply(Server.shared.messagebulk);
            cli.addReplyBulk(channel);
            cli.addReplyBulk(message);
            receivers += 1;
        }
    }

    // Send to clients listening to matching channels
    if (server.pubsub_patterns.len > 0) {
        var li = server.pubsub_patterns.iterator(.forward);
        const ch = channel.getDecoded();
        defer ch.decrRefCount();
        while (li.next()) |ln| {
            const pat = ln.value;
            const cli = pat.client;
            if (util.stringmatch(
                sds.asSentinelBytes(sds.cast(pat.pattern.v.ptr)),
                sds.asSentinelBytes(sds.cast(ch.v.ptr)),
                false,
            )) {
                cli.addReply(Server.shared.mbulkhdr[4]);
                cli.addReply(Server.shared.pmessagebulk);
                cli.addReplyBulk(pat.pattern);
                cli.addReplyBulk(ch);
                cli.addReplyBulk(message);
                receivers += 1;
            }
        }
    }

    return receivers;
}

/// Unsubscribe from all the channels. Return the number of channels the
/// client was subscribed to.
pub fn unsubscribeAllChannels(cli: *Client, notify: bool) usize {
    var count: usize = 0;

    var di = cli.pubsub_channels.iterator(true);
    defer di.release();
    while (di.next()) |de| {
        const channel = de.key;
        count += @intFromBool(unsubscribeChannel(cli, channel, notify));
    }

    // We were subscribed to nothing? Still reply to the client.
    if (notify and count == 0) {
        cli.addReply(Server.shared.mbulkhdr[3]);
        cli.addReply(Server.shared.unsubscribebulk);
        cli.addReply(Server.shared.nullbulk);
        cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    }

    return count;
}

/// Unsubscribe from all the patterns. Return the number of patterns the
/// client was subscribed from.
pub fn unsubscribeAllPatterns(cli: *Client, notify: bool) usize {
    var count: usize = 0;
    var li = cli.pubsub_patterns.iterator(.forward);
    while (li.next()) |ln| {
        const pattern = ln.value;
        count += @intFromBool(unsubscribePattern(cli, pattern, notify));
    }
    if (notify and count == 0) {
        cli.addReply(Server.shared.mbulkhdr[3]);
        cli.addReply(Server.shared.punsubscribebulk);
        cli.addReply(Server.shared.nullbulk);
        cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    }
    return count;
}

/// Unsubscribe a client from a channel. Returns TRUE if the operation succeeded,
/// or FALSE if the client was not subscribed to the specified channel.
fn unsubscribeChannel(cli: *Client, channel: *Object, notify: bool) bool {
    // channel may be just a pointer to the same object
    // we have in the hash tables. Protect it...
    _ = channel.incrRefCount();
    defer channel.decrRefCount();

    var retval = false;
    if (cli.pubsub_channels.delete(channel)) {
        retval = true;
        const de = server.pubsub_channels.find(channel).?;
        const clients = de.val;
        const ln = clients.search(cli).?;
        clients.removeNode(ln);
        if (clients.len == 0) {
            // Free the list and associated hash entry at all if this was
            // the latest client, so that it will be possible to abuse
            // Redis PUBSUB creating millions of channels
            _ = server.pubsub_channels.delete(channel);
        }
    }
    if (notify) {
        cli.addReply(Server.shared.mbulkhdr[3]);
        cli.addReply(Server.shared.unsubscribebulk);
        cli.addReplyBulk(channel);
        cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    }
    return retval;
}

/// Unsubscribe a client from a channel. Returns TRUE if the operation succeeded,
/// or FALSE if the client was not subscribed to the specified channel.
fn unsubscribePattern(cli: *Client, pattern: *Object, notify: bool) bool {
    // Protect the object. May be the same we remove
    _ = pattern.incrRefCount();
    defer pattern.decrRefCount();

    var retval = false;
    if (cli.pubsub_patterns.search(pattern)) |ln| {
        retval = true;
        cli.pubsub_patterns.removeNode(ln);
        var pat: Pattern = .{
            .client = cli,
            .pattern = pattern,
        };
        const node = server.pubsub_patterns.search(&pat).?;
        server.pubsub_patterns.removeNode(node);
    }
    if (notify) {
        cli.addReply(Server.shared.mbulkhdr[3]);
        cli.addReply(Server.shared.punsubscribebulk);
        cli.addReplyBulk(pattern);
        cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    }
    return retval;
}

/// Subscribe a client to a channel. Returns TRUE if the operation succeeded,
/// or FALSE if the client was already subscribed to that channel.
fn subscribeChannel(cli: *Client, channel: *Object) bool {
    var retval: bool = false;

    if (cli.pubsub_channels.add(channel, {})) {
        retval = true;
        var clients: *Server.ClientList = undefined;
        const de = server.pubsub_channels.find(channel);
        if (de == null) {
            clients = Server.ClientList.create(&.{});
            assert(server.pubsub_channels.add(channel, clients));
        } else {
            clients = de.?.val;
        }
        clients.append(cli);
    }

    cli.addReply(Server.shared.mbulkhdr[3]);
    cli.addReply(Server.shared.subscribebulk);
    cli.addReplyBulk(channel);
    cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    return retval;
}

/// Subscribe a client to a pattern. Returns TRUE if the operation succeeded,
/// or FALSE if the client was already subscribed to that pattern.
fn subscribePattern(cli: *Client, pattern: *Object) bool {
    var retval = false;
    if (cli.pubsub_patterns.search(pattern) == null) {
        retval = true;
        cli.pubsub_patterns.append(pattern);
        const pat = Pattern.create(cli, pattern);
        server.pubsub_patterns.append(pat);
    }

    // Notify the client
    cli.addReply(Server.shared.mbulkhdr[3]);
    cli.addReply(Server.shared.psubscribebulk);
    cli.addReplyBulk(pattern);
    cli.addReplyLongLong(@intCast(clientSubscriptionsCount(cli)));
    return retval;
}

/// Return the number of channels + patterns a client is subscribed to.
fn clientSubscriptionsCount(cli: *Client) u64 {
    return cli.pubsub_channels.size() + cli.pubsub_patterns.len;
}

pub const Pattern = struct {
    client: *Client,
    pattern: *Object,

    pub fn create(client: *Client, pattern: *Object) *Pattern {
        const pat = allocator.create(Pattern);
        pat.client = client;
        pat.pattern = pattern;
        _ = pat.pattern.incrRefCount();
        return pat;
    }

    pub fn eql(self: *Pattern, other: *Pattern) bool {
        return self.client == other.client and
            self.pattern.equalStrings(other.pattern);
    }

    pub fn destroy(self: *Pattern) void {
        self.pattern.decrRefCount();
        allocator.destroy(self);
    }
};

const Client = @import("networking.zig").Client;
const Object = @import("Object.zig");
const Server = @import("Server.zig");
const allocator = @import("allocator.zig");
const server = &Server.instance;
const std = @import("std");
const assert = std.debug.assert;
const util = @import("util.zig");
const sds = @import("sds.zig");
const eqlCase = std.ascii.eqlIgnoreCase;
