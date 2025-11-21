/// Set a client in blocking mode for the specified key (list, zset or stream),
/// with the specified timeout. The 'type' argument is BLOCKED_LIST,
/// BLOCKED_ZSET or BLOCKED_STREAM depending on the kind of operation we are
/// waiting for an empty key in order to awake the client. The client is blocked
/// for all the keys as in the 'keys' argument. When we block for stream keys,
/// we also provide an array of streamID structures: clients will be unblocked
/// only when items with an ID greater or equal to the specified one is appended
/// to the stream.
pub fn blockForKeys(
    cli: *Client,
    btype: i32,
    keys: []*Object,
    timeout: i64,
    target: ?*Object,
) void {
    cli.bpop.timeout = timeout;
    cli.bpop.target = target;
    if (target) |t| t.incrRefCount();

    for (keys) |key| {
        const bki = allocator.create(BlockInfo);
        // If the key already exists in the dictionary ignore it.
        if (!cli.bpop.keys.add(key, bki)) {
            allocator.destroy(bki);
            continue;
        }

        // And in the other "side", to map keys -> clients
        var clients: *ClientList = undefined;
        const entry = cli.db.blocking_keys.find(key);
        if (entry) |e| {
            clients = e.val;
        } else {
            clients = ClientList.create(&.{});
            _ = cli.db.blocking_keys.add(key, clients) or unreachable;
        }
        clients.append(cli);
        bki.listNode = clients.last orelse unreachable;
    }

    blockClient(cli, btype);
}

pub fn blockClient(cli: *Client, btype: i32) void {
    cli.flags |= Server.CLIENT_BLOCKED;
    cli.btype = btype;
}

/// If the specified key has clients blocked waiting for list pushes, this
/// function will put the key reference into the server.ready_keys list.
/// Note that db->ready_keys is a hash table that allows us to avoid putting
/// the same key again and again in the list in case of multiple pushes
/// made by a script or in the context of MULTI/EXEC.
///
/// The list will be finally processed by handleClientsBlockedOnLists()
pub fn signalKeyAsReady(db: *Database, key: *Object) void {
    // No clients blocking for this key? No need to queue it.
    if (db.blocking_keys.find(key) == null) return;

    // Key was already signaled? No need to queue it again.
    if (db.ready_keys.find(key) != null) return;

    // Ok, we need to queue this key into server.ready_keys.
    const rl = allocator.create(ReadyList);
    rl.db = db;
    rl.key = key;
    server.ready_keys.append(rl);

    // We also add the key in the db->ready_keys dictionary in order
    // to avoid adding it multiple times into a list with a simple O(1)
    // check.
    _ = db.ready_keys.add(key, {}) or unreachable;
}

/// This function should be called by Redis every time a single command,
/// a MULTI/EXEC block, or a Lua script, terminated its execution after
/// being called by a client. It handles serving clients blocked in
/// lists, streams, and sorted sets, via a blocking commands.
///
/// All the keys with at least one client blocked that received at least
/// one new element via some write operation are accumulated into
/// the server.ready_keys list. This function will run the list and will
/// serve clients accordingly. Note that the function will iterate again and
/// again as a result of serving BRPOPLPUSH we can have new blocking clients
/// to serve because of the PUSH side of BRPOPLPUSH.
///
/// This function is normally "fair", that is, it will server clients
/// using a FIFO behavior. However this fairness is violated in certain
/// edge cases, that is, when we have clients blocked at the same time
/// in a sorted set and in a list, for the same key (a very odd thing to
/// do client side, indeed!). Because mismatching clients (blocking for
/// a different type compared to the current key type) are moved in the
/// other side of the linked list. However as long as the key starts to
/// be used only for a single type, like virtually any Redis application will
/// do, the function is already fair.
pub fn handleClientsBlockedOnKeys() void {
    while (server.ready_keys.len != 0) {
        const ln = server.ready_keys.first orelse unreachable;
        const rl = ln.value;
        defer server.ready_keys.removeNode(ln);

        // First of all remove this key from db.ready_keys so that
        // we can safely call signalKeyAsReady() against this key.
        _ = rl.db.ready_keys.delete(rl.key);

        // Even if we are not inside call(), increment the call depth
        // in order to make sure that keys are expired against a fixed
        // reference time, and not against the wallclock time. This
        // way we can lookup an object multiple times (BRPOPLPUSH does
        // that) without the risk of it being freed in the second
        // lookup, invalidating the first one.
        server.fixed_time_expire +%= 1;
        server.updateCachedTime();
        defer server.fixed_time_expire -%= 1;

        const o = rl.db.lookupKeyWrite(rl.key) orelse {
            @branchHint(.unlikely);
            continue;
        };
        if (o.type == .list) {
            if (rl.db.blocking_keys.find(rl.key)) |de| {
                const clients: *ClientList = de.val;
                var numclients = clients.len;
                while (numclients > 0) : (numclients -= 1) {
                    const clientnode = clients.first orelse unreachable;
                    const receiver = clientnode.value;
                    if (receiver.btype != Server.BLOCKED_LIST) {
                        // Put at the tail, so that at the next call
                        // we'll not run into it again.
                        clients.rotateHeadToTail();
                        continue;
                    }
                    const dstkey = receiver.bpop.target;
                    const where = blk: {
                        if (receiver.lastcmd) |cmd| {
                            if (cmd.proc == list.blpopCommand) {
                                break :blk list.Where.head;
                            }
                        }
                        break :blk list.Where.tail;
                    };
                    const value = List.pop(o, where).?;
                    defer value.decrRefCount();

                    unblockClient(receiver);

                    if (!list.serveClientBlockedOnList(
                        receiver,
                        rl.key,
                        dstkey,
                        rl.db,
                        value,
                        where,
                    )) {
                        // If we failed serving the client we need
                        // to also undo the POP operation.
                        List.push(o, value, where);
                    }
                }
            }
            if (List.length(o) == 0) {
                _ = rl.db.delete(rl.key) or unreachable;
            }
        }
    }
}

pub fn unblockClient(cli: *Client) void {
    if (cli.btype == Server.BLOCKED_LIST) {
        unblockClientWaitingData(cli);
    } else {
        @panic("Unknown btype in unblockClient().");
    }

    // Clear the flags, and put the client in the unblocked list so that
    // we'll process new commands in its query buffer ASAP.
    cli.flags &= ~@as(i32, Server.CLIENT_BLOCKED);
    cli.btype = Server.BLOCKED_NONE;
    queueClientForReprocessing(cli);
}

/// Unblock a client that's waiting in a blocking operation such as BLPOP.
/// You should never call this function directly, but unblockClient() instead.
fn unblockClientWaitingData(cli: *Client) void {
    std.debug.assert(cli.bpop.keys.size() != 0);

    var di = cli.bpop.keys.iterator(false);
    while (di.next()) |de| {
        const key: *Object = de.key;
        const bki: *BlockInfo = de.val;
        const clients = cli.db.blocking_keys.fetchValue(key) orelse {
            unreachable;
        };
        clients.removeNode(bki.listNode);
        if (clients.len == 0) {
            _ = cli.db.blocking_keys.delete(key) or unreachable;
        }
    }
    di.release();
    cli.bpop.keys.empty(null);

    if (cli.bpop.target) |target| {
        target.decrRefCount();
        cli.bpop.target = null;
    }
}

/// This function will schedule the client for reprocessing at a safe time.
///
/// This is useful when a client was blocked for some reason (blocking opeation,
/// CLIENT PAUSE, or whatever), because it may end with some accumulated query
/// buffer that needs to be processed ASAP:
///
/// 1. When a client is blocked, its readable handler is still active.
/// 2. However in this case it only gets data into the query buffer, but the
///    query is not parsed or executed once there is enough to proceed as
///    usually (because the client is blocked... so we can't execute commands).
/// 3. When the client is unblocked, without this function, the client would
///    have to write some query in order for the readable handler to finally
///    call cli.processInputBuffer() on it.
/// 4. With this function instead we can put the client in a queue that will
///    process it for queries ready to be executed at a safe time.
pub fn queueClientForReprocessing(cli: *Client) void {
    if (cli.flags & Server.CLIENT_UNBLOCKED == 0) {
        cli.flags |= Server.CLIENT_UNBLOCKED;
        server.unblocked_clients.append(cli);
    }
}

/// This function gets called when a blocked client timed out in order to
/// send it a reply of some kind. After this function is called,
/// unblockClient() will be called with the same client as argument.
pub fn replyToBlockedClientTimedOut(cli: *Client) void {
    if (cli.btype == Server.BLOCKED_LIST) {
        cli.addReply(Server.shared.nullmultibulk);
    } else {
        @panic("Unknown btype in replyToBlockedClientTimedOut().");
    }
}

/// This function is called in the beforeSleep() function of the event loop
/// in order to process the pending input buffer of clients that were
/// unblocked after a blocking operation.
pub fn processUnblockedClients() void {
    const clients = server.unblocked_clients;
    while (clients.len > 0) {
        const ln = clients.first orelse unreachable;
        const cli = ln.value;
        clients.removeNode(ln);
        cli.flags &= ~@as(i32, Server.CLIENT_UNBLOCKED);

        // Process remaining data in the input buffer, unless the client
        // is blocked again. Actually processInputBuffer() checks that the
        // client is not blocked before to proceed, but things may change and
        // the code is conceptually more correct this way.
        if (cli.flags & Server.CLIENT_BLOCKED == 0) {
            if (sds.getLen(cli.querybuf) > 0) {
                cli.processInputBufferAndReplicate();
            }
        }
    }
}

/// Get a timeout value from an object and store it into 'timeout'.
/// The final timeout is always stored as milliseconds as a time where the
/// timeout will expire, however the parsing is performed according to
/// the 'unit' that can be seconds or milliseconds.
///
/// Note that if the timeout is zero (usually from the point of view of
/// commands API this means no timeout) the value stored into 'timeout'
/// is zero.
pub fn getTimeoutFromObjectOrReply(
    cli: *Client,
    object: *Object,
    timeout: *i64,
    unit: u32,
) bool {
    var llval: i64 = undefined;
    if (!object.getLongLongFromObjectOrReply(
        cli,
        &llval,
        "timeout is not an integer or out of range",
    )) {
        return false;
    }
    if (llval < 0) {
        cli.addReplyErr("timeout is negative");
        return false;
    }
    if (llval > 0) {
        if (unit == Server.UNIT_SECONDS) {
            llval *= std.time.ms_per_s;
        }
        llval += std.time.milliTimestamp();
    }
    timeout.* = llval;
    return true;
}

pub const BlockInfo = struct {
    listNode: *ClientList.Node,
};

pub const ReadyList = struct {
    db: *Database,
    key: *Object,
};

const std = @import("std");
const Database = @import("db.zig").Database;
const Object = @import("Object.zig");
const networking = @import("networking.zig");
const Client = networking.Client;
const Server = @import("Server.zig");
const ClientList = Server.ClientList;
const allocator = @import("allocator.zig");
const server = &Server.instance;
const list = @import("t_list.zig");
const List = list.List;
const sds = @import("sds.zig");
const log = std.log.scoped(.blocked);
