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
    stream_ids: ?[]Stream.Id,
) void {
    cli.bpop.timeout = timeout;
    cli.bpop.target = target;
    if (target) |t| _ = t.incrRefCount();

    for (keys, 0..) |key, i| {
        // Allocate our BlockInfo structure, associated to each key the client
        // is blocked for.
        const bki = allocator.create(BlockInfo);
        if (btype == Server.BLOCKED_STREAM) {
            bki.stream_id = stream_ids.?[i];
        }

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
            const added = cli.db.blocking_keys.add(key, clients);
            assert(added);
        }
        clients.append(cli);
        bki.listNode = clients.last.?;
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
    const rl = ReadyList.create(db, key);
    server.ready_keys.append(rl);

    // We also add the key in the db->ready_keys dictionary in order
    // to avoid adding it multiple times into a list with a simple O(1)
    // check.
    assert(db.ready_keys.add(key, {}));
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
        const ln = server.ready_keys.first.?;
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

        const coll = rl.db.lookupKeyWrite(rl.key) orelse {
            @branchHint(.unlikely);
            continue;
        };

        // Serve clients blocked on list key.
        if (coll.type == .list) {
            const de = rl.db.blocking_keys.find(rl.key) orelse {
                continue;
            };
            const clients: *ClientList = de.val;
            var numclients = clients.len;
            while (numclients > 0) : (numclients -= 1) {
                const clientnode = clients.first.?;
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
                const value = List.pop(coll, where).?;
                defer value.decrRefCount();

                // Protect receiver.bpop.target, that will be
                // freed by the next unblockClient()
                // call.
                if (dstkey) |dst| _ = dst.incrRefCount();
                defer if (dstkey) |dst| dst.decrRefCount();

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
                    @branchHint(.unlikely);
                    List.push(coll, value, where);
                }
            }
            if (List.length(coll) == 0) {
                assert(rl.db.delete(rl.key));
            }
        }
        // Serve clients blocked on sorted set key.
        else if (coll.type == .zset) {
            const de = rl.db.blocking_keys.find(rl.key) orelse {
                continue;
            };
            const clients: *ClientList = de.val;
            var numclients = clients.len;
            var zcard = Zset.length(coll);
            while (numclients > 0 and zcard > 0) : (numclients -= 1) {
                const clientnode = clients.first.?;
                const receiver = clientnode.value;
                if (receiver.btype != Server.BLOCKED_ZSET) {
                    // Put at the tail, so that at the next call
                    // we'll not run into it again.
                    clients.rotateHeadToTail();
                    continue;
                }
                const where = blk: {
                    if (receiver.lastcmd) |cmd| {
                        if (cmd.proc == zset.bzpopminCommand) {
                            break :blk zset.Where.min;
                        }
                    }
                    break :blk zset.Where.max;
                };
                unblockClient(receiver);
                zset.zpop(receiver, &.{rl.key}, where, true, null);
                zcard -= 1;
            }
        }
        // Serve clients blocked on stream key.
        else if (coll.type == .stream) {
            // We need to provide the new data arrived on the stream
            // to all the clients that are waiting for an offset smaller
            // than the current top item.
            const de = rl.db.blocking_keys.find(rl.key) orelse {
                continue;
            };
            const stream: *Stream = .cast(coll.v.ptr);
            const clients: *ClientList = de.val;
            var li = clients.iterator(.forward);
            while (li.next()) |node| {
                const receiver = node.value;
                if (receiver.btype != Server.BLOCKED_STREAM) {
                    continue;
                }
                const bki = receiver.bpop.keys.fetchValue(rl.key).?;
                const gt = &bki.stream_id.?;

                // If we blocked in the context of a consumer
                // group, we need to resolve the group and update the
                // last ID the client is blocked for: this is needed
                // because serving other clients in the same consumer
                // group will alter the "last ID" of the consumer
                // group, and clients blocked in a consumer group are
                // always blocked for the ">" ID: we need to deliver
                // only new messages and avoid unblocking the client
                // otherwise.
                var group: ?*Stream.CG = null;
                if (receiver.bpop.xread_group) |gpo| {
                    group = stream.lookupCG(sds.cast(gpo.v.ptr));
                    // If the group was not found, send an error
                    // to the consumer.
                    if (group == null) {
                        receiver.addReplyErr("-NOGROUP the consumer group this client " ++
                            "was blocked on no longer exists");
                        unblockClient(receiver);
                        continue;
                    } else {
                        gt.* = group.?.last_id;
                    }
                }
                if (stream.last_id.compare(gt) == .gt) {
                    var start = gt.*;
                    start.incr();

                    // Lookup the consumer for the group, if any.
                    var consumer: ?*Stream.Consumer = null;
                    var noack = false;
                    if (group) |g| {
                        consumer = g.lookupConsumer(
                            sds.cast(receiver.bpop.xread_consumer.?.v.ptr),
                            Stream.SLC_NONE,
                        );
                        noack = receiver.bpop.xread_group_noack;
                    }

                    // Emit the two elements sub-array consisting of
                    // the name of the stream and the data we
                    // extracted from it. Wrapped in a single-item
                    // array, since we have just one key.
                    receiver.addReplyMultiBulkLen(1);
                    receiver.addReplyMultiBulkLen(2);
                    receiver.addReplyBulk(rl.key);
                    const spi: ?*Stream.PropInfo = null; // TODO: PropInfo
                    var flags: i32 = 0;
                    if (noack) flags |= Stream.RWR_NOACK;
                    _ = stream.replyWithRange(
                        receiver,
                        &start,
                        null,
                        receiver.bpop.xread_count,
                        false,
                        group,
                        consumer,
                        flags,
                        spi,
                    );
                    // Note that after we unblock the client, 'gt'
                    // and other receiver->bpop stuff are no longer
                    // valid, so we must do the setup above before
                    // this call.
                    unblockClient(receiver);
                }
            }
        }
    }
}

pub fn unblockClient(cli: *Client) void {
    // TODO: BLOCKED_WAIT, BLOCKED_MODULE
    if (cli.btype == Server.BLOCKED_LIST or
        cli.btype == Server.BLOCKED_ZSET or
        cli.btype == Server.BLOCKED_STREAM)
    {
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
    assert(cli.bpop.keys.size() != 0);

    var di = cli.bpop.keys.iterator(false);
    while (di.next()) |de| {
        const key: *Object = de.key;
        const bki: *BlockInfo = de.val;
        const clients = cli.db.blocking_keys.fetchValue(key).?;
        clients.removeNode(bki.listNode);
        if (clients.len == 0) {
            _ = cli.db.blocking_keys.delete(key);
        }
    }
    di.release();

    // Cleanup the client structure
    cli.bpop.keys.empty(null);
    if (cli.bpop.target) |target| {
        target.decrRefCount();
        cli.bpop.target = null;
    }
    if (cli.bpop.xread_group != null) {
        cli.bpop.xread_group.?.decrRefCount();
        cli.bpop.xread_consumer.?.decrRefCount();
        cli.bpop.xread_group = null;
        cli.bpop.xread_consumer = null;
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
    // TODO: BLOCKED_WAIT, BLOCKED_MODULE
    if (cli.btype == Server.BLOCKED_LIST or
        cli.btype == Server.BLOCKED_ZSET or
        cli.btype == Server.BLOCKED_STREAM)
    {
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
        const ln = clients.first.?;
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
    unit: u32,
) ?i64 {
    var llval = object.getLongLongOrReply(
        cli,
        "timeout is not an integer or out of range",
    ) orelse return null;
    if (llval < 0) {
        cli.addReplyErr("timeout is negative");
        return null;
    }
    if (llval > 0) {
        if (unit == Server.UNIT_SECONDS) {
            llval *= std.time.ms_per_s;
        }
        llval += std.time.milliTimestamp();
    }
    return llval;
}

/// This structure represents the blocked key information that we store
/// in the client structure. Each client blocked on keys, has a
/// client->bpop.keys hash table. The keys of the hash table are Redis
/// keys pointers to 'robj' structures. The value is this structure.
/// The structure has two goals: firstly we store the list node that this
/// client uses to be listed in the database "blocked clients for this key"
/// list, so we can later unblock in O(1) without a list scan.
/// Secondly for certain blocking types, we have additional info. Right now
/// the only use for additional info we have is when clients are blocked
/// on streams, as we have to remember the ID it blocked for.
pub const BlockInfo = struct {
    /// List node for db->blocking_keys[key] list.
    listNode: *ClientList.Node,

    /// Stream ID if we blocked in a stream.
    stream_id: ?Stream.Id,

    pub fn destroy(self: *BlockInfo) void {
        allocator.destroy(self);
    }
};

/// The following structure represents a node in the server.ready_keys list,
/// where we accumulate all the keys that had clients blocked with a blocking
/// operation such as B[LR]POP, but received new data in the context of the
/// last executed command.
///
/// After the execution of every command or script, we run this list to check
/// if as a result we should serve data to clients blocked, unblocking them.
/// Note that server.ready_keys will not have duplicates as there dictionary
/// also called ready_keys in every structure representing a Redis database,
/// where we make sure to remember if a given key was already added in the
/// server.ready_keys list.
pub const ReadyList = struct {
    db: *Database,
    key: *Object,

    pub fn create(db: *Database, key: *Object) *ReadyList {
        const rl = allocator.create(ReadyList);
        rl.db = db;
        rl.key = key;
        _ = rl.key.incrRefCount();
        return rl;
    }

    pub fn destroy(self: *ReadyList) void {
        self.key.decrRefCount();
        allocator.destroy(self);
    }
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
const assert = std.debug.assert;
const zset = @import("t_zset.zig");
const Zset = zset.Zset;
const Stream = @import("t_stream.zig").Stream;
