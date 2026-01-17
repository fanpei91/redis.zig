/// SADD key member [member ...]
pub fn saddCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const sobj = cli.db.lookupKeyWrite(key) orelse blk: {
        const o = Set.create(sds.cast(argv[2].v.ptr));
        defer o.decrRefCount();
        cli.db.add(key, o);
        break :blk o;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    var added: i64 = 0;
    for (argv[2..]) |member| {
        if (Set.add(sobj, sds.cast(member.v.ptr))) {
            added += 1;
        }
    }
    if (added > 0) {
        cli.db.signalModifiedKey(key);
    }
    server.dirty +%= added;
    cli.addReplyLongLong(added);
}

/// SREM key member [member ...]
pub fn sremCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    var deleted: i64 = 0;
    for (argv[2..]) |member| {
        if (Set.remove(sobj, sds.cast(member.v.ptr))) {
            deleted += 1;
            if (Set.size(sobj) == 0) {
                _ = cli.db.delete(key);
                break;
            }
        }
    }
    if (deleted > 0) {
        cli.db.signalModifiedKey(key);
        server.dirty +%= deleted;
    }
    cli.addReplyLongLong(deleted);
}

/// SISMEMBER key member
pub fn sismemberCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    if (Set.isMember(sobj, sds.cast(argv[2].v.ptr))) {
        cli.addReply(Server.shared.cone);
        return;
    }
    cli.addReply(Server.shared.czero);
}

/// SMOVE source destination member
pub fn smoveCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const srckey = argv[1];
    const srcset = cli.db.lookupKeyWriteOrReply(
        cli,
        srckey,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (srcset.checkTypeOrReply(cli, .set)) {
        return;
    }

    const dstkey = argv[2];
    const dstset = cli.db.lookupKeyWrite(dstkey);
    if (dstset) |dst| if (dst.checkTypeOrReply(cli, .set)) {
        return;
    };

    // If srcset and dstset are equal, SMOVE is a no-op.
    const member = sds.cast(argv[3].v.ptr);
    if (srcset == dstset) {
        cli.addReply(
            if (Set.isMember(srcset, member))
                Server.shared.cone
            else
                Server.shared.czero,
        );
        return;
    }

    // If the element cannot be removed from the src set, return 0.
    if (!Set.remove(srcset, member)) {
        cli.addReply(Server.shared.czero);
        return;
    }

    // Remove the src set from the database when empty.
    if (Set.size(srcset) == 0) {
        const ok = cli.db.delete(srckey);
        assert(ok);
    }

    // Create the destination set when it doesn't exist.
    const dst = dstset orelse blk: {
        const o = Set.create(member);
        defer o.decrRefCount();
        cli.db.add(dstkey, o);
        break :blk o;
    };

    cli.db.signalModifiedKey(srckey);
    cli.db.signalModifiedKey(dstkey);
    server.dirty +%= 1;

    // An extra key has changed when ele was successfully added to dstset
    if (Set.add(dst, member)) {
        server.dirty +%= 1;
    }
    cli.addReply(Server.shared.cone);
}

/// SCARD key
pub fn scardCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    cli.addReplyLongLong(@intCast(Set.size(sobj)));
}

/// SRANDMEMBER key [count]
pub fn srandmemberCommand(cli: *Client) void {
    const argv = cli.argv.?;

    if (cli.argc == 3) {
        srandmemberWithCount(cli);
        return;
    }
    if (cli.argc > 3) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    switch (Set.randomElement(sobj)) {
        .num => |v| cli.addReplyBulkLongLong(v),
        .s => |v| cli.addReplyBulkString(sds.asBytes(v)),
    }
}

/// How many times bigger should be the set compared to the requested size
/// for us to don't use the "remove elements" strategy? Read later in the
/// implementation for more info.
const SRANDMEMBER_SUB_STRATEGY_MUL = 3;

fn srandmemberWithCount(cli: *Client) void {
    assert(cli.argc == 3);
    const argv = cli.argv.?;

    const l = argv[2].getLongLongOrReply(cli, null) orelse {
        return;
    };

    var count: u64 = 0;
    var uniq: bool = true;

    if (l >= 0) {
        count = @intCast(l);
    } else {
        // A negative count means: return the same elements multiple times
        // (i.e. don't remove the extracted element after every extraction).
        count = @intCast(-l);
        uniq = false;
    }

    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    // If count is zero, serve it ASAP to avoid special cases later.
    if (count == 0) {
        cli.addReply(Server.shared.emptymultibulk);
        return;
    }

    // CASE 1: The count was negative, so the extraction method is just:
    // "return N random elements" sampling the whole set every time.
    // This case is trivial and can be served without auxiliary data
    // structures.
    if (!uniq) {
        cli.addReplyMultiBulkLen(@intCast(count));
        while (count > 0) : (count -= 1) {
            switch (Set.randomElement(sobj)) {
                .num => |v| cli.addReplyBulkLongLong(v),
                .s => |v| cli.addReplyBulkString(sds.asBytes(v)),
            }
        }
        return;
    }

    // CASE 2:
    // The number of requested elements is greater than the number of
    // elements inside the set: simply return the whole set.
    var size = Set.size(sobj);
    if (count >= size) {
        cli.addReplyMultiBulkLen(@intCast(size));
        var it = Set.Iterator.create(sobj);
        defer it.release();
        while (it.next()) |value| switch (value) {
            .num => |v| cli.addReplyBulkLongLong(v),
            .s => |v| cli.addReplyBulkString(sds.asBytes(v)),
        };
        return;
    }

    // For CASE 3 and CASE 4 we need an auxiliary dictionary.
    const d: *dict.Dict(*Object, void) = .create(&.{
        .hash = Object.hash,
        .eql = Object.eql,
        .freeKey = Object.decrRefCount,
    });
    defer d.destroy();

    // CASE 3:
    // The number of elements inside the set is not greater than
    // SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
    // In this case we create a set from scratch with all the elements, and
    // subtract random elements to reach the requested number of elements.
    //
    // This is done because if the number of requsted elements is just
    // a bit less than the number of elements in the set, the natural approach
    // used into CASE 3 is highly inefficient.
    if (count * SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        var it = Set.Iterator.create(sobj);
        defer it.release();
        while (it.next()) |value| {
            const obj = switch (value) {
                .num => |v| Object.createStringFromLonglong(v),
                .s => |v| Object.createString(sds.asBytes(v)),
            };
            const ok = d.add(obj, {});
            assert(ok);
        }
        assert(d.size() == size);

        // Remove random elements to reach the right count.
        while (size > count) : (size -= 1) {
            const de = d.getRandom().?;
            const ok = d.delete(de.key);
            assert(ok);
        }
    }
    // CASE 4: We have a big set compared to the requested number of elements.
    // In this case we can simply get random elements from the set and add
    // to the temporary set, trying to eventually get enough unique elements
    // to reach the specified count.
    else {
        var added: u64 = 0;
        while (added < count) {
            const obj = switch (Set.randomElement(sobj)) {
                .num => |v| Object.createStringFromLonglong(v),
                .s => |v| Object.createString(sds.asBytes(v)),
            };
            if (d.add(obj, {})) {
                added += 1;
            } else {
                obj.decrRefCount();
            }
        }
    }

    // CASE 3 & 4: send the result to the user.
    cli.addReplyMultiBulkLen(@intCast(count));
    var it = d.iterator(false);
    defer it.release();
    while (it.next()) |entry| {
        cli.addReplyBulk(entry.key);
    }
}

/// SINTER key [key ...]
pub fn sinterCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sinter(cli, argv[1..cli.argc], null);
}

/// SINTERSTORE destination key [key ...]
pub fn sinterstoreCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sinter(cli, argv[2..cli.argc], argv[1]);
}

fn sinter(cli: *Client, keys: []*Object, dstkey: ?*Object) void {
    var sets = std.ArrayList(*Object).initCapacity(
        allocator.impl,
        keys.len,
    ) catch allocator.oom();
    defer sets.deinit(allocator.impl);

    for (keys) |key| {
        const sobj = if (dstkey != null)
            cli.db.lookupKeyWrite(key)
        else
            cli.db.lookupKeyRead(key);
        if (sobj == null) {
            if (dstkey) |dst| {
                if (cli.db.delete(dst)) {
                    cli.db.signalModifiedKey(dst);
                    server.dirty +%= 1;
                }
                cli.addReply(Server.shared.czero);
            } else {
                cli.addReply(Server.shared.emptymultibulk);
            }
            return;
        }
        if (sobj.?.checkTypeOrReply(cli, .set)) {
            return;
        }
        sets.append(allocator.impl, sobj.?) catch allocator.oom();
    }

    // Sort sets from the smallest to largest, this will improve our
    // algorithm's performance.
    std.mem.sort(*Object, sets.items, {}, sortSetsByCardinality);

    var replylen: ?*Client.ReplyBlock = null;
    var dstset: ?*Object = null;
    defer if (dstset) |obj| obj.decrRefCount();

    if (dstkey == null) {
        // The first thing we should output is the total number of elements...
        // since this is a multi-bulk write, but at this stage we don't know
        // the intersection set size, so we use a trick, append an placeholer
        // object to the output list and save the pointer to later modify it
        // with the right length.
        replylen = cli.addDeferredMultiBulkLength();
    } else {
        // If we have a target key where to store the resulting set
        // create this key with an empty set inside
        dstset = Object.createIntSet();
    }

    var cardinality: i64 = 0;

    var stack_impl = std.heap.stackFallback(
        128,
        allocator.impl,
    );
    const stack_allocator = stack_impl.get();
    // Iterate all the elements of the first (smallest) set, and test
    // the element against all the other sets, if at least one set does
    // not include the element it is discarded
    const first = sets.items[0];
    var it = Set.Iterator.create(first);
    defer it.release();
    next: while (it.next()) |value| {
        for (sets.items[1..]) |set| {
            if (set == first) continue :next;
            switch (value) {
                .num => |v| {
                    assert(first.encoding == .intset);
                    if (set.encoding == .intset) {
                        const is = IntSet.cast(set.v.ptr);
                        if (!is.find(v)) continue :next;
                    } else if (set.encoding == .ht) {
                        const member = sds.fromLonglong(stack_allocator, v);
                        defer sds.free(stack_allocator, member);
                        if (!Set.isMember(set, member)) continue :next;
                    }
                },
                .s => |v| {
                    assert(first.encoding == .ht);
                    if (!Set.isMember(set, v)) continue :next;
                },
            }
        }
        if (dstkey == null) {
            switch (value) {
                .num => |v| cli.addReplyLongLong(v),
                .s => |v| cli.addReplyBulkString(sds.asBytes(v)),
            }
            cardinality += 1;
        } else {
            switch (value) {
                .num => |v| {
                    const member = sds.fromLonglong(stack_allocator, v);
                    defer sds.free(stack_allocator, member);
                    const ok = Set.add(dstset.?, member);
                    assert(ok);
                },
                .s => |v| {
                    const ok = Set.add(dstset.?, v);
                    assert(ok);
                },
            }
        }
    }

    if (dstkey) |dst| {
        // Store the resulting set into the target, if the intersection
        // is not an empty set.
        _ = cli.db.delete(dst);
        const len = Set.size(dstset.?);
        if (len > 0) {
            cli.db.add(dst, dstset.?);
            cli.addReplyLongLong(@intCast(len));
        } else {
            cli.addReply(Server.shared.czero);
        }
        cli.db.signalModifiedKey(dst);
        server.dirty +%= 1;
    } else {
        cli.setDeferredMultiBulkLength(replylen.?, cardinality);
    }
}

fn sortSetsByCardinality(_: void, lhs: *Object, rhs: *Object) bool {
    return Set.size(lhs) < Set.size(rhs);
}

/// SUNION key [key ...]
pub fn sunionCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sunionDiff(cli, argv[1..cli.argc], null, Op.@"union");
}

/// SUNIONSTORE destination key [key ...]
pub fn sunionstoreCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sunionDiff(cli, argv[2..cli.argc], argv[1], Op.@"union");
}

/// SDIFF key [key ...]
pub fn sdiffCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sunionDiff(cli, argv[1..cli.argc], null, Op.diff);
}

/// SDIFFSTORE destination key [key ...]
pub fn sdiffstoreCommand(cli: *Client) void {
    const argv = cli.argv.?;
    sunionDiff(cli, argv[2..cli.argc], argv[1], Op.diff);
}

const Op = enum {
    @"union",
    diff,
};
fn sunionDiff(cli: *Client, keys: []*Object, dstkey: ?*Object, op: Op) void {
    var sets = std.ArrayList(?*Object).initCapacity(
        allocator.impl,
        keys.len,
    ) catch allocator.oom();
    defer sets.deinit(allocator.impl);

    for (keys) |key| {
        const sobj = if (dstkey != null)
            cli.db.lookupKeyWrite(key)
        else
            cli.db.lookupKeyRead(key);
        if (sobj) |obj| if (obj.checkTypeOrReply(cli, .set)) {
            return;
        };
        sets.append(allocator.impl, sobj) catch allocator.oom();
    }

    // Select what DIFF algorithm to use.
    //
    // Algorithm 1 is O(N*M) where N is the size of the element first set
    // and M the total number of sets.
    //
    // Algorithm 2 is O(N) where N is the total number of elements in all
    // the sets.
    //
    // We compute what is the best bet with the current input here.
    var diff_algo: u32 = 1;
    if (op == .diff) if (sets.items[0]) |first| {
        var algo_one_work: usize = 0;
        var algo_two_work: usize = 0;
        for (sets.items) |set| if (set) |s| {
            algo_one_work += Set.size(first);
            algo_two_work += Set.size(s);
        };
        // Algorithm 1 has better constant times and performs less operations
        // if there are elements in common. Give it some advantage.
        algo_one_work = @divFloor(algo_one_work, 2);
        diff_algo = if (algo_one_work <= algo_two_work) 1 else 2;

        if (diff_algo == 1 and sets.items.len > 1) {
            // With algorithm 1 it is better to order the sets to subtract
            // by decreasing size, so that we are more likely to find
            // duplicated elements ASAP.
            std.mem.sort(
                ?*Object,
                sets.items[1..],
                {},
                sortSetsByReversedCardinality,
            );
        }
    };

    // We need a temp set object to store our union. If the dstkey
    // is not NULL (that is, we are inside an SUNIONSTORE operation) then
    // this set object will be the resulting object to set into the target key
    const dstset = Object.createIntSet();
    defer dstset.decrRefCount();

    var stack_impl = std.heap.stackFallback(
        2048,
        allocator.impl,
    );
    const stack_allocator = stack_impl.get();

    if (op == .@"union") {
        // Union is trivial, just add every element of every set to the
        // temporary set.
        for (sets.items) |item| if (item) |set| {
            var it = Set.Iterator.create(set);
            while (it.nextObject(stack_allocator)) |value| {
                defer sds.free(stack_allocator, value);
                _ = Set.add(dstset, value);
            }
            it.release();
        };
    } else if (op == .diff and sets.items[0] != null and diff_algo == 1) {
        // DIFF Algorithm 1:
        //
        // We perform the diff by iterating all the elements of the first set,
        // and only adding it to the target set if the element does not exist
        // into all the other sets.
        //
        // This way we perform at max N*M operations, where N is the size of
        // the first set, and M the number of sets.
        const first = sets.items[0].?;
        var it = Set.Iterator.create(first);
        next: while (it.nextObject(stack_allocator)) |value| {
            defer sds.free(stack_allocator, value);
            for (sets.items[1..]) |item| if (item) |set| {
                if (set == first) continue :next;
                if (Set.isMember(set, value)) continue :next;
            };
            const ok = Set.add(dstset, value);
            assert(ok);
        }
        it.release();
    } else if (op == .diff and sets.items[0] != null and diff_algo == 2) {
        // DIFF Algorithm 2:
        //
        // Add all the elements of the first set to the auxiliary set.
        // Then remove all the elements of all the next sets from it.
        //
        // This is O(N) where N is the sum of all the elements in every
        // set.
        for (sets.items, 0..) |item, i| if (item) |set| {
            var it = Set.Iterator.create(set);
            while (it.nextObject(stack_allocator)) |value| {
                defer sds.free(stack_allocator, value);
                if (i == 0) {
                    const ok = Set.add(dstset, value);
                    assert(ok);
                } else {
                    _ = Set.remove(dstset, value);
                }
            }
            it.release();
            // Exit if result set is empty as any additional removal
            // of elements will have no effect.
            if (Set.size(dstset) == 0) break;
        };
    }

    // Output the content of the resulting set, if not in STORE mode
    if (dstkey == null) {
        cli.addReplyMultiBulkLen(@intCast(Set.size(dstset)));
        var it = Set.Iterator.create(dstset);
        while (it.nextObject(stack_allocator)) |value| {
            defer sds.free(stack_allocator, value);
            cli.addReplyBulkString(sds.asBytes(value));
        }
        it.release();
        return;
    }

    // If we have a target key where to store the resulting set
    // create this key with the result set inside
    _ = cli.db.delete(dstkey.?);
    const len = Set.size(dstset);
    if (len > 0) {
        cli.db.add(dstkey.?, dstset);
        cli.addReplyLongLong(@intCast(len));
    } else {
        cli.addReply(Server.shared.czero);
    }
    cli.db.signalModifiedKey(dstkey.?);
    server.dirty +%= 1;
}

fn sortSetsByReversedCardinality(_: void, lhs: ?*Object, rhs: ?*Object) bool {
    const l = if (lhs) |set| Set.size(set) else 0;
    const r = if (rhs) |set| Set.size(set) else 0;
    return l > r;
}

/// SPOP key [count]
pub fn spopCommand(cli: *Client) void {
    if (cli.argc == 3) {
        spopWithCount(cli);
        return;
    }
    if (cli.argc > 3) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    const argv = cli.argv.?;
    const key = argv[1];
    const sobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    var ele: *Object = undefined;
    defer ele.decrRefCount();

    switch (Set.randomElement(sobj)) {
        .num => |v| {
            assert(sobj.encoding == .intset);
            ele = Object.createStringFromLonglong(v);
            const is = IntSet.cast(sobj.v.ptr);
            const ret = is.remove(v);
            sobj.v = .{ .ptr = ret.set };
        },
        .s => |v| {
            assert(sobj.encoding == .ht);
            ele = Object.createString(sds.asBytes(v));
            _ = Set.remove(sobj, v);
        },
    }

    // Replicate/AOF this command as an SREM operation
    cli.rewriteCommandVector(&.{ Server.shared.srem, key, ele });

    // Add the element to the reply
    cli.addReplyBulk(ele);

    // Delete the set if it's empty
    if (Set.size(sobj) == 0) {
        assert(cli.db.delete(key));
    }

    // Set has been modified
    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;
}

/// How many times bigger should be the set compared to the remaining size
/// for us to use the "create new set" strategy? Read later in the
/// implementation for more info.
const SPOP_MOVE_STRATEGY_MUL = 5;

fn spopWithCount(cli: *Client) void {
    assert(cli.argc == 3);

    const argv = cli.argv.?;
    var count = argv[2].getLongLongOrReply(cli, null) orelse {
        return;
    };
    if (count < 0) {
        cli.addReply(Server.shared.outofrangeerr);
        return;
    }

    const key = argv[1];
    const sobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    if (count == 0) {
        cli.addReply(Server.shared.emptymultibulk);
        return;
    }

    const size = Set.size(sobj);
    server.dirty +%= count;

    // CASE 1:
    // The number of requested elements is greater than or equal to
    // the number of elements inside the set: simply return the whole set.
    if (count >= size) {
        cli.addReplyMultiBulkLen(@intCast(size));
        var it = Set.Iterator.create(sobj);
        while (it.next()) |value| switch (value) {
            .num => |v| cli.addReplyBulkLongLong(v),
            .s => |v| cli.addReplyBulkString(sds.asBytes(v)),
        };
        it.release();
        // Delete the set as it is now empty
        assert(cli.db.delete(key));
        // Propagate this command as an DEL operation
        cli.rewriteCommandVector(&.{ Server.shared.del, key });
        cli.db.signalModifiedKey(key);
        server.dirty +%= 1;
        return;
    }

    // Send the array length which is common to both the code paths.
    cli.addReplyMultiBulkLen(@intCast(count));

    // Case 2 and 3 require to replicate SPOP as a set of SREM commands.
    // Prepare our replication argument vector. Also send the array length
    // which is common to both the code paths.
    var propargv: [3]*Object = undefined;
    propargv[0] = Server.shared.srem;
    propargv[1] = key;

    // Elements left after SPOP.
    var remaining = size - @as(u64, @intCast(count));

    // If we are here, the number of requested elements is less than the
    // number of elements inside the set. Also we are sure that count < size.
    // Use two different strategies.
    //
    // CASE 2: The number of elements to return is small compared to the
    // set size. We can just extract random elements and return them to
    // the set.
    if (remaining * SPOP_MOVE_STRATEGY_MUL > count) {
        while (count > 0) : (count -= 1) {
            var objele: *Object = undefined;
            defer objele.decrRefCount();
            switch (Set.randomElement(sobj)) {
                .num => |v| {
                    assert(sobj.encoding == .intset);
                    cli.addReplyBulkLongLong(v);
                    const is = IntSet.cast(sobj.v.ptr);
                    objele = Object.createStringFromLonglong(v);
                    const ret = is.remove(v);
                    assert(ret.success);
                    sobj.v = .{ .ptr = ret.set };
                },
                .s => |v| {
                    assert(sobj.encoding == .ht);
                    cli.addReplyBulkString(sds.asBytes(v));
                    objele = Object.createString(sds.asBytes(v));
                    const ok = Set.remove(sobj, v);
                    assert(ok);
                },
            }

            // Replicate/AOF this command as an SREM operation
            propargv[2] = objele;
            server.alsoPropagate(
                server.sremCommand,
                cli.db.id,
                &propargv,
                propargv.len,
                Server.PROPAGATE_AOF | Server.PROPAGATE_REPL,
            );
        }
    } else {
        // CASE 3: The number of elements to return is very big, approaching
        // the size of the set itself. After some time extracting random elements
        // from such a set becomes computationally expensive, so we use
        // a different strategy, we extract random elements that we don't
        // want to return (the elements that will remain part of the set),
        // creating a new set as we do this (that will be stored as the original
        // set). Then we return the elements left in the original set and
        // release it.
        const newset: *Object = blk: {
            if (sobj.encoding == .intset) {
                break :blk Object.createIntSet();
            }
            break :blk Object.createSet();
        };
        defer newset.decrRefCount();

        var stack_impl = std.heap.stackFallback(
            2048,
            allocator.impl,
        );
        const stack_allocator = stack_impl.get();

        // Create a new set with just the remaining elements.
        while (remaining > 0) : (remaining -= 1) {
            const ele: sds.String = switch (Set.randomElement(sobj)) {
                .num => |v| sds.fromLonglong(stack_allocator, v),
                .s => |v| sds.dupe(stack_allocator, v),
            };
            defer sds.free(stack_allocator, ele);
            _ = Set.add(newset, ele);
            _ = Set.remove(sobj, ele);
        }

        // Transfer the old set to the client.
        var it = Set.Iterator.create(sobj);
        while (it.next()) |value| {
            var objele: *Object = undefined;
            defer objele.decrRefCount();
            switch (value) {
                .num => |v| {
                    objele = Object.createStringFromLonglong(v);
                    cli.addReplyBulkLongLong(v);
                },
                .s => |v| {
                    objele = Object.createString(sds.asBytes(v));
                    cli.addReplyBulkString(sds.asBytes(v));
                },
            }
            // Replicate/AOF this command as an SREM operation
            propargv[2] = objele;
            server.alsoPropagate(
                server.sremCommand,
                cli.db.id,
                &propargv,
                propargv.len,
                Server.PROPAGATE_AOF | Server.PROPAGATE_REPL,
            );
        }
        it.release();

        // Assign the new set as the key value.
        cli.db.overwrite(key, newset);
    }

    // Don't propagate the command itself even if we incremented the
    // dirty counter. We don't want to propagate an SPOP command since
    // we propagated the command as a set of SREMs operations using
    // the alsoPropagate() API.
    Server.preventCommandPropagation(cli);
    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;
}

/// SSCAN key cursor [MATCH pattern] [COUNT count]
pub fn sscanCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const cursor = db.Scan.parseCursorOrReply(argv[2], cli) orelse {
        return;
    };

    const key = argv[1];
    const sobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptyscan,
    ) orelse {
        return;
    };
    if (sobj.checkTypeOrReply(cli, .set)) {
        return;
    }

    db.Scan.scan(cli, sobj, cursor, Set.Hash, sscanCallback);
}

fn sscanCallback(privdata: ?*anyopaque, entry: *const Set.Hash.Entry) void {
    const keys: *db.Scan.Keys = @ptrCast(@alignCast(privdata.?));
    keys.append(Object.createString(sds.asBytes(entry.key)));
}

pub const Set = struct {
    const Value = union(enum) {
        num: i64,
        s: sds.String,
    };

    /// Return a set that *can* hold "value". When the object has
    /// an integer-encodable value, an intset will be returned.
    /// Otherwise a regular hash table.
    fn create(value: sds.String) *Object {
        _ = sds.asLongLong(value) orelse {
            return Object.createSet();
        };
        return Object.createIntSet();
    }

    /// Add the specified value into a set. If the value was already member of
    /// the set, nothing is done and FALSE is returned, otherwise the new
    /// element is added and TRUE is returned.
    fn add(sobj: *Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h = Hash.cast(sobj.v.ptr);
            return h.add(sds.dupe(allocator.impl, value), {});
        }
        if (sobj.encoding == .intset) {
            const llval = sds.asLongLong(value) orelse {
                convert(sobj, .ht);
                // The set *was* an intset and this value is not integer
                // encodable, so dict.add should always work.
                const h = Hash.cast(sobj.v.ptr);
                const ok = h.add(sds.dupe(allocator.impl, value), {});
                assert(ok);
                return true;
            };
            const is = IntSet.cast(sobj.v.ptr);
            const ret = is.add(llval);
            sobj.v = .{ .ptr = ret.set };
            if (ret.success) {
                // Convert to regular set when the intset contains
                // too many entries.
                var max_entries = server.set_max_intset_entries;
                // limit to 1G entries due to intset internals.
                if (max_entries >= (1 << 30)) max_entries = (1 << 30);
                if (ret.set.length.get() > max_entries) {
                    convert(sobj, .ht);
                }
                return true;
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    /// Convert the set to specified encoding. The resulting dict (when converting
    /// to a hash table) is presized to hold the number of elements in the original
    /// set.
    pub fn convert(sobj: *Object, enc: Object.Encoding) void {
        assert(sobj.type == .set and sobj.encoding == .intset);

        const is = IntSet.cast(sobj.v.ptr);
        defer is.free();

        if (enc == .ht) {
            const h = Hash.create(vtable);
            // Presize the dict to avoid rehashing
            _ = h.expand(is.length.get());
            var it = Iterator.create(sobj);
            while (it.next()) |value| switch (value) {
                .num => |v| {
                    const s = sds.fromLonglong(allocator.impl, v);
                    const ok = h.add(s, {});
                    assert(ok);
                },
                .s => unreachable,
            };
            it.release();
            sobj.v = .{ .ptr = h };
            sobj.encoding = .ht;
            return;
        }
        @panic("Unsupported set conversion");
    }

    fn remove(sobj: *Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h = Hash.cast(sobj.v.ptr);
            if (h.delete(value)) {
                if (Server.needShrinkDictToFit(h.size(), h.slots())) {
                    _ = h.shrinkToFit();
                }
                return true;
            }
            return false;
        }
        if (sobj.encoding == .intset) {
            if (sds.asLongLong(value)) |num| {
                const is = IntSet.cast(sobj.v.ptr);
                const ret = is.remove(num);
                sobj.v = .{ .ptr = ret.set };
                return ret.success;
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    fn isMember(sobj: *const Object, value: sds.String) bool {
        if (sobj.encoding == .ht) {
            const h = Hash.cast(sobj.v.ptr);
            return h.find(value) != null;
        }
        if (sobj.encoding == .intset) {
            if (sds.asLongLong(value)) |num| {
                const is = IntSet.cast(sobj.v.ptr);
                return is.find(num);
            }
            return false;
        }
        @panic("Unknown set encoding");
    }

    /// Return random element from a non empty set.
    fn randomElement(sobj: *Object) Value {
        if (sobj.encoding == .ht) {
            const h = Hash.cast(sobj.v.ptr);
            const de = h.getRandom().?;
            return .{ .s = de.key };
        }
        if (sobj.encoding == .intset) {
            const is = IntSet.cast(sobj.v.ptr);
            return .{ .num = is.random() };
        }
        @panic("Unknown set encoding");
    }

    pub fn size(sobj: *const Object) u64 {
        if (sobj.encoding == .ht) {
            const h = Hash.cast(sobj.v.ptr);
            return h.size();
        }
        if (sobj.encoding == .intset) {
            const is = IntSet.cast(sobj.v.ptr);
            return is.length.get();
        }
        @panic("Unknown set encoding");
    }

    pub const Iterator = struct {
        subject: *Object,
        encoding: Object.Encoding,
        ii: ?u32 = null, // intset iterator
        di: ?Hash.Iterator = null,

        pub fn create(sobj: *Object) Iterator {
            assert(sobj.type == .set);
            var it: Iterator = .{
                .subject = sobj,
                .encoding = sobj.encoding,
            };
            if (it.encoding == .ht) {
                const h = Hash.cast(sobj.v.ptr);
                it.di = h.iterator(false);
            } else if (it.encoding == .intset) {
                it.ii = 0;
            } else {
                @panic("Unknown set encoding");
            }
            return it;
        }

        /// Move to the next entry in the set. Returns the object at the current
        /// position.
        pub fn next(self: *Iterator) ?Value {
            if (self.encoding == .ht) {
                const entry = self.di.?.next() orelse {
                    return null;
                };
                return .{ .s = entry.key };
            }
            if (self.encoding == .intset) {
                const is = IntSet.cast(self.subject.v.ptr);
                const v = is.get(self.ii.?) orelse {
                    return null;
                };
                self.ii.? += 1;
                return .{ .num = v };
            }
            @panic("Wrong set encoding in Iterator.next");
        }

        /// The not copy on write friendly version but easy to use version
        /// of next() is nextObject(), returning new SDS strings.
        /// So if you don't retain a pointer to this object you should call
        /// sds.allocFree() against it.
        ///
        /// This function is the way to go for write operations where COW is not
        /// an issue.
        pub fn nextObject(self: *Iterator, alloc: Allocator) ?sds.String {
            if (self.next()) |value| switch (value) {
                .s => |v| return sds.dupe(alloc, v),
                .num => |v| return sds.fromLonglong(alloc, v),
            };
            return null;
        }

        pub fn release(self: *Iterator) void {
            if (self.encoding == .ht) {
                self.di.?.release();
            }
        }
    };

    pub const Hash = dict.Dict(sds.String, void);

    const vtable: *const Hash.VTable = &.{
        .hash = sds.hash,
        .eql = sds.eql,
        .freeKey = freeKey,
    };

    fn freeKey(key: sds.String) void {
        sds.free(allocator.impl, key);
    }
};

pub fn create() *Set.Hash {
    return Set.Hash.create(Set.vtable);
}

const Client = @import("networking.zig").Client;
const Object = @import("Object.zig");
const dict = @import("dict.zig");
const sds = @import("sds.zig");
const IntSet = @import("IntSet.zig");
const std = @import("std");
const assert = std.debug.assert;
const Server = @import("Server.zig");
const server = &Server.instance;
const db = @import("db.zig");
const allocator = @import("allocator.zig");
const Allocator = std.mem.Allocator;
const util = @import("util.zig");
const hasher = @import("hasher.zig");
