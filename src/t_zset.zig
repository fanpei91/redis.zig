// Input flags.
pub const ZADD_NONE = 0;
pub const ZADD_INCR = (1 << 0); // Increment the score instead of setting it.
pub const ZADD_NX = (1 << 1); // Don't touch elements not already existing.
pub const ZADD_XX = (1 << 2); // Only touch elements already existing.

// Output flags.
pub const ZADD_NOP = (1 << 3); // Operation not performed because of conditionals.
pub const ZADD_NAN = (1 << 4); // Only touch elements already existing.
pub const ZADD_ADDED = (1 << 5); // The element was new and was added.
pub const ZADD_UPDATED = (1 << 6); // The element already existed, score updated.

// Flags only used by the ZADD command but not by Zset.add() API:
pub const ZADD_CH = (1 << 16); // Return num of elements added or updated.

/// ZADD key [NX | XX] [CH] [INCR] score member [score member...]
pub fn zaddCommand(cli: *Client) void {
    zadd(cli, ZADD_NONE);
}

/// ZINCRBY key increment member
pub fn zincrbyCommand(cli: *Client) void {
    zadd(cli, ZADD_INCR);
}

/// Implements both ZADD and ZINCRBY.
fn zadd(cli: *Client, flags: i32) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // zig fmt: off
    var added: i64 = 0;     // Number of new elements added.
    var updated: i64 = 0;   // Number of elements with updated score.
    var processed: i64 = 0; // Number of elements processed, may remain zero
                            // with options like XX.
    // zig fmt: on

    // Parse options. At the end 'scoreidx' is set to the argument position
    // of the score of the first score-element pair.
    var scoreidx: usize = 2;
    var newflags = flags;
    while (scoreidx < cli.argc) {
        const opt = sds.castBytes(argv[scoreidx].v.ptr);
        if (eqlCase(opt, "nx")) {
            newflags |= ZADD_NX;
        } else if (eqlCase(opt, "xx")) {
            newflags |= ZADD_XX;
        } else if (eqlCase(opt, "ch")) {
            newflags |= ZADD_CH;
        } else if (eqlCase(opt, "incr")) {
            newflags |= ZADD_INCR;
        } else {
            break;
        }
        scoreidx += 1;
    }

    // Turn options into simple to check vars.
    const incr = newflags & ZADD_INCR != 0;
    const nx = newflags & ZADD_NX != 0;
    const xx = newflags & ZADD_XX != 0;
    const ch = newflags & ZADD_CH != 0;

    var elements = cli.argc - scoreidx;
    if (elements % 2 != 0 or elements == 0) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }
    // Now this holds the number of score-element pairs.
    elements = @divExact(elements, 2);

    // Check for incompatible options.
    if (nx and xx) {
        cli.addReplyErr("XX and NX options at the same time are not compatible");
        return;
    }

    if (incr and elements > 1) {
        cli.addReplyErr("INCR option supports a single increment-element pair");
        return;
    }

    // Start parsing all the scores, we need to emit any syntax error
    // before executing additions to the sorted set, as the command should
    // either execute fully or nothing at all.
    var scores = allocator.alloc(f64, elements);
    defer allocator.free(scores);
    for (0..elements) |i| {
        scores[i] = argv[scoreidx + i * 2].getDoubleOrReply(cli, null) orelse {
            return;
        };
    }

    var score: f64 = 0;
    biz: {
        const zobj = cli.db.lookupKeyWrite(key) orelse blk: {
            if (xx) break :biz;
            const member = sds.cast(argv[scoreidx + 1].v.ptr);
            var o: *Object = undefined;
            defer o.decrRefCount();
            if (server.zset_max_ziplist_entries == 0 or
                server.zset_max_ziplist_value < sds.getLen(member))
            {
                o = Object.createZset();
            } else {
                o = Object.createZsetZipList();
            }
            cli.db.add(key, o);
            break :blk o;
        };
        if (zobj.checkTypeOrReply(cli, .zset)) {
            return;
        }

        for (0..elements) |i| {
            score = scores[i];
            const ele = sds.cast(argv[scoreidx + i * 2 + 1].v.ptr);
            var newscore: f64 = undefined;
            if (!Zset.add(zobj, score, ele, &newflags, &newscore)) {
                cli.addReplyErr("resulting score is not a number (NaN)");
                return;
            }
            if (newflags & ZADD_ADDED != 0) added += 1;
            if (newflags & ZADD_UPDATED != 0) updated += 1;
            if (newflags & ZADD_NOP == 0) processed += 1;
            score = newscore;
        }
    }

    // reply to client
    if (incr) {
        // ZINCRBY or INCR option
        if (processed > 0) {
            cli.addReplyDouble(score);
        } else {
            cli.addReply(Server.shared.nullbulk);
        }
    } else {
        // ZADD
        cli.addReplyLongLong(if (ch) added + updated else added);
    }
}

/// ZCARD key
pub fn zcardCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    cli.addReplyLongLong(@intCast(Zset.length(zobj)));
}

/// ZSCORE key member
pub fn zscoreCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    const member = sds.cast(argv[2].v.ptr);
    const score = Zset.scoreOf(zobj, member) orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    cli.addReplyDouble(score);
}

/// ZRANK key member
pub fn zrankCommand(cli: *Client) void {
    zrank(cli, false);
}

/// ZREVRANK key member
pub fn zrevrankCommand(cli: *Client) void {
    zrank(cli, true);
}

/// ZRANK/ZREVRANK key member
fn zrank(cli: *Client, reverse: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullbulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    const ele = sds.cast(argv[2].v.ptr);
    const ranking = Zset.rank(zobj, ele, reverse) orelse {
        cli.addReply(Server.shared.nullbulk);
        return;
    };
    cli.addReplyLongLong(@intCast(ranking));
}

/// ZREM key member [member ...]
pub fn zremCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const zobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    var deleted: i64 = 0;
    for (argv[2..cli.argc]) |member| {
        if (Zset.del(zobj, sds.cast(member.v.ptr))) {
            deleted += 1;
        }
        if (Zset.length(zobj) == 0) {
            const ok = cli.db.delete(key);
            assert(ok);
            break;
        }
    }
    cli.addReplyLongLong(deleted);
}

/// ZREMRANGEBYRANK key start stop
pub fn zremrangebyrankCommand(cli: *Client) void {
    zremrangeby(cli, .rank);
}

/// ZREMRANGEBYSCORE key min max
pub fn zremrangebyscoreCommand(cli: *Client) void {
    zremrangeby(cli, .score);
}

/// ZREMRANGEBYLEX key min max
pub fn zremrangebylexCommand(cli: *Client) void {
    zremrangeby(cli, .lex);
}

/// Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE and ZREMRANGEBYLEX.
fn zremrangeby(cli: *Client, rangetype: RangeType) void {
    const argv = cli.argv.?;
    const key = argv[1];

    var start: i64 = undefined;
    var end: i64 = undefined;
    var range: Range = undefined;
    var lexrange: LexRange = undefined;
    defer if (rangetype == .lex) {
        lexrange.free();
    };

    switch (rangetype) {
        .rank => {
            start = argv[2].getLongLongOrReply(cli, null) orelse {
                return;
            };
            end = argv[3].getLongLongOrReply(cli, null) orelse {
                return;
            };
        },
        .score => {
            range = Range.parse(argv[2], argv[3]) orelse {
                cli.addReplyErr("min or max is not a float");
                return;
            };
        },
        .lex => {
            lexrange = LexRange.parse(argv[2], argv[3]) orelse {
                cli.addReplyErr("min or max not valid string range item");
                return;
            };
        },
    }

    const zobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    // Sanitize indexes.
    if (rangetype == .rank) {
        const llen: i64 = @intCast(Zset.length(zobj));
        if (start < 0) start = llen +% start;
        if (end < 0) end = llen +% end;
        if (start < 0) start = 0;

        // Invariant: start >= 0, so this test will be true when end < 0.
        // The range is empty when start > end or start >= length.
        if (start > end or start >= llen) {
            cli.addReply(Server.shared.czero);
            return;
        }
        if (end >= llen) end = llen - 1;
    }

    // Perform the range deletion operation.
    var deleted: u64 = 0;
    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        deleted = switch (rangetype) {
            .rank => zl.deleteRangeByRank(@intCast(start + 1), @intCast(end + 1)),
            .score => zl.deleteRangeByScore(range),
            .lex => zl.deleteRangeByLex(lexrange),
        };
        if (zl.length() == 0) {
            const ok = cli.db.delete(key);
            assert(ok);
        }
    } else if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        deleted = switch (rangetype) {
            .rank => sl.deleteRangeByRank(@intCast(start + 1), @intCast(end + 1)),
            .score => sl.deleteRangeByScore(range),
            .lex => sl.deleteRangeByLex(lexrange),
        };
        if (Server.needShrinkDictToFit(sl.dict.size(), sl.dict.slots())) {
            _ = sl.dict.shrinkToFit();
        }
        if (sl.sl.length == 0) {
            const ok = cli.db.delete(key);
            assert(ok);
        }
    } else {
        @panic("Unknown sorted set encoding");
    }

    cli.addReplyLongLong(@intCast(deleted));
}

/// ZRANGE key start stop [WITHSCORES]
pub fn zrangeCommand(cli: *Client) void {
    zrange(cli, false);
}

/// ZREVRANGE key start stop [WITHSCORES]
pub fn zrevrangeCommand(cli: *Client) void {
    zrange(cli, true);
}

/// ZRANGE/ZREVRANGE key start stop [WITHSCORES]
fn zrange(cli: *Client, reverse: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    var start = argv[2].getLongLongOrReply(cli, null) orelse {
        return;
    };
    var end = argv[3].getLongLongOrReply(cli, null) orelse {
        return;
    };
    var withscores = false;
    if (cli.argc == 5 and
        eqlCase(sds.castBytes(argv[4].v.ptr), "withscores"))
    {
        withscores = true;
    } else if (cli.argc >= 5) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    // Sanitize indexes.
    const llen: i64 = @intCast(Zset.length(zobj));
    if (start < 0) start = llen +% start;
    if (end < 0) end = llen +% end;
    if (start < 0) start = 0;

    // Invariant: start >= 0, so this test will be true when end < 0.
    // The range is empty when start > end or start >= length.
    if (start > end or start >= llen) {
        cli.addReply(Server.shared.emptymultibulk);
        return;
    }

    if (end >= llen) end = llen - 1;
    var rangelen = (end - start) + 1;

    // Return the result in form of a multi-bulk reply
    cli.addReplyMultiBulkLen(if (withscores) rangelen * 2 else rangelen);

    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        var eptr: ?[*]u8 = undefined;
        if (reverse) {
            eptr = zl.zl.index(@intCast(-2 - (2 * start)));
        } else {
            eptr = zl.zl.index(@intCast(2 * start));
        }
        assert(eptr != null);
        var sptr = zl.zl.next(eptr.?);

        while (rangelen > 0) : (rangelen -= 1) {
            assert(eptr != null and sptr != null);
            const value = ZipList.get(eptr.?).?;
            switch (value) {
                .num => |v| cli.addReplyLongLong(v),
                .str => |v| cli.addReplyBulkString(v),
            }
            if (withscores) {
                cli.addReplyDouble(ZipListSet.getScore(sptr.?));
            }
            if (reverse) {
                zl.prev(&eptr, &sptr);
            } else {
                zl.next(&eptr, &sptr);
            }
        }

        return;
    }

    if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        var ln: ?*SkipList.Node = null;
        // Check if starting point is trivial, before doing log(N) lookup.
        if (reverse) {
            ln = sl.sl.tail;
            if (start > 0) {
                ln = sl.sl.getElementByRank(@intCast(llen - start));
            }
        } else {
            ln = sl.sl.header.level(0).forward;
            if (start > 0) {
                ln = sl.sl.getElementByRank(@intCast(start + 1));
            }
        }
        while (rangelen > 0) : (rangelen -= 1) {
            assert(ln != null);
            const ele = ln.?.ele.?;
            cli.addReplyBulkString(sds.asBytes(ele));
            if (withscores) {
                cli.addReplyDouble(ln.?.score);
            }
            ln = if (reverse) ln.?.backward else ln.?.level(0).forward;
        }
        return;
    }

    @panic("Unknown sorted set encoding");
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
pub fn zrangebyscoreCommand(cli: *Client) void {
    zrangebyscore(cli, false);
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
pub fn zrevrangebyscoreCommand(cli: *Client) void {
    zrangebyscore(cli, true);
}

/// Implements ZRANGEBYSCORE and ZREVRANGEBYSCORE.
fn zrangebyscore(cli: *Client, reverse: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // Range is given as [min,max]
    var minidx: usize = 2;
    var maxidx: usize = 3;
    // Range is given as [max,min]
    if (reverse) {
        maxidx = 2;
        minidx = 3;
    }
    const range = Range.parse(argv[minidx], argv[maxidx]) orelse {
        cli.addReplyErr("min or max is not a float");
        return;
    };

    // Parse optional extra arguments.
    var withscores = false;
    var limit: i64 = -1;
    var offset: i64 = 0;
    if (cli.argc > 4) {
        var pos: usize = 4;
        var remaining = cli.argc - 4;
        while (remaining > 0) {
            if (remaining >= 1 and
                eqlCase(sds.castBytes(argv[pos].v.ptr), "withscores"))
            {
                pos += 1;
                remaining -= 1;
                withscores = true;
            } else if (remaining >= 3 and
                eqlCase(sds.castBytes(argv[pos].v.ptr), "limit"))
            {
                offset = argv[pos + 1].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                limit = argv[pos + 2].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                pos += 3;
                remaining -= 3;
            } else {
                cli.addReply(Server.shared.syntaxerr);
                return;
            }
        }
    }

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    var replylen: ?*Client.ReplyBlock = null;
    var rangelen: i64 = 0;
    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        var eptr = blk: {
            // If reversed, get the last node in range as starting point.
            if (reverse) {
                break :blk zl.lastInRange(range);
            }
            break :blk zl.firstInRange(range);
        };
        if (eptr == null) {
            cli.addReply(Server.shared.emptymultibulk);
            return;
        }
        assert(eptr != null);

        // Get score pointer for the first element.
        var sptr = zl.zl.next(eptr.?);
        assert(sptr != null);

        // We don't know in advance how many matching elements there are in the
        // list, so we push this object that will represent the multi-bulk
        // length in the output buffer, and will "fix" it later
        replylen = cli.addDeferredMultiBulkLength();

        // If there is an offset, just traverse the number of elements without
        // checking the score because that is done in the next loop.
        while (eptr != null and offset > 0) : (offset -= 1) {
            if (reverse) {
                zl.prev(&eptr, &sptr);
            } else {
                zl.next(&eptr, &sptr);
            }
        }

        while (eptr != null and limit != 0) : (limit -= 1) {
            const score = ZipListSet.getScore(sptr.?);
            // Abort when the node is no longer in range.
            if (reverse) {
                if (!range.valueGteMin(score)) break;
            } else {
                if (!range.valueLteMax(score)) break;
            }

            // We know the element exists, so ZipList.get() should always succeed
            const value = ZipList.get(eptr.?).?;
            rangelen += 1;
            switch (value) {
                .num => |v| cli.addReplyBulkLongLong(v),
                .str => |v| cli.addReplyBulkString(v),
            }
            if (withscores) {
                cli.addReplyDouble(score);
            }

            // Move to next node
            if (reverse) {
                zl.prev(&eptr, &sptr);
            } else {
                zl.next(&eptr, &sptr);
            }
        }
    } else if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        var ln = blk: {
            if (reverse) {
                // If reversed, get the last node in range as starting point.
                break :blk sl.sl.lastInRange(range);
            }
            break :blk sl.sl.firstInRange(range);
        };
        // No "first" element in the specified interval.
        if (ln == null) {
            cli.addReply(Server.shared.emptymultibulk);
            return;
        }

        // We don't know in advance how many matching elements there are in the
        // list, so we push this object that will represent the multi-bulk
        // length in the output buffer, and will "fix" it later
        replylen = cli.addDeferredMultiBulkLength();

        // If there is an offset, just traverse the number of elements without
        // checking the score because that is done in the next loop.
        while (ln != null and offset > 0) : (offset -= 1) {
            if (reverse) {
                ln = ln.?.backward;
            } else {
                ln = ln.?.level(0).forward;
            }
        }

        while (ln != null and limit != 0) : (limit -= 1) {
            if (reverse) {
                if (!range.valueGteMin(ln.?.score)) break;
            } else {
                if (!range.valueLteMax(ln.?.score)) break;
            }

            rangelen += 1;
            cli.addReplyBulkString(sds.asBytes(ln.?.ele.?));
            if (withscores) {
                cli.addReplyDouble(ln.?.score);
            }

            // Move to next node
            if (reverse) {
                ln = ln.?.backward;
            } else {
                ln = ln.?.level(0).forward;
            }
        }
    } else {
        @panic("Unknown sorted set encoding");
    }

    if (withscores) {
        rangelen *= 2;
    }

    cli.setDeferredMultiBulkLength(replylen, rangelen);
}

/// ZRANGEBYLEX key min max [LIMIT offset count]
pub fn zrangebylexCommand(cli: *Client) void {
    zrangebylex(cli, false);
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count]
pub fn zrevrangebylexCommand(cli: *Client) void {
    zrangebylex(cli, true);
}

/// Implements ZRANGEBYLEX and ZREVRANGEBYLEX
fn zrangebylex(cli: *Client, reverse: bool) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // Range is given as [min,max]
    var minidx: usize = 2;
    var maxidx: usize = 3;
    if (reverse) {
        // Range is given as [max,min]
        maxidx = 2;
        minidx = 3;
    }
    const lexrange = LexRange.parse(argv[minidx], argv[maxidx]) orelse {
        cli.addReplyErr("min or max not valid string range item");
        return;
    };
    defer lexrange.free();

    // Parse optional extra arguments.
    var offset: i64 = 0;
    var limit: i64 = -1;
    if (cli.argc > 4) {
        var pos: usize = 4;
        var remaining = cli.argc - 4;
        while (remaining > 0) {
            if (remaining >= 3 and
                eqlCase(sds.castBytes(argv[pos].v.ptr), "limit"))
            {
                offset = argv[pos + 1].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                limit = argv[pos + 2].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                pos += 3;
                remaining -= 3;
            } else {
                cli.addReply(Server.shared.syntaxerr);
                return;
            }
        }
    }

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.emptymultibulk,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    var replylen: ?*Client.ReplyBlock = null;
    var rangelen: i64 = 0;
    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        var eptr = blk: {
            // If reversed, get the last node in range as starting point.
            if (reverse) {
                break :blk zl.lastInLexRange(lexrange);
            }
            break :blk zl.firstInLexRange(lexrange);
        };
        if (eptr == null) {
            cli.addReply(Server.shared.emptymultibulk);
            return;
        }
        assert(eptr != null);
        // Get score pointer for the first element.
        var sptr = zl.zl.next(eptr.?);
        assert(sptr != null);

        // We don't know in advance how many matching elements there are in the
        // list, so we push this object that will represent the multi-bulk
        // length in the output buffer, and will "fix" it later
        replylen = cli.addDeferredMultiBulkLength();

        // If there is an offset, just traverse the number of elements without
        // checking the score because that is done in the next loop.
        while (eptr != null and offset > 0) : (offset -= 1) {
            if (reverse) {
                zl.prev(&eptr, &sptr);
            } else {
                zl.next(&eptr, &sptr);
            }
        }

        while (eptr != null and limit != 0) : (limit -= 1) {
            const epo = ZipList.getObject(eptr.?);
            defer sds.free(allocator.child, epo);
            // Abort when the node is no longer in range.
            if (reverse) {
                if (!lexrange.valueGteMin(epo)) break;
            } else {
                if (!lexrange.valueLteMax(epo)) break;
            }

            // We know the element exists, so ZipList.get() should always succeed
            const value = ZipList.get(eptr.?).?;
            rangelen += 1;
            switch (value) {
                .num => |v| cli.addReplyBulkLongLong(v),
                .str => |v| cli.addReplyBulkString(v),
            }

            // Move to next node
            if (reverse) {
                zl.prev(&eptr, &sptr);
            } else {
                zl.next(&eptr, &sptr);
            }
        }
    } else if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        var ln = blk: {
            if (reverse) {
                // If reversed, get the last node in lex range as starting point.
                break :blk sl.sl.lastInLexRange(lexrange);
            }
            break :blk sl.sl.firstInLexRange(lexrange);
        };
        // No "first" element in the specified interval.
        if (ln == null) {
            cli.addReply(Server.shared.emptymultibulk);
            return;
        }

        // We don't know in advance how many matching elements there are in the
        // list, so we push this object that will represent the multi-bulk
        // length in the output buffer, and will "fix" it later
        replylen = cli.addDeferredMultiBulkLength();

        // If there is an offset, just traverse the number of elements without
        // checking the score because that is done in the next loop.
        while (ln != null and offset > 0) : (offset -= 1) {
            if (reverse) {
                ln = ln.?.backward;
            } else {
                ln = ln.?.level(0).forward;
            }
        }

        while (ln != null and limit != 0) : (limit -= 1) {
            if (reverse) {
                if (!lexrange.valueGteMin(ln.?.ele.?)) break;
            } else {
                if (!lexrange.valueLteMax(ln.?.ele.?)) break;
            }

            rangelen += 1;
            cli.addReplyBulkString(sds.asBytes(ln.?.ele.?));

            // Move to next node
            if (reverse) {
                ln = ln.?.backward;
            } else {
                ln = ln.?.level(0).forward;
            }
        }
    } else {
        @panic("Unknown sorted set encoding");
    }
    cli.setDeferredMultiBulkLength(replylen, rangelen);
}

/// ZCOUNT key min max
pub fn zcountCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const range = Range.parse(argv[2], argv[3]) orelse {
        cli.addReplyErr("min or max is not a float");
        return;
    };

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    var count: i64 = 0;
    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        var eptr = zl.firstInRange(range);
        if (eptr == null) {
            cli.addReply(Server.shared.czero);
            return;
        }

        var sptr = zl.zl.next(eptr.?);
        assert(sptr != null);

        // Iterate over elements in range
        while (eptr != null) {
            const score = ZipListSet.getScore(sptr.?);
            // Abort when the node is no longer in range.
            if (!range.valueLteMax(score)) {
                break;
            } else {
                count += 1;
                zl.next(&eptr, &sptr);
            }
        }
        assert(count > 0);
    } else if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        // Find first element in range
        if (sl.sl.firstInRange(range)) |first| {
            // Use rank of first element, if any, to determine
            // preliminary count
            var rank = sl.sl.getRank(first.score, first.ele.?);
            var cnt = sl.sl.length - (rank - 1);

            // Find last element in range
            if (sl.sl.lastInRange(range)) |last| {
                // Use rank of last element, if any, to determine the
                // actual count
                rank = sl.sl.getRank(last.score, last.ele.?);
                cnt -= (sl.sl.length - rank);
            }
            count = @intCast(cnt);
            assert(count > 0);
        }
    } else {
        @panic("Unknown sorted set encoding");
    }
    cli.addReplyLongLong(count);
}

/// ZLEXCOUNT key min max
pub fn zlexcountCommand(cli: *Client) void {
    const argv = cli.argv.?;
    const key = argv[1];

    const range = LexRange.parse(argv[2], argv[3]) orelse {
        cli.addReplyErr("min or max not valid string range item");
        return;
    };

    const zobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (zobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    var count: i64 = 0;
    if (zobj.encoding == .ziplist) {
        const zl: *ZipListSet = .cast(zobj.v.ptr);
        var eptr = zl.firstInLexRange(range);
        if (eptr == null) {
            cli.addReply(Server.shared.czero);
            return;
        }

        var sptr = zl.zl.next(eptr.?);
        assert(sptr != null);

        // Iterate over elements in range
        while (eptr != null) {
            const value = ZipList.getObject(eptr.?);
            defer sds.free(allocator.child, value);
            // Abort when the node is no longer in range.
            if (!range.valueLteMax(value)) {
                break;
            } else {
                count += 1;
                zl.next(&eptr, &sptr);
            }
        }
        assert(count > 0);
    } else if (zobj.encoding == .skiplist) {
        const sl: *SkipListSet = .cast(zobj.v.ptr);
        // Find first element in range
        if (sl.sl.firstInLexRange(range)) |first| {
            // Use rank of first element, if any, to determine
            // preliminary count
            var rank = sl.sl.getRank(first.score, first.ele.?);
            var cnt = sl.sl.length - (rank - 1);

            // Find last element in range
            if (sl.sl.lastInLexRange(range)) |last| {
                // Use rank of last element, if any, to determine the
                // actual count
                rank = sl.sl.getRank(last.score, last.ele.?);
                cnt -= (sl.sl.length - rank);
            }
            count = @intCast(cnt);
            assert(count > 0);
        }
    } else {
        @panic("Unknown sorted set encoding");
    }
    cli.addReplyLongLong(count);
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
pub fn zscanCommand(cli: *Client) void {
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
    if (sobj.checkTypeOrReply(cli, .zset)) {
        return;
    }

    db.Scan.scan(cli, sobj, cursor, SkipListSet.HashMap, zscanCallback);
}

fn zscanCallback(privdata: ?*anyopaque, entry: *const SkipListSet.HashMap.Entry) void {
    const keys: *db.Scan.Keys = @ptrCast(@alignCast(privdata.?));
    keys.append(Object.createString(sds.asBytes(entry.key)));
    keys.append(Object.createStringFromLongDouble(entry.val.*, true));
}

const Zset = struct {
    /// Add a new element or update the score of an existing element in a sorted
    /// set, regardless of its encoding.
    ///
    /// The set of flags change the command behavior. They are passed with an integer
    /// pointer since the function will clear the flags and populate them with
    /// other flags to indicate different conditions.
    ///
    /// The input flags are the following:
    ///
    /// ZADD_INCR: Increment the current element score by 'score' instead of updating
    ///            the current element score. If the element does not exist, we
    ///            assume 0 as previous score.
    /// ZADD_NX:   Perform the operation only if the element does not exist.
    /// ZADD_XX:   Perform the operation only if the element already exist.
    ///
    /// When ZADD_INCR is used, the new score of the element is stored in
    /// '*newscore' if 'newscore' is not NULL.
    ///
    /// The returned flags are the following:
    ///
    /// ZADD_NAN:     The resulting score is not a number.
    /// ZADD_ADDED:   The element was added (not present before the call).
    /// ZADD_UPDATED: The element score was updated.
    /// ZADD_NOP:     No operation was performed because of NX or XX.
    ///
    /// Return value:
    ///
    /// The function returns TRUE on success, and sets the appropriate flags
    /// ADDED or UPDATED to signal what happened during the operation (note that
    /// none could be set if we re-added an element using the same score it used
    /// to have, or in the case a zero increment is used).
    ///
    /// The function returns FALSE on erorr, currently only when the increment
    /// produces a NAN condition, or when the 'score' value is NAN since the
    /// start.
    ///
    /// The commad as a side effect of adding a new element may convert the sorted
    /// set internal encoding from ziplist to hashtable+skiplist.
    ///
    /// Memory managemnet of 'ele':
    ///
    /// The function does not take ownership of the 'ele' SDS string, but copies
    /// it if needed.
    pub fn add(
        zobj: *Object,
        score: f64,
        ele: sds.String,
        flags: *i32,
        newscore: ?*f64,
    ) bool {
        // Turn options into simple to check vars.
        const incr = flags.* & ZADD_INCR != 0;
        const nx = flags.* & ZADD_NX != 0;
        const xx = flags.* & ZADD_XX != 0;
        flags.* = 0; // We'll return our response flags.
        var inscore = score;

        // NaN as input is an error regardless of all the other parameters.
        if (isNan(score)) {
            flags.* |= ZADD_NAN;
            return false;
        }

        if (zobj.encoding == .ziplist) {
            var curscore: f64 = undefined;
            var zl: *ZipListSet = .cast(zobj.v.ptr);
            if (zl.find(ele, &curscore)) |eptr| {
                // NX? Return, same element already exists.
                if (nx) {
                    flags.* |= ZADD_NOP;
                    return true;
                }
                // Prepare the score for the increment if needed.
                if (incr) {
                    inscore += curscore;
                    if (isNan(inscore)) {
                        flags.* |= ZADD_NAN;
                        return false;
                    }
                    if (newscore) |new| new.* = inscore;
                }
                // Remove and re-insert when score changed.
                if (inscore != curscore) {
                    _ = zl.delete(eptr);
                    zl.insert(ele, inscore);
                    flags.* |= ZADD_UPDATED;
                }
                return true;
            } else if (!xx) {
                // check if the element is too large or the list
                // becomes too long *before* executing insert.
                if (zl.length() + 1 > server.zset_max_ziplist_entries or
                    sds.getLen(ele) > server.zset_max_ziplist_value or
                    !ZipList.safeToAdd(zl.zl, sds.getLen(ele)))
                {
                    convert(zobj, .skiplist);
                } else {
                    zl.insert(ele, score);
                    if (newscore) |new| new.* = score;
                    flags.* |= ZADD_ADDED;
                    return true;
                }
            } else {
                flags.* |= ZADD_NOP;
                return true;
            }
        }

        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            if (sl.dict.find(ele)) |de| {
                // NX? Return, same element already exists.
                if (nx) {
                    flags.* |= ZADD_NOP;
                    return true;
                }
                const curscore = de.val.*;
                if (incr) {
                    inscore += curscore;
                    if (isNan(inscore)) {
                        flags.* |= ZADD_NAN;
                        return false;
                    }
                    if (newscore) |new| new.* = inscore;
                }
                // Remove and re-insert when score changes.
                if (inscore != curscore) {
                    const node = sl.sl.updateScore(curscore, ele, inscore);
                    // Note that we did not removed the original element from
                    // the hash table representing the sorted set, so we just
                    // update the score.
                    de.val = &node.score;
                    flags.* |= ZADD_UPDATED;
                }
                return true;
            } else if (!xx) {
                const elm = sds.dupe(allocator.child, ele);
                const node = sl.sl.insert(score, elm);
                const ok = sl.dict.add(elm, &node.score);
                assert(ok);
                flags.* |= ZADD_ADDED;
                if (newscore) |new| new.* = score;
                return true;
            }
            flags.* |= ZADD_NOP;
            return true;
        }

        @panic("Unknown sorted set encoding");
    }

    /// Delete the element 'ele' from the sorted set, returning TRUE if the
    /// element existed and was deleted, FALSE otherwise if the element was
    /// not there.
    pub fn del(zobj: *Object, ele: sds.String) bool {
        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            const eptr = zl.find(ele, null) orelse {
                return false;
            };
            _ = zl.delete(eptr);
            return true;
        }

        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            const de = sl.dict.find(ele) orelse {
                return false;
            };
            // Get the score in order to delete from the skiplist later.
            const score = de.val.*;

            var ok = sl.dict.delete(de.key);
            assert(ok);
            ok = sl.sl.delete(score, ele);
            assert(ok);

            if (Server.needShrinkDictToFit(sl.dict.size(), sl.dict.slots())) {
                _ = sl.dict.shrinkToFit();
            }
            return true;
        }

        @panic("Unknown sorted set encoding");
    }

    pub fn convert(zobj: *Object, encoding: Object.Encoding) void {
        if (zobj.encoding == encoding) return;

        if (zobj.encoding == .ziplist) {
            assert(encoding == .skiplist);

            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var eptr = zl.zl.index(0);
            assert(eptr != null);
            var sptr = zl.zl.next(eptr.?);
            assert(sptr != null);

            const sl = SkipListSet.create();
            while (eptr) |ep| {
                const score = ZipListSet.getScore(sptr.?);
                const ele = switch (ZipList.get(ep).?) {
                    .num => |v| sds.fromLonglong(allocator.child, v),
                    .str => |v| sds.new(allocator.child, v),
                };
                sl.insert(score, ele);
                zl.next(&eptr, &sptr);
            }

            zl.destroy();
            zobj.v = .{ .ptr = sl };
            zobj.encoding = .skiplist;
            return;
        }

        if (zobj.encoding == .skiplist) {
            assert(encoding == .ziplist);

            const sl: *SkipListSet = .cast(zobj.v.ptr);
            var node = sl.sl.header.level(0).forward;
            allocator.destroy(sl.sl.header);
            allocator.destroy(sl.sl);
            sl.dict.destroy();
            allocator.destroy(sl);

            const zl = ZipListSet.create();
            while (node) |n| {
                zl.insertAt(null, n.ele.?, n.score);
                node = n.level(0).forward;
                allocator.destroy(n);
            }

            zobj.v = .{ .ptr = zl };
            zobj.encoding = .ziplist;
            return;
        }

        @panic("Unknown sorted set encoding");
    }

    pub fn length(zobj: *Object) u64 {
        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            return zl.length();
        }
        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            return sl.sl.length;
        }
        @panic("Unknown sorted set encoding");
    }

    /// Given a sorted set object returns the 0-based rank of the object or
    /// NULL if the object does not exist.
    ///
    /// For rank we mean the position of the element in the sorted collection
    /// of elements. So the first element has rank 0, the second rank 1, and so
    /// forth up to length-1 elements.
    ///
    /// If 'reverse' is false, the rank is returned considering as first element
    /// the one with the lowest score. Otherwise if 'reverse' is non-zero
    /// the rank is computed considering as element with rank 0 the one with
    /// the highest score.
    fn rank(zobj: *Object, ele: sds.String, reverse: bool) ?u64 {
        const llen = length(zobj);

        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var eptr = zl.zl.index(0);
            assert(eptr != null);
            var sptr = zl.zl.next(eptr.?);
            assert(sptr != null);

            var ranking: u64 = 1;
            while (eptr) |ep| {
                if (ZipList.eql(ep, sds.asBytes(ele))) {
                    break;
                }
                ranking += 1;
                zl.next(&eptr, &sptr);
            }
            if (eptr != null) {
                if (reverse) {
                    return llen - ranking;
                }
                return ranking - 1;
            }
            return null;
        }

        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            const de = sl.dict.find(ele) orelse {
                return null;
            };
            const score = de.val.*;
            const ranking = sl.sl.getRank(score, ele);
            // Existing elements always have a rank.
            assert(ranking != 0);
            if (reverse) {
                return llen - ranking;
            }
            return ranking - 1;
        }

        @panic("Unknown sorted set encoding");
    }

    /// Return the score of the specified member of the sorted set. If the
    /// element does not exist NULL is returned.
    pub fn scoreOf(zobj: *Object, member: sds.String) ?f64 {
        if (zobj.encoding == .ziplist) {
            const zl: *ZipListSet = .cast(zobj.v.ptr);
            var score: f64 = undefined;
            _ = zl.find(member, &score) orelse {
                return null;
            };
            return score;
        }
        if (zobj.encoding == .skiplist) {
            const sl: *SkipListSet = .cast(zobj.v.ptr);
            const de = sl.dict.find(member) orelse {
                return null;
            };
            return de.val.*;
        }
        @panic("Unknown sorted set encoding");
    }
};

pub const ZipListSet = struct {
    zl: *ZipList,

    pub fn create() *ZipListSet {
        const zls = allocator.create(ZipListSet);
        zls.zl = ZipList.new();
        return zls;
    }

    pub fn cast(ptr: *anyopaque) *ZipListSet {
        return @ptrCast(@alignCast(ptr));
    }

    fn find(self: *ZipListSet, ele: sds.String, score: ?*f64) ?[*]u8 {
        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const sptr = self.zl.next(ep).?;
            if (ZipList.eql(ep, sds.asBytes(ele))) {
                // Matching element, pull out score.
                if (score) |s| s.* = getScore(sptr);
                return ep;
            }
            // Move to next element.
            eptr = self.zl.next(sptr);
        }
        return null;
    }

    pub fn getScore(sptr: [*]u8) f64 {
        const value = ZipList.get(sptr).?;
        return switch (value) {
            .num => |v| @floatFromInt(v),
            .str => |v| std.fmt.parseFloat(f64, v) catch unreachable,
        };
    }

    fn length(self: *ZipListSet) usize {
        return @divExact(self.zl.len.get(), 2);
    }

    /// Find pointer to the last element contained in the specified range.
    /// Returns NULL when no element is contained in the range.
    pub fn lastInRange(self: *ZipListSet, range: Range) ?[*]u8 {
        // If everything is out of range, return early.
        if (!self.isInRange(range)) {
            return null;
        }

        var eptr = self.zl.index(-2);
        while (eptr) |ep| {
            var sptr = self.zl.next(ep).?;
            const score = getScore(sptr);
            if (range.valueLteMax(score)) {
                // Check if score >= min.
                if (range.valueGteMin(score)) {
                    return ep;
                }
                return null;
            }
            // Move to previous element by moving to the score of previous
            // element. When this returns NULL, we know there also is no
            // element.
            sptr = self.zl.prev(ep) orelse {
                return null;
            };
            eptr = self.zl.prev(sptr);
            assert(eptr != null);
        }

        return null;
    }

    /// Find pointer to the last element contained in the specified lex range.
    /// Returns NULL when no element is contained in the lex range.
    pub fn lastInLexRange(self: *ZipListSet, range: LexRange) ?[*]u8 {
        // If everything is out of range, return early.
        if (!self.isInLexRange(range)) {
            return null;
        }

        var eptr = self.zl.index(-2);
        while (eptr) |ep| {
            const value = ZipList.getObject(ep);
            defer sds.free(allocator.child, value);
            if (range.valueLteMax(value)) {
                // Check if value >= min.
                if (range.valueGteMin(value)) {
                    return ep;
                }
                return null;
            }
            // Move to previous element by moving to the score of previous
            // element. When this returns NULL, we know there also is no
            // element.
            const sptr = self.zl.prev(ep) orelse {
                return null;
            };
            eptr = self.zl.prev(sptr);
            assert(eptr != null);
        }

        return null;
    }

    /// Find pointer to the first element contained in the specified range.
    /// Returns NULL when no element is contained in the range.
    pub fn firstInRange(self: *ZipListSet, range: Range) ?[*]u8 {
        // If everything is out of range, return early.
        if (!self.isInRange(range)) {
            return null;
        }

        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const sptr = self.zl.next(ep).?;
            const score = getScore(sptr);
            if (range.valueGteMin(score)) {
                // Check if score <= max.
                if (range.valueLteMax(score)) {
                    return ep;
                }
                return null;
            }
            // Move to next element.
            eptr = self.zl.next(sptr);
        }

        return null;
    }

    /// Find pointer to the first element contained in the specified lex range.
    /// Returns NULL when no element is contained in the lex range.
    pub fn firstInLexRange(self: *ZipListSet, range: LexRange) ?[*]u8 {
        // If everything is out of range, return early.
        if (!self.isInLexRange(range)) {
            return null;
        }

        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const value = ZipList.getObject(ep);
            defer sds.free(allocator.child, value);
            if (range.valueGteMin(value)) {
                // Check if value <= max.
                if (range.valueLteMax(value)) {
                    return ep;
                }
                return null;
            }
            // Move to next element.
            const sptr = self.zl.next(ep).?; // This element score. Skip it.
            eptr = self.zl.next(sptr);
        }

        return null;
    }

    /// Returns if there is a part of the zset is in range.
    fn isInRange(self: *ZipListSet, range: Range) bool {
        if (range.min > range.max or
            (range.min == range.max and (range.minex or range.maxex)))
        {
            return false;
        }

        // Last score
        var sptr = self.zl.index(-1) orelse {
            return false;
        };
        var score = getScore(sptr);
        if (!range.valueGteMin(score)) {
            return false;
        }

        // First Score
        sptr = self.zl.index(1).?;
        score = getScore(sptr);
        if (!range.valueLteMax(score)) {
            return false;
        }

        return true;
    }
    /// Returns if there is a part of the zset is in lex range.
    fn isInLexRange(self: *ZipListSet, range: LexRange) bool {
        //  Test for ranges that will always be empty.
        const order = sds.cmplex(
            range.min.?,
            range.max.?,
            Server.shared.minstring,
            Server.shared.maxstring,
        );
        if (order == .gt or (order == .eq and (range.minex or range.maxex))) {
            return false;
        }

        // Last element
        var eptr = self.zl.index(-2) orelse {
            return false;
        };
        const last = ZipList.getObject(eptr);
        defer sds.free(allocator.child, last);
        if (!range.valueGteMin(last)) {
            return false;
        }

        // First Element
        eptr = self.zl.index(0).?;
        const first = ZipList.getObject(eptr);
        defer sds.free(allocator.child, first);
        if (!range.valueLteMax(first)) {
            return false;
        }

        return true;
    }

    /// Move to next entry based on the values in eptr and sptr. Both are set to
    /// NULL when there is no next entry.
    pub fn next(self: *ZipListSet, eptr: *?[*]u8, sptr: *?[*]u8) void {
        assert(eptr.* != null);
        assert(sptr.* != null);

        const ep = self.zl.next(sptr.*.?);
        if (ep) |ptr| {
            const sp = self.zl.next(ptr).?;
            eptr.* = ptr;
            sptr.* = sp;
            return;
        }
        eptr.* = null;
        sptr.* = null;
    }

    /// Move to the previous entry based on the values in eptr and sptr. Both are
    /// set to NULL when there is no next entry.
    fn prev(self: *ZipListSet, eptr: *?[*]u8, sptr: *?[*]u8) void {
        assert(eptr.* != null);
        assert(sptr.* != null);

        const sp = self.zl.prev(eptr.*.?);
        if (sp) |ptr| {
            const ep = self.zl.prev(ptr).?;
            eptr.* = ep;
            sptr.* = ptr;
            return;
        }
        eptr.* = null;
        sptr.* = null;
    }

    /// Delete (element,score) pair from ziplist. Return the element pointer
    /// next to the deleted score pointer.
    fn delete(self: *ZipListSet, eptr: [*]u8) [*]u8 {
        var p = eptr;
        self.zl = self.zl.delete(&p);
        self.zl = self.zl.delete(&p);
        return p;
    }

    /// Delete all the elements with rank between start and end from the ziplist.
    /// Start and end are inclusive. Note that start and end need to be 1-based.
    /// Return the number of deleted elements.
    fn deleteRangeByRank(self: *ZipListSet, start: u64, end: u64) u64 {
        const num: u32 = @intCast((end - start) + 1);
        self.zl = self.zl.deleteRange(@intCast(2 * (start - 1)), num * 2);
        return num;
    }

    /// Delete all the elements in the range from the ziplist.
    /// Return the number of deleted elements.
    fn deleteRangeByScore(self: *ZipListSet, range: Range) u64 {
        var eptr = self.firstInRange(range) orelse {
            return 0;
        };

        var num: u64 = 0;
        // When the tail of the ziplist is deleted, eptr will point to the
        // sentinel byte and ZipList.Next() will return NULL.
        while (self.zl.next(eptr)) |sptr| {
            const score = getScore(sptr);
            if (range.valueLteMax(score)) {
                // Delete both the element and the score.
                eptr = self.delete(eptr);
                num += 1;
            } else {
                // No longer in range.
                break;
            }
        }
        return num;
    }

    /// Delete all the elements in the lex range from the ziplist.
    /// Return the number of deleted elements.
    fn deleteRangeByLex(self: *ZipListSet, range: LexRange) u64 {
        var eptr = self.firstInLexRange(range) orelse {
            return 0;
        };

        var num: u64 = 0;
        // When the tail of the ziplist is deleted, eptr will point to the
        // sentinel byte and ZipList.Next() will return NULL.
        while (self.zl.next(eptr)) |_| {
            const value = ZipList.getObject(eptr);
            defer sds.free(allocator.child, value);
            if (range.valueLteMax(value)) {
                // Delete both the element and the score.
                eptr = self.delete(eptr);
                num += 1;
            } else {
                // No longer in range.
                break;
            }
        }
        return num;
    }

    /// Insert (element,score) pair in ziplist. This function assumes the
    /// element is not yet present in the list.
    fn insert(self: *ZipListSet, ele: sds.String, score: f64) void {
        var eptr = self.zl.index(0);
        while (eptr) |ep| {
            const sptr = self.zl.next(ep).?;
            const s = getScore(sptr);
            if (s > score) {
                // First element with score larger than score for element to be
                // inserted. This means we should take its spot in the list to
                // maintain ordering.
                self.insertAt(ep, ele, score);
                break;
            } else if (s == score) {
                // Ensure lexicographical ordering for elements.
                if (compareElements(ep, ele) == .gt) {
                    self.insertAt(ep, ele, score);
                    break;
                }
            }
            // Move to next element.
            eptr = self.zl.next(sptr);
        }

        // Push on tail of list when it was not yet inserted.
        if (eptr == null) {
            self.insertAt(null, ele, score);
        }
    }

    fn insertAt(
        self: *ZipListSet,
        eptr: ?[*]u8,
        ele: sds.String,
        score: f64,
    ) void {
        var buf: [128]u8 = undefined;
        const estr = sds.asBytes(ele);
        const sstr = util.d2string(&buf, score);
        if (eptr) |ep| {
            // Keep offset relative to zl, as it might be re-allocated.
            const offset = @intFromPtr(ep) - @intFromPtr(self.zl.addr());
            self.zl = self.zl.insert(ep, estr);
            var ptr = self.zl.addr() + offset;
            ptr = self.zl.next(ptr).?;
            // Insert score after the element.
            self.zl = self.zl.insert(ptr, sstr);
        } else {
            self.zl = self.zl.push(estr, .tail);
            self.zl = self.zl.push(sstr, .tail);
        }
    }

    /// Compare element in sorted set with given element.
    fn compareElements(eptr: [*]u8, ele: sds.String) std.math.Order {
        const value = ZipList.get(eptr).?;
        var buf: [32]u8 = undefined;
        const estr = switch (value) {
            .str => |v| v,
            .num => |v| util.ll2string(&buf, v),
        };
        return std.mem.order(u8, estr, sds.asBytes(ele));
    }

    pub fn destroy(self: *ZipListSet) void {
        self.zl.free();
        allocator.destroy(self);
    }
};

pub const SkipListSet = struct {
    pub const HashMap = dict.Dict(sds.String, *f64);

    sl: *SkipList,
    dict: *HashMap,

    pub fn create() *SkipListSet {
        const z = allocator.create(SkipListSet);
        z.dict = HashMap.create(&.{
            .hash = sds.hash,
            .eql = sds.eql,
        });
        z.sl = SkipList.create();
        return z;
    }

    /// The inside skiplist takes ownership of the passed SDS string 'ele'.
    fn insert(self: *SkipListSet, score: f64, ele: sds.String) void {
        const node = self.sl.insert(score, ele);
        const ok = self.dict.add(ele, &node.score);
        assert(ok);
    }

    /// Delete all the elements with rank between start and end from the skiplist.
    /// Start and end are inclusive. Note that start and end need to be 1-based.
    fn deleteRangeByRank(self: *SkipListSet, start: u64, end: u64) u64 {
        var update: [ZSKIPLIST_MAXLEVEL]*SkipList.Node = undefined;
        var x = self.sl.header;
        var i = self.sl.level - 1;
        var traversed: usize = 0;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward != null and
                (traversed + x.level(i).span < start))
            {
                traversed += x.level(i).span;
                x = x.level(i).forward.?;
            }
            update[i] = x;
            if (i == 0) break;
        }

        var removed: u64 = 0;
        traversed += 1;
        var f = x.level(0).forward;
        while (f != null and traversed <= end) {
            const next = f.?.level(0).forward;
            self.sl.deleteNode(f.?, &update);
            const ok = self.dict.delete(f.?.ele.?);
            assert(ok);
            f.?.free();
            removed += 1;
            traversed += 1;
            f = next;
        }
        return removed;
    }

    /// Delete all the elements with score between min and max from the skiplist.
    fn deleteRangeByScore(self: *SkipListSet, range: Range) u64 {
        var update: [ZSKIPLIST_MAXLEVEL]*SkipList.Node = undefined;
        var x = self.sl.header;
        var i = self.sl.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward != null and
                if (range.minex)
                    x.level(i).forward.?.score <= range.min
                else
                    x.level(i).forward.?.score < range.min)
            {
                x = x.level(i).forward.?;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // Current node is the last with score < or <= min.
        var f = x.level(0).forward;

        // Delete nodes while in range.
        var removed: u64 = 0;
        while (f != null and
            if (range.maxex)
                f.?.score < range.max
            else
                f.?.score <= range.max)
        {
            const next = f.?.level(0).forward;
            self.sl.deleteNode(f.?, &update);
            const ok = self.dict.delete(f.?.ele.?);
            assert(ok);
            f.?.free();
            removed += 1;
            f = next;
        }
        return removed;
    }

    /// Delete all the elements with lex order between min and max from the skiplist.
    fn deleteRangeByLex(self: *SkipListSet, range: LexRange) u64 {
        var update: [ZSKIPLIST_MAXLEVEL]*SkipList.Node = undefined;
        var x = self.sl.header;
        var i = self.sl.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward != null and
                !range.valueGteMin(x.level(i).forward.?.ele.?))
            {
                x = x.level(i).forward.?;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // Current node is the last with lex order < or <= min.
        var f = x.level(0).forward;

        // Delete nodes while in range.
        var removed: u64 = 0;
        while (f != null and range.valueLteMax(f.?.ele.?)) {
            const next = f.?.level(0).forward;
            self.sl.deleteNode(f.?, &update);
            const ok = self.dict.delete(f.?.ele.?);
            assert(ok);
            f.?.free();
            removed += 1;
            f = next;
        }
        return removed;
    }

    pub fn cast(ptr: *anyopaque) *SkipListSet {
        return @ptrCast(@alignCast(ptr));
    }

    pub fn destroy(self: *SkipListSet) void {
        self.dict.destroy();
        self.sl.destroy();
        allocator.destroy(self);
    }
};

// Should be enough for 2^64 elements.
pub const ZSKIPLIST_MAXLEVEL = 64;

pub const SkipList = struct {
    header: *Node,
    tail: ?*Node,
    length: u64,
    level: u32,

    const SizedNode = struct {
        size: usize,
        node: Node,
    };

    pub const Node = struct {
        ele: ?sds.String,
        score: f64,
        backward: ?*Node,

        const Level = struct {
            forward: ?*Node,
            span: u64,
        };

        pub fn create(num_level: u32, score: f64, ele: ?sds.String) *Node {
            const mem_size: usize = @sizeOf(SizedNode) + num_level * @sizeOf(Level);
            const mem = allocator.alignedAlloc(
                u8,
                .of(SizedNode),
                mem_size,
            );
            const sno: *SizedNode = @ptrCast(@alignCast(mem));
            sno.size = mem_size;
            sno.node = .{
                .ele = ele,
                .score = score,
                .backward = null,
            };
            return &sno.node;
        }

        pub inline fn level(self: *const Node, i: usize) *Level {
            const offset = @sizeOf(Node) + @sizeOf(Level) * i;
            return @ptrFromInt(@intFromPtr(self) + offset);
        }

        /// Free the specified skiplist node. The referenced SDS string
        /// representation of the element is freed too, unless node.ele
        /// is set to NULL before calling this function.
        pub fn free(self: *Node) void {
            if (self.ele) |ele| {
                sds.free(allocator.child, ele);
            }
            const parent: *SizedNode = @fieldParentPtr("node", self);
            const mem: [*]align(@alignOf(SizedNode)) u8 = @ptrCast(@alignCast(parent));
            allocator.free(mem[0..parent.size]);
        }
    };

    pub fn create() *SkipList {
        const sl = allocator.create(SkipList);
        const header = Node.create(ZSKIPLIST_MAXLEVEL, 0, null);
        for (0..ZSKIPLIST_MAXLEVEL) |i| {
            const lvl = header.level(i);
            lvl.forward = null;
            lvl.span = 0;
        }
        sl.* = .{
            .header = header,
            .tail = null,
            .length = 0,
            .level = 1,
        };
        return sl;
    }

    /// Insert a new node in the skiplist. Assumes the element does not already
    /// exist (up to the caller to enforce that). The skiplist takes ownership
    /// of the passed SDS string 'ele'.
    pub fn insert(self: *SkipList, score: f64, ele: sds.String) *Node {
        assert(!isNan(score));

        var rank: [ZSKIPLIST_MAXLEVEL]u64 = undefined;
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            rank[i] = if (i == self.level - 1) 0 else rank[i + 1];
            while (x.level(i).forward) |forward| {
                if (forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele) == .lt)
                {
                    rank[i] += x.level(i).span;
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        const level = zslRandomLevel();
        if (level > self.level) {
            i = self.level;
            while (i < level) : (i += 1) {
                rank[i] = 0;
                update[i] = self.header;
                update[i].level(i).span = @intCast(self.length);
            }
            self.level = level;
        }

        x = Node.create(level, score, ele);
        i = 0;
        while (i < level) : (i += 1) {
            x.level(i).forward = update[i].level(i).forward;
            update[i].level(i).forward = x;

            x.level(i).span = update[i].level(i).span - (rank[0] - rank[i]);
            update[i].level(i).span = (rank[0] - rank[i]) + 1;
        }

        i = level;
        while (i < self.level) : (i += 1) {
            update[i].level(i).span += 1;
        }

        x.backward = if (update[0] == self.header) null else update[0];
        if (x.level(0).forward) |forward| {
            forward.backward = x;
        } else {
            self.tail = x;
        }
        self.length += 1;
        return x;
    }

    /// Update the score of an elmenent. Note that the element must exist and
    /// must match 'score'.
    ///
    /// Note that this function attempts to just update the node, in case after
    /// the score update, the node would be exactly at the same position.
    /// Otherwise the skiplist is modified by removing and re-adding a new
    /// element, which is more costly.
    ///
    /// The function returns the updated element skiplist node pointer.
    fn updateScore(
        self: *SkipList,
        curscore: f64,
        ele: sds.String,
        newscore: f64,
    ) *Node {
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;

        // We need to seek to element to update to start: this is useful anyway,
        // we'll have to update or remove it.
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (forward.score < curscore or
                    (forward.score == curscore and
                        sds.cmp(forward.ele.?, ele).compare(.lt)))
                {
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // Jump to our element: note that this function assumes that the
        // element with the matching score exists.
        x = x.level(0).forward.?;
        assert(curscore == x.score and sds.eql(x.ele.?, ele));

        // If the node, after the score update, would be still exactly
        // at the same position, we can just update the score without
        // actually removing and re-inserting the element in the skiplist.
        if ((x.backward == null or x.backward.?.score < newscore) and
            (x.level(0).forward == null or x.level(0).forward.?.score > newscore))
        {
            x.score = newscore;
            return x;
        }

        // No way to reuse the old node: we need to remove and insert a new
        // one at a different place.
        self.deleteNode(x, &update);
        const newnode = self.insert(newscore, x.ele.?);

        // We reused the old node x.ele SDS string, free the node now
        // since insert() created a new one.
        x.ele = null;
        x.free();

        return newnode;
    }

    /// Delete an element with matching score/element from the skiplist.
    /// The function returns TRUE if the node was found and deleted, otherwise
    /// FALSE is returned.
    pub fn delete(self: *SkipList, score: f64, ele: sds.String) bool {
        var update: [ZSKIPLIST_MAXLEVEL]*Node = undefined;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele) == .lt)
                {
                    x = forward;
                    continue;
                }
                break;
            }
            update[i] = x;
            if (i == 0) break;
        }

        // We may have multiple elements with the same score, what we need
        // is to find the element with both the right score and sds.
        if (x.level(0).forward) |forward| {
            if (score == forward.score and sds.eql(forward.ele.?, ele)) {
                self.deleteNode(forward, &update);
                forward.free();
                return true;
            }
        }
        return false;
    }

    /// Find the rank for an element by both score and key.
    /// Returns 0 when the element cannot be found, rank otherwise.
    /// Note that the rank is 1-based due to the span of SkipList.header
    /// to the first element.
    pub fn getRank(self: *const SkipList, score: f64, ele: sds.String) u64 {
        var rank: u64 = 0;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if ((forward.score < score or
                    forward.score == score and
                        sds.cmp(forward.ele.?, ele).compare(.lte)))
                {
                    rank += x.level(i).span;
                    x = forward;
                    continue;
                }
                break;
            }

            // x might be equal to self.header, so test if ele is non-NULL
            if (x.ele) |xe| if (sds.eql(xe, ele)) {
                return rank;
            };

            if (i == 0) break;
        }
        return rank;
    }

    /// Find the first node that is contained in the specified range.
    /// Returns NULL when no element is contained in the range.
    pub fn firstInRange(self: *const SkipList, range: Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (!range.valueGteMin(forward.score)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        // This is an inner range, so the next node cannot be NULL.
        x = x.level(0).forward.?;

        // Check if score <= max.
        if (!range.valueLteMax(x.score)) return null;
        return x;
    }

    /// Find the first node that is contained in the specified range.
    /// Returns NULL when no element is contained in the range.
    pub fn firstInLexRange(self: *const SkipList, range: LexRange) ?*Node {
        if (!self.isInLexRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (!range.valueGteMin(forward.ele.?)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        // This is an inner range, so the next node cannot be NULL.
        x = x.level(0).forward.?;

        // Check if lex order <= max.
        if (!range.valueLteMax(x.ele.?)) return null;
        return x;
    }

    /// Find the last node that is contained in the specified range.
    /// Returns NULL when no element is contained in the lex range.
    pub fn lastInRange(self: *const SkipList, range: Range) ?*Node {
        if (!self.isInRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (range.valueLteMax(forward.score)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        if (!range.valueGteMin(x.score)) return null;
        return x;
    }

    /// Find the last node that is contained in the specified lex range.
    /// Returns NULL when no element is contained in the lex range.
    pub fn lastInLexRange(self: *const SkipList, range: LexRange) ?*Node {
        if (!self.isInLexRange(range)) return null;

        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward) |forward| {
                if (range.valueLteMax(forward.ele.?)) {
                    x = forward;
                    continue;
                }
                break;
            }
            if (i == 0) break;
        }

        // Check if lex order >= min.
        if (!range.valueGteMin(x.ele.?)) return null;
        return x;
    }

    pub fn destroy(self: *SkipList) void {
        var node = self.header.level(0).forward;
        self.header.free();
        while (node) |n| {
            node = n.level(0).forward;
            n.free();
        }
        allocator.destroy(self);
    }

    /// Returns if there is a part of the skiplist is in range.
    fn isInRange(self: *const SkipList, range: Range) bool {
        if (range.min > range.max) return false;
        if (range.min == range.max and (range.minex or range.maxex)) {
            return false;
        }

        const last = self.tail;
        if (last == null or !range.valueGteMin(last.?.score)) return false;

        const first = self.header.level(0).forward;
        if (first == null or !range.valueLteMax(first.?.score)) return false;

        return true;
    }

    /// /// Returns if there is a part of the skiplist is in lex range.
    fn isInLexRange(self: *const SkipList, range: LexRange) bool {
        //  Test for ranges that will always be empty.
        const order = sds.cmplex(
            range.min.?,
            range.max.?,
            Server.shared.minstring,
            Server.shared.maxstring,
        );
        if (order == .gt or (order == .eq and (range.minex or range.maxex))) {
            return false;
        }

        const last = self.tail orelse {
            return false;
        };
        if (!range.valueGteMin(last.ele.?)) {
            return false;
        }

        const first = self.header.level(0).forward orelse {
            return false;
        };
        if (!range.valueLteMax(first.ele.?)) {
            return false;
        }

        return true;
    }

    fn deleteNode(self: *SkipList, x: *Node, update: []*Node) void {
        var i: u32 = 0;
        while (i < self.level) : (i += 1) {
            if (update[i].level(i).forward == x) {
                // update[i].level(i).span += x.level(i).span - 1;
                // This is for avoiding integer overflow because `x` maybe is
                // the last node with 0 span.
                update[i].level(i).span += x.level(i).span;
                update[i].level(i).span -= 1;
                update[i].level(i).forward = x.level(i).forward;
            } else {
                update[i].level(i).span -= 1;
            }
        }
        if (x.level(0).forward) |forward| {
            forward.backward = x.backward;
        } else {
            self.tail = x.backward;
        }

        while (self.level > 1 and
            self.header.level(self.level - 1).forward == null)
        {
            self.level -= 1;
        }
        self.length -= 1;
    }

    /// Finds an element by its rank. The rank argument needs to be 1-based.
    fn getElementByRank(self: *SkipList, rank: u64) ?*Node {
        var traversed: u64 = 0;
        var x = self.header;
        var i = self.level - 1;
        while (i >= 0) : (i -= 1) {
            while (x.level(i).forward != null and
                (traversed + x.level(i).span) <= rank)
            {
                traversed += x.level(i).span;
                x = x.level(i).forward.?;
            }
            if (traversed == rank) {
                return x;
            }
            if (i == 0) break;
        }
        return null;
    }
};

fn zslRandomLevel() u32 {
    const P = 0.25;
    var level: u32 = 1;
    while (random.int(u16) < @as(u16, @intFromFloat(P * 0xFFFF))) {
        level += 1;
    }
    return @min(level, ZSKIPLIST_MAXLEVEL);
}

const RangeType = enum {
    rank,
    score,
    lex,
};

/// Hold an inclusive/exclusive range by score comparison.
pub const Range = struct {
    min: f64,
    max: f64,

    // Are min or max exclusive?
    minex: bool,
    maxex: bool,

    pub fn valueGteMin(range: *const Range, value: f64) bool {
        return if (range.minex)
            value > range.min
        else
            value >= range.min;
    }

    pub fn valueLteMax(range: *const Range, value: f64) bool {
        return if (range.maxex)
            value < range.max
        else
            value <= range.max;
    }

    fn parse(min: *Object, max: *Object) ?Range {
        var range: Range = .{
            .min = 0,
            .max = 0,
            .minex = false,
            .maxex = false,
        };

        // Parse the min-max interval. If one of the values is prefixed
        // by the "(" character, it's considered "open". For instance
        // ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
        // ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max
        if (min.encoding == .int) {
            range.min = @floatFromInt(min.v.int);
        } else {
            const s = sds.castBytes(min.v.ptr);
            var i: usize = 0;
            if (s[0] == '(') {
                i = 1;
                range.minex = true;
            }
            range.min = std.fmt.parseFloat(f64, s[i..]) catch {
                return null;
            };
            if (isNan(range.min)) {
                return null;
            }
        }
        if (max.encoding == .int) {
            range.max = @floatFromInt(max.v.int);
        } else {
            const s = sds.castBytes(max.v.ptr);
            var i: usize = 0;
            if (s[0] == '(') {
                i = 1;
                range.maxex = true;
            }
            range.max = std.fmt.parseFloat(f64, s[i..]) catch {
                return null;
            };
            if (isNan(range.max)) {
                return null;
            }
        }

        return range;
    }
};

/// Hold an inclusive/exclusive range spec by lexicographic comparison.
pub const LexRange = struct {
    min: ?sds.String,
    max: ?sds.String,

    // Are min or max exclusive?
    minex: bool,
    maxex: bool,

    /// Parse the lex range according to the objects min and max.
    /// The returned structure must be freed with LexRange.free().
    pub fn parse(min: *Object, max: *Object) ?LexRange {
        // The range can't be valid if objects are integer encoded.
        // Every item must start with ( or [.
        if (min.encoding == .int or max.encoding == .int) {
            return null;
        }
        var range: LexRange = .{
            .min = null,
            .max = null,
            .minex = false,
            .maxex = false,
        };
        if (!parseItem(min, &range.min, &range.minex) or
            !parseItem(max, &range.max, &range.maxex))
        {
            range.free();
            return null;
        }
        return range;
    }

    /// Parse max or min argument of ZRANGEBYLEX.
    /// (foo means foo (open interval)
    /// [foo means foo (closed interval)
    /// - means the min string possible
    /// + means the max string possible
    ///
    /// If the string is valid the *dest pointer is set to the redis object
    /// that will be used for the comparison, and ex will be set to 0 or 1
    /// respectively if the item is exclusive or inclusive. TRUE will be
    /// returned.
    ///
    /// If the string is not a valid range FALSE is returned, and the value
    /// of *dest and *ex is undefined.
    fn parseItem(item: *Object, dest: *?sds.String, ex: *bool) bool {
        var c = sds.castBytes(item.v.ptr);

        switch (c[0]) {
            '+' => {
                if (c.len != 1) return false;
                ex.* = true;
                dest.* = Server.shared.maxstring;
                return true;
            },
            '-' => {
                if (c.len != 1) return false;
                ex.* = true;
                dest.* = Server.shared.minstring;
                return true;
            },
            '(' => {
                ex.* = true;
                dest.* = sds.new(allocator.child, c[1..]);
                return true;
            },
            '[' => {
                ex.* = false;
                dest.* = sds.new(allocator.child, c[1..]);
                return true;
            },
            else => return false,
        }
    }

    pub fn valueGteMin(self: *const LexRange, value: sds.String) bool {
        if (self.minex) {
            return sds.cmplex(
                value,
                self.min.?,
                Server.shared.minstring,
                Server.shared.maxstring,
            ).compare(.gt);
        }
        return sds.cmplex(
            value,
            self.min.?,
            Server.shared.minstring,
            Server.shared.maxstring,
        ).compare(.gte);
    }

    pub fn valueLteMax(self: *const LexRange, value: sds.String) bool {
        if (self.maxex) {
            return sds.cmplex(
                value,
                self.max.?,
                Server.shared.minstring,
                Server.shared.maxstring,
            ).compare(.lt);
        }
        return sds.cmplex(
            value,
            self.max.?,
            Server.shared.minstring,
            Server.shared.maxstring,
        ).compare(.lte);
    }

    /// Free a lex range structure, must be called only after LexRange.parse().
    pub fn free(self: *const LexRange) void {
        if (self.min != Server.shared.minstring and
            self.min != Server.shared.maxstring)
        {
            if (self.min) |s| {
                sds.free(allocator.child, s);
            }
        }
        if (self.max != Server.shared.maxstring and
            self.max != Server.shared.minstring)
        {
            if (self.max) |s| {
                sds.free(allocator.child, s);
            }
        }
    }
};

test zslRandomLevel {
    const level = zslRandomLevel();
    try expect(level >= 1);
}

test SkipList {
    var sl = SkipList.create();
    defer sl.destroy();

    var ele = sds.new(allocator.child, "score 1");
    try expect(sl.getRank(1, ele) == 0);
    const score1 = sl.insert(
        1,
        ele,
    );
    try expect(score1.backward == null);
    try expect(sl.length == 1);
    try expect(sl.getRank(1, ele) == 1);

    ele = sds.new(allocator.child, "score 2");
    var score2 = sl.insert(
        2,
        ele,
    );
    try expect(sl.header.level(0).forward.? == score1);
    try expect(sl.tail == score2);
    try expect(sl.length == 2);

    var range = Range{
        .min = 1,
        .max = 2,
        .minex = false,
        .maxex = false,
    };
    const first = sl.firstInRange(range);
    try expect(first != null);
    try expect(first.? == score1);
    var last = sl.lastInRange(range);
    try expect(last != null);
    try expect(last.? == score2);
    range.maxex = true;
    last = sl.lastInRange(range);
    try expect(last != null);
    try expect(last.? == score1);

    score2 = sl.insert(
        2,
        sds.new(allocator.child, "score 2.0"),
    );
    try expect(sl.tail == score2);
    try expect(sl.length == 3);

    var node = sl.getElementByRank(2);
    try expectEqualStrings(sds.asBytes(node.?.ele.?), "score 2");
    node = sl.getElementByRank(5);
    try expect(node == null);

    ele = sds.new(allocator.child, "deleted");
    _ = sl.insert(3, ele);
    try expect(sl.length == 4);
    const deleted = sl.delete(3, ele);
    try expect(deleted);
    try expect(sl.length == 3);
    try expect(sl.tail.?.score == 2);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;
const expectEqualStrings = testing.expectEqualStrings;
const assert = std.debug.assert;
const random = @import("random.zig");
const isNan = std.math.isNan;
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const sds = @import("sds.zig");
const Server = @import("Server.zig");
const dict = @import("dict.zig");
const hasher = @import("hasher.zig");
const Client = @import("networking.zig").Client;
const eqlCase = std.ascii.eqlIgnoreCase;
const Object = @import("Object.zig");
const server = &Server.instance;
const ZipList = @import("ZipList.zig");
const util = @import("util.zig");
const log = std.log.scoped(.zset);
const db = @import("db.zig");
