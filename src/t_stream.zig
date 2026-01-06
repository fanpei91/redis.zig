/// XADD key [MAXLEN [~|=] <count>] <ID or *> [field value] [field value] ...
pub fn xaddCommand(cli: *CLient) void {
    const argv = cli.argv.?;

    // Was an ID different than "*" specified?
    var id_given = false;
    // If left to -1 no trimming is performed.
    var maxlen: i64 = -1;
    // If true only delete whole radix tree nodes, so the maxium length is not
    // applied verbatim.
    var approx_maxlen: bool = false;

    // Parse options.
    //
    // This is the first argument position where we could
    // find an option, or the ID.
    var i: usize = 2;
    var id: Stream.Id = undefined;
    while (i < cli.argc) : (i += 1) {
        // Number of additional arguments.
        const moreargs = cli.argc - 1 - i;
        const opt = sds.castBytes(argv[i].v.ptr);
        if (opt[0] == '*' and opt.len == 1) {
            // This is just a fast path for the common case of auto-ID
            // creation.
            break;
        } else if (eqlCase(opt, "maxlen") and moreargs > 0) {
            approx_maxlen = false;
            const next = sds.castBytes(argv[i + 1].v.ptr);
            // Check for the form MAXLEN ~ <count>.
            if (moreargs >= 2 and next[0] == '~' and next.len == 1) {
                approx_maxlen = true;
                i += 1;
            } else if (moreargs >= 2 and next[0] == '=' and next.len == 1) {
                i += 1;
            }

            maxlen = argv[i + 1].getLongLongOrReply(cli, null) orelse {
                return;
            };
            if (maxlen < 0) {
                cli.addReplyErr("The MAXLEN argument must be >= 0.");
                return;
            }
            i += 1;
        } else {
            // If we are here is a syntax error or a valid ID.
            if (!Stream.Id.parseOrReply(cli, argv[i], &id, 0, true)) return;
            id_given = true;
            break;
        }
    }
    const field_pos = i + 1;

    // Check arity.
    if ((cli.argc - field_pos) < 2 or ((cli.argc - field_pos) % 2) == 1) {
        cli.addReplyErr("wrong number of arguments for XADD");
        return;
    }

    // Return ASAP if minimal ID (0-0) was given so we avoid possibly creating
    // a new stream and have streamAppendItem fail, leaving an empty key in the
    // database.
    if (id_given and id.ms == 0 and id.seq == 0) {
        cli.addReplyErr("The ID specified in XADD must be greater than 0-0");
        return;
    }

    const key = argv[1];
    const stream = Stream.lookupCreateOrReply(cli, key) orelse {
        return;
    };

    // Return ASAP if the stream has reached the last possible ID
    const last_id = stream.last_id;
    if (last_id.ms == maxInt(u64) and last_id.seq == maxInt(u64)) {
        cli.addReplyErr("The stream has exhausted the last possible ID, " ++
            "unable to add more items");
        return;
    }

    const fields = argv[field_pos..];
    stream.append(
        fields,
        @divExact(fields.len, 2),
        &id,
        if (id_given) &id else null,
    ) catch |err| {
        switch (err) {
            error.DOM => cli.addReplyErr("The ID specified in XADD is equal or " ++
                "smaller than the target stream top item"),
            error.RANGE => cli.addReplyErr("Elements are too large to be stored"),
        }
        return;
    };

    cli.addReplyStreamID(&id);

    cli.db.signalModifiedKey(key);
    server.dirty +%= 1;

    if (maxlen >= 0) {
        _ = stream.trim(@intCast(maxlen), approx_maxlen);
    }

    // We need to signal to blocked clients that there is new data on this
    // stream.
    blocked.signalKeyAsReady(cli.db, key);
}

/// XDEL <key> [<ID1> <ID2> ... <IDN>]
///
/// Removes the specified entries from the stream. Returns the number
/// of items actually deleted, that may be different from the number
/// of IDs passed in case certain IDs do not exist.
pub fn xdelCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const xobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    // We need to sanity check the IDs passed to start. Even if not
    // a big issue, it is not great that the command is only partially
    // executed because at some point an invalid ID is parsed.
    var id: Stream.Id = undefined;
    for (argv[2..cli.argc]) |arg| {
        if (!Stream.Id.parseOrReply(cli, arg, &id, 0, true)) {
            return;
        }
    }

    // Actually apply the command.
    const stream: *Stream = .cast(xobj.v.ptr);
    var deleted: i64 = 0;
    for (argv[2..cli.argc]) |arg| {
        assert(Stream.Id.parseOrReply(null, arg, &id, 0, true));
        deleted += @intFromBool(stream.delete(&id));
    }
    if (deleted > 0) {
        cli.db.signalModifiedKey(key);
        server.dirty +%= deleted;
    }
    cli.addReplyLongLong(deleted);
}

/// XLEN key
pub fn xlenCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const xobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    const s: *Stream = .cast(xobj.v.ptr);
    cli.addReplyLongLong(@bitCast(s.length));
}

/// XRANGE key start end [COUNT <n>]
pub fn xrangeCommand(cli: *CLient) void {
    xrange(cli, false);
}

/// XREVRANGE key end start [COUNT <n>]
pub fn xrevrangeCommand(cli: *CLient) void {
    xrange(cli, true);
}

fn xrange(cli: *CLient, rev: bool) void {
    const argv = cli.argv.?;

    var startid: Stream.Id = undefined;
    var endid: Stream.Id = undefined;
    const startarg = if (rev) argv[3] else argv[2];
    const endarg = if (rev) argv[2] else argv[3];
    if (!Stream.Id.parseOrReply(cli, startarg, &startid, 0, false)) {
        return;
    }
    if (!Stream.Id.parseOrReply(cli, endarg, &endid, maxInt(u64), false)) {
        return;
    }

    var count: i64 = -1;
    // Parse the COUNT option if any.
    if (cli.argc > 4) {
        var i: usize = 4;
        while (i < cli.argc) : (i += 1) {
            const additional = cli.argc - i - 1;
            if (eqlCase(sds.castBytes(argv[i].v.ptr), "COUNT") and
                additional >= 1)
            {
                count = argv[i + 1].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                if (count < 0) count = 0;
                i += 1; // Consume additional arg.
            } else {
                cli.addReply(Server.shared.syntaxerr);
                return;
            }
        }
    }

    const key = argv[1];
    const xobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.nullmultibulk,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    if (count == 0) {
        cli.addReply(Server.shared.nullmultibulk);
        return;
    }
    if (count == -1) count = 0;
    const stream: *Stream = .cast(xobj.v.ptr);
    _ = stream.replyWithRange(
        cli,
        &startid,
        &endid,
        @intCast(count),
        rev,
        null,
        null,
        0,
        null,
    );
}

// XTRIM <key> [... options ...]
// List of options:
// MAXLEN [~|=] <count>     -- Trim so that the stream will be capped at
//                             the specified length. Use ~ before the
//                             count in order to demand approximated trimming
//                             (like XADD MAXLEN option).
const TRIM_STRATEGY_NONE = 0;
const TRIM_STRATEGY_MAXLEN = 1;
pub fn xtrimCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];

    // If the key does not exist, we are ok returning zero, that is, the
    // number of elements removed from the stream.
    const xobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    const stream: *Stream = .cast(xobj.v.ptr);
    var trim_strategy: i32 = TRIM_STRATEGY_NONE;
    // If left to -1 no trimming is performed.
    var maxlen: i64 = -1;
    // If TRUE only delete whole radix tree nodes, so
    // the maxium length is not applied verbatim.
    var approx_maxlen = false;

    // Parse options.
    var i: usize = 2; // Start of options.
    while (i < cli.argc) : (i += 1) {
        const moreargs = cli.argc - 1 - i;
        const opt = sds.castBytes(argv[i].v.ptr);
        if (eqlCase(opt, "MAXLEN") and moreargs > 0) {
            trim_strategy = TRIM_STRATEGY_MAXLEN;
            const next = sds.castBytes(argv[i + 1].v.ptr);
            // Check for the form MAXLEN ~ <count>.
            if (moreargs >= 2 and next[0] == '~' and next.len == 1) {
                approx_maxlen = true;
                i += 1;
            } else if (moreargs >= 2 and next[0] == '=' and next.len == 1) {
                i += 1;
            }
            maxlen = argv[i + 1].getLongLongOrReply(cli, null) orelse {
                return;
            };
            if (maxlen < 0) {
                cli.addReplyErr("The MAXLEN argument must be >= 0.");
                return;
            }
            i += 1;
        } else {
            cli.addReply(Server.shared.syntaxerr);
            return;
        }
    }

    // Perform the trimming.
    var deleted: u64 = 0;
    if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
        deleted = stream.trim(@intCast(maxlen), approx_maxlen);
    } else {
        cli.addReplyErr("XTRIM called without an option to trim the stream");
        return;
    }
    if (deleted > 0) {
        cli.db.signalModifiedKey(key);
        server.dirty +%= @intCast(deleted);
    }
    cli.addReplyLongLong(@intCast(deleted));
}

const XREAD_BLOCKED_DEFAULT_COUNT = 1000;
/// XREAD [BLOCK <milliseconds>] [COUNT <count>] STREAMS key_1 key_2 ... key_N
///       ID_1 ID_2 ... ID_N
///
/// This function also implements the XREADGROUP command, which is like XREAD
/// but accepting the [GROUP group-name consumer-name] additional option.
/// This is useful because while XREAD is a read command and can be called
/// on slaves, XREADGROUP is not.
pub fn xreadCommand(cli: *CLient) void {
    const argv = cli.argv.?;

    var timeout: i64 = -1; // -1 means, no BLOCK argument given.
    var count: i64 = 0;
    var noack = false; // rue if NOACK option was specified.

    var streams_count: usize = 0;
    var streams_arg: usize = 0;

    var static_ids: [8]Stream.Id = undefined;
    var ids: []Stream.Id = &static_ids;

    // XREAD or XREADGROUP?
    const xreadgroup = sds.getLen(sds.cast(argv[0].v.ptr)) == 10;
    var groupname: ?*Object = null;
    var consumername: ?*Object = null;
    var groups: ?[]*Stream.CG = null;

    // Parse arguments
    {
        var i: usize = 1;
        while (i < cli.argc) : (i += 1) {
            const moreargs = cli.argc - 1 - i;
            const opt = sds.castBytes(argv[i].v.ptr);
            if (eqlCase(opt, "BLOCK") and moreargs >= 1) {
                i += 1;
                timeout = blocked.getTimeoutFromObjectOrReply(
                    cli,
                    argv[i],
                    Server.UNIT_MILLISECONDS,
                ) orelse {
                    return;
                };
            } else if (eqlCase(opt, "COUNT") and moreargs >= 1) {
                i += 1;
                count = argv[i].getLongLongOrReply(cli, null) orelse {
                    return;
                };
                if (count < 0) count = 0;
            } else if (eqlCase(opt, "STREAMS") and moreargs >= 1) {
                streams_arg = i + 1;
                streams_count = cli.argc - streams_arg;
                if (streams_count % 2 != 0) {
                    cli.addReplyErr("Unbalanced XREAD list of streams: " ++
                        "for each stream key an ID or '$' must be " ++
                        "specified.");
                    return;
                }
                // We have two arguments for each stream.
                streams_count = @divExact(streams_count, 2);
                break;
            } else if (eqlCase(opt, "GROUP") and moreargs >= 2) {
                if (!xreadgroup) {
                    cli.addReplyErr("The GROUP option is only supported by " ++
                        "XREADGROUP. You called XREAD instead.");
                    return;
                }
                groupname = argv[i + 1];
                consumername = argv[i + 2];
                i += 2;
            } else if (eqlCase(opt, "NOACK")) {
                if (!xreadgroup) {
                    cli.addReplyErr("The NOACK option is only supported by " ++
                        "XREADGROUP. You called XREAD instead.");
                    return;
                }
                noack = true;
            } else {
                cli.addReply(Server.shared.syntaxerr);
                return;
            }
        }
    }

    // STREAMS option is mandatory.
    if (streams_count == 0) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    // If the user specified XREADGROUP then it must also
    // provide the GROUP option.
    if (xreadgroup and groupname == null) {
        cli.addReplyErr("Missing GROUP option for XREADGROUP");
        return;
    }

    // Parse the IDs and resolve the group name.
    biz: {
        if (streams_count > static_ids.len) {
            ids = allocator.alloc(Stream.Id, streams_count);
        }
        if (groupname != null) {
            groups = allocator.alloc(*Stream.CG, streams_count);
        }
        for (streams_arg + streams_count..cli.argc) |i| {
            // Specifying "$" as last-known-id means that the client wants to be
            // served with just the messages that will arrive into the stream
            // starting from now.
            const id_idx = i - streams_arg - streams_count;
            const key = argv[i - streams_count];
            const xobj = cli.db.lookupKeyRead(key);
            var group: ?*Stream.CG = null;
            if (xobj) |obj| {
                if (obj.checkTypeOrReply(cli, .stream)) {
                    break :biz;
                }
                if (groupname) |gp| {
                    const name = sds.cast(gp.v.ptr);
                    group = Stream.cast(obj.v.ptr).lookupCG(name);
                }
            }

            // If a group was specified, than we need to be sure that the
            // key and group actually exist.
            if (groupname != null) {
                if (xobj == null or group == null) {
                    cli.addReplyErrFormat(
                        "-NOGROUP No such key '{s}' or consumer " ++
                            "group '{s}' in XREADGROUP with GROUP " ++
                            "option",
                        .{
                            sds.castBytes(key.v.ptr),
                            sds.castBytes(groupname.?.v.ptr),
                        },
                    );
                    break :biz;
                }
                groups.?[id_idx] = group.?;
            }

            const idarg = argv[i];
            if (eqlCase(sds.castBytes(idarg.v.ptr), "$")) {
                if (xreadgroup) {
                    cli.addReplyErr("The $ ID is meaningless in the context of " ++
                        "XREADGROUP: you want to read the history of " ++
                        "this consumer by specifying a proper ID, or " ++
                        "use the > ID to get new messages. The $ ID would " ++
                        "just return an empty result set.");
                    break :biz;
                }
                if (xobj) |obj| {
                    ids[id_idx] = Stream.cast(obj.v.ptr).last_id;
                } else {
                    ids[id_idx].ms = 0;
                    ids[id_idx].seq = 0;
                }
                continue;
            } else if (eqlCase(sds.castBytes(idarg.v.ptr), ">")) {
                if (!xreadgroup) {
                    cli.addReplyErr("The > ID can be specified only when calling " ++
                        "XREADGROUP using the GROUP <group> " ++
                        "<consumer> option.");
                    break :biz;
                }
                // We use just the maximum ID to signal this is a ">" ID, anyway
                // the code handling the blocking clients will have to update the
                // ID later in order to match the changing consumer group last ID.
                ids[id_idx].ms = maxInt(u64);
                ids[id_idx].seq = maxInt(u64);
                continue;
            }
            if (!Stream.Id.parseOrReply(cli, idarg, &ids[id_idx], 0, true)) {
                break :biz;
            }
        }

        // Try to serve the client synchronously.
        var arraylen: i64 = 0;
        var replylen: ?*CLient.ReplyBlock = null;
        for (0..streams_count) |i| {
            const xobj = cli.db.lookupKeyRead(argv[streams_arg + i]) orelse {
                continue;
            };
            const stream: *Stream = .cast(xobj.v.ptr);
            const gt = &ids[i]; // ID must be greater than this.
            var serve_synchronously = false;
            var serve_history = false; // True for XREADGROUP with ID != ">".

            // Check if there are the conditions to serve the client
            // synchronously.
            if (groups) |grps| {
                // If the consumer is blocked on a group, we always serve it
                // synchronously (serving its local history) if the ID specified
                // was not the special ">" ID.
                if (gt.ms != maxInt(u64) or gt.seq != maxInt(u64)) {
                    serve_synchronously = true;
                    serve_history = true;
                } else if (stream.length > 0) {
                    // We also want to serve a consumer in a consumer group
                    // synchronously in case the group top item delivered is
                    // smaller than what the stream has inside.
                    const last = &grps[i].last_id;
                    var maxid: Stream.Id = undefined;
                    stream.lastValidId(&maxid);
                    if (maxid.compare(last) == .gt) {
                        serve_synchronously = true;
                        gt.* = last.*;
                    }
                }
            } else if (stream.length > 0) {
                // For consumers without a group, we serve synchronously if we can
                // actually provide at least one item from the stream.
                var maxid: Stream.Id = undefined;
                stream.lastValidId(&maxid);
                if (maxid.compare(gt) == .gt) {
                    serve_synchronously = true;
                }
            }

            if (serve_synchronously) {
                arraylen += 1;
                if (arraylen == 1) {
                    replylen = cli.addDeferredMultiBulkLength();
                }
                // Stream.replyWithRange() handles the 'start' ID as inclusive,
                // so start from the next ID, since we want only messages with
                // IDs greater than start.
                var start = gt.*;
                start.incr();

                // Emit the two elements sub-array consisting of the name
                // of the stream and the data we extracted from it.
                cli.addReplyMultiBulkLen(2);
                cli.addReplyBulk(argv[streams_arg + i]);
                var consumer: ?*Stream.Consumer = null;
                if (groups) |grps| {
                    consumer = grps[i].lookupConsumer(
                        sds.cast(consumername.?.v.ptr),
                        Stream.SLC_NONE,
                    );
                }

                const spi: ?*Stream.PropInfo = null; // TODO: PropInfo
                var flags: i32 = 0;
                if (noack) flags |= Stream.RWR_NOACK;
                if (serve_history) flags |= Stream.RWR_HISTORY;
                _ = stream.replyWithRange(
                    cli,
                    &start,
                    null,
                    @intCast(count),
                    false,
                    if (groups != null) groups.?[i] else null,
                    consumer,
                    flags,
                    spi,
                );
                if (groups != null) server.dirty +%= 1;
            }
        }

        // We replied synchronously! Set the top array len and return to caller.
        if (arraylen > 0) {
            cli.setDeferredMultiBulkLength(replylen, arraylen);
            break :biz;
        }

        // Block if needed.
        if (timeout != -1) {
            // If we are inside a MULTI/EXEC and the list is empty the only thing
            // we can do is treating it as a timeout (even with timeout 0).
            if (cli.flags & Server.CLIENT_MULTI != 0) {
                cli.addReply(Server.shared.nullmultibulk);
                break :biz;
            }
            blocked.blockForKeys(
                cli,
                Server.BLOCKED_STREAM,
                argv[streams_arg .. streams_arg + streams_count],
                timeout,
                null,
                ids,
            );

            // If no COUNT is given and we block, set a relatively small count:
            // in case the ID provided is too low, we do not want the server to
            // block just to serve this client a huge stream of messages.
            cli.bpop.xread_count = if (count > 0)
                @intCast(count)
            else
                XREAD_BLOCKED_DEFAULT_COUNT;
            if (groupname != null) {
                _ = groupname.?.incrRefCount();
                _ = consumername.?.incrRefCount();
                cli.bpop.xread_group = groupname;
                cli.bpop.xread_consumer = consumername;
                cli.bpop.xread_group_noack = noack;
            } else {
                cli.bpop.xread_group = null;
                cli.bpop.xread_consumer = null;
            }
            break :biz;
        }

        // No BLOCK option, nor any stream we can serve. Reply as with a
        // timeout happened.
        cli.addReply(Server.shared.nullmultibulk);
    }

    // cleanup
    if (groups) |grps| allocator.free(grps);
    if (!std.meta.eql(ids, &static_ids)) allocator.free(ids);
}

/// XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM]
/// XGROUP SETID <key> <groupname> <id or $>
/// XGROUP DESTROY <key> <groupname>
/// XGROUP DELCONSUMER <key> <groupname> <consumername>
/// XGROUP HELP
pub fn xgroupCommand(cli: *CLient) void {
    const help: []const []const u8 = &.{
        "CREATE      <key> <groupname> <id or $> [opt] -- Create a new consumer group.",
        "            option MKSTREAM: create the empty stream if it does not exist.",
        "SETID       <key> <groupname> <id or $>  -- Set the current group ID.",
        "DESTROY     <key> <groupname>            -- Remove the specified group.",
        "DELCONSUMER <key> <groupname> <consumer> -- Remove the specified consumer.",
        "HELP                                     -- Prints this help.",
    };

    const argv = cli.argv.?;
    const opt = sds.castBytes(argv[1].v.ptr);
    var groupname: sds.String = undefined;

    // CREATE has an MKSTREAM option that creates the stream if it
    // does not exist.
    var mkstream = false;
    if (cli.argc == 6 and eqlCase(opt, "CREATE")) {
        if (!eqlCase(sds.castBytes(argv[5].v.ptr), "MKSTREAM")) {
            cli.addReplySubcommandSyntaxError();
            return;
        }
        mkstream = true;
    }

    var stream: ?*Stream = null;
    // Everything but the "HELP" option requires a key and group name.
    if (cli.argc >= 4) {
        if (cli.db.lookupKeyWrite(argv[2])) |xobj| {
            if (xobj.checkTypeOrReply(cli, .stream)) {
                return;
            }
            stream = Stream.cast(xobj.v.ptr);
        }
        groupname = sds.cast(argv[3].v.ptr);
    }

    // Check for missing key/group.
    var cg: ?*Stream.CG = null;
    if (cli.argc >= 4 and !mkstream) {
        // At this point key must exist, or there is an error.
        if (stream == null) {
            cli.addReplyErr("The XGROUP subcommand requires the key to exist. " ++
                "Note that for CREATE you may want to use the MKSTREAM " ++
                "option to create an empty stream automatically.");
            return;
        }
        cg = stream.?.lookupCG(groupname);
        if (cg == null and (eqlCase(opt, "SETID") or eqlCase(opt, "DELCONSUMER"))) {
            cli.addReplyErrFormat(
                "-NOGROUP No such consumer group '{s}' for key name '{s}'",
                .{
                    sds.asBytes(groupname),
                    sds.castBytes(argv[2].v.ptr),
                },
            );
            return;
        }
    }

    // Dispatch the different subcommands.
    if (eqlCase(opt, "CREATE") and (cli.argc == 5 or cli.argc == 6)) {
        var id: Stream.Id = undefined;
        if (eqlCase(sds.castBytes(argv[4].v.ptr), "$")) {
            if (stream) |s| {
                id = s.last_id;
            } else {
                id.ms = 0;
                id.seq = 0;
            }
        } else if (!Stream.Id.parseOrReply(cli, argv[4], &id, 0, true)) {
            return;
        }
        // Handle the MKSTREAM option now that the command can no longer fail.
        if (stream == null) {
            assert(mkstream);
            const xobj = Object.createStream();
            defer xobj.decrRefCount();
            cli.db.add(argv[2], xobj);
            stream = Stream.cast(xobj.v.ptr);
        }

        cg = stream.?.createCG(sds.asBytes(groupname), &id);
        if (cg != null) {
            cli.addReply(Server.shared.ok);
            server.dirty +%= 1;
        } else {
            cli.addReplyBulkString("-BUSYGROUP Consumer Group name already exists");
        }
    } else if (eqlCase(opt, "SETID") and cli.argc == 5) {
        var id: Stream.Id = undefined;
        if (eqlCase(sds.castBytes(argv[4].v.ptr), "$")) {
            id = stream.?.last_id;
        } else if (!Stream.Id.parseOrReply(cli, argv[4], &id, 0, true)) {
            return;
        }
        cg.?.last_id = id;
        cli.addReply(Server.shared.ok);
        server.dirty +%= 1;
    } else if (eqlCase(opt, "DESTROY") and cli.argc == 4) {
        if (cg) |group| {
            _ = raxlib.raxRemove(
                stream.?.cgroups.?,
                groupname,
                sds.getLen(groupname),
                null,
            );
            group.destroy();
            cli.addReply(Server.shared.cone);
            server.dirty +%= 1;
        } else {
            cli.addReply(Server.shared.czero);
        }
    } else if (eqlCase(opt, "DELCONSUMER") and cli.argc == 5) {
        // Delete the consumer and returns the number of pending messages
        // that were yet associated with such a consumer.
        const pending = cg.?.delConsumer(sds.cast(argv[4].v.ptr));
        cli.addReplyLongLong(@intCast(pending));
        server.dirty +%= 1;
    } else if (eqlCase(opt, "HELP")) {
        cli.addReplyHelp(help);
    } else {
        cli.addReplySubcommandSyntaxError();
    }
}

/// XACK <key> <group> <id> <id> ... <id>
///
/// Acknowledge a message as processed. In practical terms we just check the
/// pendine entries list (PEL) of the group, and delete the PEL entry both from
/// the group and the consumer (pending messages are referenced in both places).
///
/// Return value of the command is the number of messages successfully
/// acknowledged, that is, the IDs we were actually able to resolve in the PEL.
pub fn xackCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const xobj = cli.db.lookupKeyReadOrReply(
        cli,
        key,
        Server.shared.czero,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    const stream: *Stream = .cast(xobj.v.ptr);
    const cg = stream.lookupCG(sds.cast(argv[2])) orelse {
        cli.addReply(Server.shared.czero);
        return;
    };

    var acknowledged: i64 = 0;
    for (argv[3..cli.argc]) |arg| {
        var id: Stream.Id = undefined;
        if (!Stream.Id.parseOrReply(cli, arg, &id, 0, true)) {
            return;
        }

        var buf: [@sizeOf(Stream.Id)]u8 = undefined;
        id.decode(&buf);

        // Lookup the ID in the group PEL: it will have a reference to the
        // NACK structure that will have a reference to the consumer, so that
        // we are able to remove the entry from both PELs.
        const found = raxlib.raxFind(cg.pel, &buf, buf.len);
        if (found != raxlib.raxNotFound) {
            const nack: *Stream.NACK = .cast(found.?);
            _ = raxlib.raxRemove(cg.pel, &buf, buf.len, null);
            _ = raxlib.raxRemove(nack.consumer.?.pel, &buf, buf.len, null);
            nack.destroy();
            acknowledged += 1;
            server.dirty +%= 1;
        }
    }
    cli.addReplyLongLong(acknowledged);
}

/// XPENDING <key> <group> [<start> <stop> <count> [<consumer>]]
///
/// If start and stop are omitted, the command just outputs information about
/// the amount of pending messages for the key/group pair, together with
/// the minimum and maxium ID of pending messages.
///
/// If start and stop are provided instead, the pending messages are returned
/// with informations about the current owner, number of deliveries and last
/// delivery time and so forth.
pub fn xpendingCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    // Without the range just outputs general
    // informations about the PEL.
    const justinfo = cli.argc == 3;

    const key = argv[1];
    const groupname = argv[2];
    const consumername = if (cli.argc == 7) argv[6] else null;

    var startid: Stream.Id = undefined;
    var endid: Stream.Id = undefined;
    var count: i64 = undefined;

    // Start and stop, and the consumer, can be omitted.
    if (cli.argc != 3 and cli.argc != 6 and cli.argc != 7) {
        cli.addReply(Server.shared.syntaxerr);
        return;
    }

    // Parse start/end/count arguments ASAP if needed, in order to report
    // syntax errors before any other error.
    if (cli.argc >= 6) {
        count = argv[5].getLongLongOrReply(cli, null) orelse {
            return;
        };
        if (count < 0) count = 0;
        if (!Stream.Id.parseOrReply(cli, argv[3], &startid, 0, false)) {
            return;
        }
        if (!Stream.Id.parseOrReply(cli, argv[4], &endid, maxInt(u64), false)) {
            return;
        }
    }

    // Lookup the key and the group inside the stream.
    var group: ?*Stream.CG = null;
    const xobj = cli.db.lookupKeyRead(key);
    if (xobj) |o| {
        if (o.checkTypeOrReply(cli, .stream)) {
            return;
        }
        const stream: *Stream = .cast(o.v.ptr);
        group = stream.lookupCG(sds.cast(groupname.v.ptr));
    }
    if (xobj == null or group == null) {
        cli.addReplyErrFormat(
            "-NOGROUP No such key '{s}' or consumer group '{s}'",
            .{
                sds.castBytes(key.v.ptr),
                sds.castBytes(groupname.v.ptr),
            },
        );
        return;
    }

    // XPENDING <key> <group> variant.
    if (justinfo) {
        cli.addReplyMultiBulkLen(4);
        // Total number of messages in the PEL.
        const pel_len: i64 = @intCast(raxlib.raxSize(group.?.pel));
        cli.addReplyLongLong(pel_len);
        // First and last IDs.
        if (pel_len == 0) {
            cli.addReply(Server.shared.nullbulk); // Start
            cli.addReply(Server.shared.nullbulk); // End;
            cli.addReply(Server.shared.nullmultibulk); // Consumers.
        } else {
            var ri: raxlib.raxIterator = undefined;
            raxlib.raxStart(&ri, group.?.pel);

            // Start
            _ = raxlib.raxSeek(&ri, "^", null, 0);
            _ = raxlib.raxNext(&ri);
            startid.decode(ri.key[0..ri.key_len]);
            cli.addReplyStreamID(&startid);

            // End
            _ = raxlib.raxSeek(&ri, "$", null, 0);
            _ = raxlib.raxNext(&ri);
            endid.decode(ri.key[0..ri.key_len]);
            cli.addReplyStreamID(&endid);
            raxlib.raxStop(&ri);

            // Consumers with pending messages.
            raxlib.raxStart(&ri, group.?.consumers);
            _ = raxlib.raxSeek(&ri, "^", null, 0);
            const replylen = cli.addDeferredMultiBulkLength();
            var arraylen: i64 = 0;
            while (raxlib.raxNext(&ri) != 0) {
                const consumer: *Stream.Consumer = .cast(ri.data.?);
                if (raxlib.raxSize(consumer.pel) == 0) continue;
                cli.addReplyMultiBulkLen(2);
                cli.addReplyBulkString(ri.key[0..ri.key_len]); // Name
                cli.addReplyLongLong(@intCast(raxlib.raxSize(consumer.pel)));
                arraylen += 1;
            }
            cli.setDeferredMultiBulkLength(replylen, arraylen);
            raxlib.raxStop(&ri);
        }
    }
    // XPENDING <key> <group> <start> <stop> <count> [<consumer>] variant.
    else {
        var consumer: ?*Stream.Consumer = null;
        if (consumername) |name| {
            consumer = group.?.lookupConsumer(
                sds.cast(name.v.ptr),
                Stream.SLC_NOCREAT | Stream.SLC_NOREFRESH,
            );
            // If a consumer name was mentioned but it does not exist, we can
            // just return an empty array.
            if (consumer == null) {
                cli.addReplyMultiBulkLen(0);
                return;
            }
        }
        const pel = if (consumer != null) consumer.?.pel else group.?.pel;
        var startkey: [@sizeOf(Stream.Id)]u8 = undefined;
        var endkey: [@sizeOf(Stream.Id)]u8 = undefined;
        startid.encode(&startkey);
        endid.encode(&endkey);

        const now = std.time.milliTimestamp();
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, pel);
        defer raxlib.raxStop(&ri);

        _ = raxlib.raxSeek(&ri, ">=", &startkey, startkey.len);
        const replylen = cli.addDeferredMultiBulkLength();
        var arraylen: i64 = 0;
        while (count > 0 and raxlib.raxNext(&ri) != 0) {
            if (memcmp(ri.key[0..ri.key_len], &endkey) == .gt) break;

            count -= 1;
            arraylen += 1;
            cli.addReplyMultiBulkLen(4);

            // Entry ID.
            var id: Stream.Id = undefined;
            id.decode(ri.key[0..ri.key_len]);
            cli.addReplyStreamID(&id);

            const nack: *Stream.NACK = .cast(ri.data.?);
            // Consumer name.
            cli.addReplyBulkString(sds.asBytes(nack.consumer.?.name));

            // Milliseconds elapsed since last delivery.
            var elapsed = now - nack.delivery_time;
            if (elapsed < 0) elapsed = 0;
            cli.addReplyLongLong(elapsed);

            // Number of deliveries.
            cli.addReplyLongLong(@intCast(nack.delivery_count));
        }
        cli.setDeferredMultiBulkLength(replylen, arraylen);
    }
}

/// XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
///        [IDLE <milliseconds>] [TIME <mstime>] [RETRYCOUNT <count>]
///        [FORCE] [JUSTID]
///
/// Gets ownership of one or multiple messages in the Pending Entries List
/// of a given stream consumer group.
///
/// If the message ID (among the specified ones) exists, and its idle
/// time greater or equal to <min-idle-time>, then the message new owner
/// becomes the specified <consumer>. If the minimum idle time specified
/// is zero, messages are claimed regardless of their idle time.
///
/// All the messages that cannot be found inside the pending entries list
/// are ignored, but in case the FORCE option is used. In that case we
/// create the NACK (representing a not yet acknowledged message) entry in
/// the consumer group PEL.
///
/// This command creates the consumer as side effect if it does not yet
/// exists. Moreover the command reset the idle time of the message to 0,
/// even if by using the IDLE or TIME options, the user can control the
/// new idle time.
///
/// The options at the end can be used in order to specify more attributes
/// to set in the representation of the pending message:
///
/// 1. IDLE <ms>:
///      Set the idle time (last time it was delivered) of the message.
///      If IDLE is not specified, an IDLE of 0 is assumed, that is,
///      the time count is reset because the message has now a new
///      owner trying to process it.
///
/// 2. TIME <ms-unix-time>:
///      This is the same as IDLE but instead of a relative amount of
///      milliseconds, it sets the idle time to a specific unix time
///      (in milliseconds). This is useful in order to rewrite the AOF
///      file generating XCLAIM commands.
///
/// 3. RETRYCOUNT <count>:
///      Set the retry counter to the specified value. This counter is
///      incremented every time a message is delivered again. Normally
///      XCLAIM does not alter this counter, which is just served to clients
///      when the XPENDING command is called: this way clients can detect
///      anomalies, like messages that are never processed for some reason
///      after a big number of delivery attempts.
///
/// 4. FORCE:
///      Creates the pending message entry in the PEL even if certain
///      specified IDs are not already in the PEL assigned to a different
///      client. However the message must be exist in the stream, otherwise
///      the IDs of non existing messages are ignored.
///
/// 5. JUSTID:
///      Return just an array of IDs of messages successfully claimed,
///      without returning the actual message.
///
/// 6. LASTID <id>:
///      Update the consumer group last ID with the specified ID if the
///      current last ID is smaller than the provided one.
///      This is used for replication / AOF, so that when we read from a
///      consumer group, the XCLAIM that gets propagated to give ownership
///      to the consumer, is also used in order to update the group current
///      ID.
///
/// The command returns an array of messages that the user
/// successfully claimed, so that the caller is able to understand
/// what messages it is now in charge of.
pub fn xclaimCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const groupname = sds.cast(argv[2].v.ptr);

    var retrycount: i64 = -1; // -1 means RETRYCOUNT option not given.
    var deliverytime: i64 = -1; // -1 means IDLE/TIME options not given.
    var force = false;
    var justid = false;

    var stream: ?*Stream = null;
    var group: ?*Stream.CG = null;
    const xobj = cli.db.lookupKeyRead(key);
    if (xobj) |obj| {
        if (obj.checkTypeOrReply(cli, .stream)) {
            return;
        }
        stream = .cast(obj.v.ptr);
        group = stream.?.lookupCG(groupname);
    }
    // No key or group? Send an error given that the group creation
    // is mandatory.
    if (stream == null or group == null) {
        cli.addReplyErrFormat(
            "-NOGROUP No such key '{s}' or " ++
                "consumer group '{s}'",
            .{
                sds.castBytes(key.v.ptr),
                sds.asBytes(groupname),
            },
        );
        return;
    }

    var minidle = argv[4].getLongLongOrReply(
        cli,
        "Invalid min-idle-time argument for XCLAIM",
    ) orelse {
        return;
    };
    if (minidle < 0) minidle = 0;

    // Start parsing the IDs, so that we abort ASAP if there is a syntax
    // error: the return value of this command cannot be an error in case
    // the client successfully claimed some message, so it should be
    // executed in a "all or nothing" fashion.
    var j: usize = 5;
    while (j < cli.argc) : (j += 1) {
        var id: Stream.Id = undefined;
        if (!Stream.Id.parseOrReply(null, argv[j], &id, 0, true)) {
            break;
        }
    }
    // Next time we iterate the IDs we now the range.
    const last_id_arg = j - 1;

    // If we stopped because some IDs cannot be parsed, perhaps they
    // are trailing options.
    const now = std.time.milliTimestamp();
    var last_id: Stream.Id = .{ .ms = 0, .seq = 0 };
    while (j < cli.argc) : (j += 1) {
        const moreargs = cli.argc - 1 - j; //Number of additional arguments.
        const opt = sds.castBytes(argv[j].v.ptr);
        if (eqlCase(opt, "FORCE")) {
            force = true;
        } else if (eqlCase(opt, "JUSTID")) {
            justid = true;
        } else if (eqlCase(opt, "IDLE") and moreargs >= 1) {
            j += 1;
            deliverytime = argv[j].getLongLongOrReply(
                cli,
                "Invalid IDLE option argument for XCLAIM",
            ) orelse {
                return;
            };
            deliverytime = now - deliverytime;
        } else if (eqlCase(opt, "TIME") and moreargs >= 1) {
            j += 1;
            deliverytime = argv[j].getLongLongOrReply(
                cli,
                "Invalid TIME option argument for XCLAIM",
            ) orelse {
                return;
            };
        } else if (eqlCase(opt, "RETRYCOUNT") and moreargs >= 1) {
            j += 1;
            retrycount = argv[j].getLongLongOrReply(
                cli,
                "Invalid RETRYCOUNT option argument for XCLAIM",
            ) orelse {
                return;
            };
        } else if (eqlCase(opt, "LASTID") and moreargs >= 1) {
            j += 1;
            if (!Stream.Id.parseOrReply(cli, argv[j], &last_id, 0, true)) {
                return;
            }
        } else {
            cli.addReplyErrFormat("Unrecognized XCLAIM option '{s}'", .{opt});
            return;
        }
    }

    if (last_id.compare(&group.?.last_id) == .gt) {
        group.?.last_id = last_id;
    }

    if (deliverytime != -1) {
        // If a delivery time was passed, either with IDLE or TIME, we
        // do some sanity check on it, and set the deliverytime to now
        // (which is a sane choice usually) if the value is bogus.
        // To raise an error here is not wise because clients may compute
        // the idle time doing some math starting from their local time,
        // and this is not a good excuse to fail in case, for instance,
        // the computer time is a bit in the future from our POV.
        if (deliverytime < 0 or deliverytime > now) {
            deliverytime = now;
        }
    } else {
        // If no IDLE/TIME option was passed, we want the last delivery
        // time to be now, so that the idle time of the message will be
        // zero.
        deliverytime = now;
    }

    // Do the actual claiming.
    var consumer: ?*Stream.Consumer = null;
    const replylen = cli.addDeferredMultiBulkLength();
    var arraylen: i64 = 0;
    j = 5;
    while (j <= last_id_arg) : (j += 1) {
        var id: Stream.Id = undefined;
        if (!Stream.Id.parseOrReply(cli, argv[j], &id, 0, true)) {
            @panic("StreamID invalid after check. Should not be possible.");
        }
        var idbuf: [@sizeOf(Stream.Id)]u8 = undefined;
        id.encode(&idbuf);

        // Lookup the ID in the group PEL.
        const foundnack = raxlib.raxFind(group.?.pel, &idbuf, idbuf.len);
        var nack: *Stream.NACK = undefined;
        // If FORCE is passed, let's check if at least the entry
        // exists in the Stream. In such case, we'll crate a new
        // entry in the PEL from scratch, so that XCLAIM can also
        // be used to create entries in the PEL. Useful for AOF
        // and replication of consumer groups.
        if (force and foundnack == raxlib.raxNotFound) {
            var it: Stream.Iterator = undefined;
            it.start(stream.?, &id, &id, false);
            defer it.stop();

            var item_id: Stream.Id = undefined;
            var numfields: i64 = undefined;
            if (!it.getId(&item_id, &numfields)) {
                // Item must exist for us to create a NACK for it.
                continue;
            }

            // Create the NACK.
            nack = Stream.NACK.create(null);
            _ = raxlib.raxInsert(group.?.pel, &idbuf, idbuf.len, nack, null);
        } else if (foundnack != raxlib.raxNotFound) {
            nack = .cast(foundnack.?);
        }

        if (foundnack != raxlib.raxNotFound) {
            // We need to check if the minimum idle time requested
            // by the caller is satisfied by this entry.
            //
            // Note that the nack could be created by FORCE, in this
            // case there was no pre-existing entry and minidle should
            // be ignored, but in that case nack->consumer is NULL.
            if (nack.consumer != null and minidle != 0) {
                if (now - nack.delivery_time < minidle) continue;
            }

            // Remove the entry from the old consumer.
            // Note that nack->consumer is NULL if we created the
            // NACK above because of the FORCE option.
            if (nack.consumer) |c| {
                _ = raxlib.raxRemove(c.pel, &idbuf, idbuf.len, null);
            }
            // Update the consumer and idle time.
            if (consumer == null) {
                consumer = group.?.lookupConsumer(
                    sds.cast(argv[3].v.ptr),
                    Stream.SLC_NONE,
                );
            }
            nack.consumer = consumer;
            nack.delivery_time = deliverytime;
            // Set the delivery attempts counter if given, otherwise
            // autoincrement unless JUSTID option provided
            if (retrycount >= 0) {
                nack.delivery_count = @intCast(retrycount);
            } else if (!justid) {
                nack.delivery_count +%= 1;
            }
            // Add the entry in the new consumer local PEL.
            _ = raxlib.raxInsert(consumer.?.pel, &idbuf, idbuf.len, nack, null);
            // Send the reply for this entry.
            if (justid) {
                cli.addReplyStreamID(&id);
            } else {
                const emitted = stream.?.replyWithRange(
                    cli,
                    &id,
                    &id,
                    1,
                    false,
                    null,
                    null,
                    Stream.RWR_RAWENTRIES,
                    null,
                );
                if (emitted == 0) cli.addReply(Server.shared.nullbulk);
            }
            arraylen += 1;
            server.dirty +%= 1;
        }
    }
    cli.setDeferredMultiBulkLength(replylen, arraylen);
}

/// XSETID key last-id
///
/// Set the internal "last ID" of a stream.
pub fn xsetidCommand(cli: *CLient) void {
    const argv = cli.argv.?;
    const key = argv[1];
    const xobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nokeyerr,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }

    var id: Stream.Id = undefined;
    if (!Stream.Id.parseOrReply(cli, argv[2], &id, 0, true)) {
        return;
    }

    const stream: *Stream = .cast(xobj.v.ptr);
    // If the stream has at least one item, we want to check that the user
    // is setting a last ID that is equal or greater than the current top
    // item, otherwise the fundamental ID monotonicity assumption is violated.
    if (stream.length > 0) {
        var maxid: Stream.Id = undefined;
        stream.lastValidId(&maxid);
        if (id.compare(&maxid) == .lt) {
            cli.addReplyErr("The ID specified in XSETID is smaller than the " ++
                "target stream top item");
            return;
        }
    }
    stream.last_id = id;
    cli.addReply(Server.shared.ok);
    server.dirty +%= 1;
}

/// XINFO CONSUMERS <key> <group>
/// XINFO GROUPS <key>
/// XINFO STREAM <key>
/// XINFO HELP
pub fn xinfoCommand(cli: *CLient) void {
    const help: []const []const u8 = &.{
        "CONSUMERS <key> <groupname>  -- Show consumer groups of group <groupname>.",
        "GROUPS <key>                 -- Show the stream consumer groups.",
        "STREAM <key>                 -- Show information about the stream.",
        "HELP                         -- Print this help.",
    };

    const argv = cli.argv.?;
    const opt = sds.castBytes(argv[1].v.ptr);
    if (eqlCase(opt, "HELP")) {
        cli.addReplyHelp(help);
        return;
    } else if (cli.argc < 3) {
        cli.addReplyErr("syntax error, try 'XINFO HELP'");
        return;
    }

    // With the exception of HELP handled before any other sub commands, all
    // the ones are in the form of "<subcommand> <key>".
    const key = argv[2];
    const xobj = cli.db.lookupKeyWriteOrReply(
        cli,
        key,
        Server.shared.nokeyerr,
    ) orelse {
        return;
    };
    if (xobj.checkTypeOrReply(cli, .stream)) {
        return;
    }
    const stream: *Stream = .cast(xobj.v.ptr);

    if (eqlCase(opt, "CONSUMERS") and cli.argc == 4) {
        const groupname = sds.cast(argv[3].v.ptr);
        const cg: *Stream.CG = stream.lookupCG(groupname) orelse {
            cli.addReplyErrFormat(
                "-NOGROUP No such consumer group '{s}' for key name '{s}'",
                .{
                    sds.asBytes(groupname),
                    sds.castBytes(key.v.ptr),
                },
            );
            return;
        };

        cli.addReplyMultiBulkLen(@intCast(raxlib.raxSize(cg.consumers)));
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, cg.consumers);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "^", null, 0);
        const now = std.time.milliTimestamp();
        while (raxlib.raxNext(&ri) != 0) {
            const consumer: *Stream.Consumer = .cast(ri.data.?);
            var idle = now - consumer.seen_time;
            if (idle < 0) idle = 0;
            cli.addReplyMultiBulkLen(6);
            cli.addReplyBulkString("name");
            cli.addReplyBulkString(sds.asBytes(consumer.name));
            cli.addReplyBulkString("pending");
            cli.addReplyLongLong(@intCast(raxlib.raxSize(consumer.pel)));
            cli.addReplyBulkString("idle");
            cli.addReplyLongLong(idle);
        }
    } else if (eqlCase(opt, "GROUPS") and cli.argc == 3) {
        const cgroups = stream.cgroups orelse {
            cli.addReplyMultiBulkLen(0);
            return;
        };

        cli.addReplyMultiBulkLen(@intCast(raxlib.raxSize(cgroups)));
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, cgroups);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "^", null, 0);
        while (raxlib.raxNext(&ri) != 0) {
            const cg: *Stream.CG = .cast(ri.data.?);
            cli.addReplyMultiBulkLen(8);
            cli.addReplyBulkString("name");
            cli.addReplyBulkString(ri.key[0..ri.key_len]);
            cli.addReplyBulkString("consumers");
            cli.addReplyLongLong(@intCast(raxlib.raxSize(cg.consumers)));
            cli.addReplyBulkString("pending");
            cli.addReplyLongLong(@intCast(raxlib.raxSize(cg.pel)));
            cli.addReplyBulkString("last-delivered-id");
            cli.addReplyStreamID(&cg.last_id);
        }
    } else if (eqlCase(opt, "STREAM") and cli.argc == 3) {
        cli.addReplyMultiBulkLen(14);
        cli.addReplyBulkString("length");
        cli.addReplyLongLong(@intCast(stream.length));
        cli.addReplyBulkString("radix-tree-keys");
        cli.addReplyLongLong(@intCast(raxlib.raxSize(stream.rax)));
        cli.addReplyBulkString("radix-tree-nodes");
        cli.addReplyLongLong(@intCast(stream.rax.*.numnodes));
        cli.addReplyBulkString("groups");
        cli.addReplyLongLong(@intCast(
            if (stream.cgroups) |cgroups|
                raxlib.raxSize(cgroups)
            else
                0,
        ));
        cli.addReplyBulkString("last-generated-id");
        cli.addReplyStreamID(&stream.last_id);
        // To emit the first/last entry we us the replyWithRange() API.
        var start: Stream.Id = .{ .ms = 0, .seq = 0 };
        var end: Stream.Id = .{ .ms = maxInt(u64), .seq = maxInt(u64) };
        cli.addReplyBulkString("first-entry");
        var count = stream.replyWithRange(
            cli,
            &start,
            &end,
            1,
            false,
            null,
            null,
            Stream.RWR_RAWENTRIES,
            null,
        );
        if (count == 0) cli.addReply(Server.shared.nullbulk);
        cli.addReplyBulkString("last-entry");
        count = stream.replyWithRange(
            cli,
            &start,
            &end,
            1,
            true,
            null,
            null,
            Stream.RWR_RAWENTRIES,
            null,
        );
        if (count == 0) cli.addReply(Server.shared.nullbulk);
    } else {
        cli.addReplySubcommandSyntaxError();
    }
}

pub const Stream = struct {
    /// Don't let listpacks grow too big, even if the user config allows it.
    /// Doing so can lead to an overflow (trying to store more than 32bit length
    /// into the listpack header), or actually an assertion since LispPask.insert()
    /// will return NULL.
    pub const LISTPACK_MAX_SIZE = (1 << 30);

    // Every stream item inside the listpack, has a flags field that is used to
    // mark the entry as deleted, or having the same field as the "master"
    // entry at the start of the listpack> */
    const ITEM_FLAG_NONE = 0; // No special flags.
    const ITEM_FLAG_DELETED = (1 << 0); // Entry is delted. Skip it.
    const ITEM_FLAG_SAMEFIELDS = (1 << 1); // Same fields as master entry.

    // Flags for lookupConsumer()
    pub const SLC_NONE = 0;
    /// Do not create the consumer if it doesn't exist
    pub const SLC_NOCREAT = (1 << 0);
    /// Do not update consumer's seen-time
    pub const SLC_NOREFRESH = (1 << 1);

    // Flags for replyWithRange()
    /// Do not create entries in the PEL.
    pub const RWR_NOACK = (1 << 0);
    /// Do not emit protocol for array boundaries, just the entries.
    pub const RWR_RAWENTRIES = (1 << 1);
    /// Only serve consumer local PEL.
    pub const RWR_HISTORY = (1 << 2);

    /// Stream item ID: a 128 bit number composed of a milliseconds time and
    /// a sequence counter. IDs generated in the same millisecond (or in a past
    /// millisecond if the clock jumped backward) will use the millisecond time
    /// of the latest generated ID and an incremented sequence.
    pub const Id = struct {
        /// Unix time in milliseconds.
        ms: u64,
        /// Sequence number.
        seq: u64,

        /// Parse a stream ID in the format given by clients to Redis, that is
        /// <ms>-<seq>, and converts it into a Stream.Id structure. If the
        /// specified ID is invalid FALSE is returned and an error is reported
        /// to the client, otherwise TRUE is returned. The ID may be in incomplete
        /// form, just stating the milliseconds time part of the stream. In such
        /// a case the missing part is set according to the value of 'missing_seq'
        /// parameter.
        ///
        /// The IDs "-" and "+" specify respectively the minimum and maximum IDs
        /// that can be represented. If 'strict' is set to TRUE, "-" and "+" will
        /// be treated as an invalid ID.
        ///
        /// If 'cli' is set to NULL, no reply is sent to the client.
        fn parseOrReply(
            cli: ?*CLient,
            arg: *Object,
            id: *Stream.Id,
            missing_seq: u64,
            strict: bool,
        ) bool {
            biz: {
                const o = sds.castBytes(arg.v.ptr);
                if (o.len > 128) break :biz;
                if (strict and o.len == 1 and (o[0] == '-' or o[0] == '+')) {
                    break :biz;
                }
                // Handle the "-" and "+" special cases.
                if (o.len == 1 and o[0] == '-') {
                    id.ms = 0;
                    id.seq = 0;
                    return true;
                } else if (o.len == 1 and o[0] == '+') {
                    id.ms = maxInt(u64);
                    id.seq = maxInt(u64);
                    return true;
                }
                // Parse <ms>-<seq> form.
                const dot = std.mem.indexOf(u8, o, "-");
                id.ms = util.string2ull(o[0 .. dot orelse o.len]) orelse {
                    break :biz;
                };
                if (dot) |i| {
                    id.seq = util.string2ull(o[i + 1 ..]) orelse {
                        break :biz;
                    };
                } else {
                    id.seq = missing_seq;
                }
                return true;
            }
            if (cli) |c| {
                c.addReplyErr("Invalid stream ID specified as stream command argument");
            }
            return false;
        }

        /// Convert the specified stream entry ID as a 128 bit big endian number, so
        /// that the IDs can be sorted lexicographically.
        pub fn encode(self: *const Id, buf: []u8) void {
            assert(buf.len >= @sizeOf(Id));
            writeInt(u64, buf[0..8], self.ms, .big);
            writeInt(u64, buf[8..16], self.seq, .big);
        }

        /// This is the reverse of decode(): the decoded ID will be stored
        /// in the 'self' structure passed by reference. The buffer 'buf'
        /// must point to a 128 bit big-endian encoded ID.
        pub fn decode(self: *Id, buf: []u8) void {
            assert(buf.len >= @sizeOf(Id));
            self.ms = readInt(u64, buf[0..8], .big);
            self.seq = readInt(u64, buf[8..16], .big);
        }

        /// Generate the next stream item ID given the previous one. If the current
        /// milliseconds Unix time is greater than the previous one, just use this
        /// as time part and start with sequence part of zero. Otherwise we use the
        /// previous time (and never go backward) and increment the sequence.
        pub fn nextId(self: *const Id) Id {
            var new_id: Id = undefined;
            const ms = std.time.milliTimestamp();
            if (ms > self.ms) {
                new_id.ms = @intCast(ms);
                new_id.seq = 0;
            } else {
                new_id = self.*;
                new_id.incr();
            }
            return new_id;
        }

        /// Set 'id' to be its successor Id.
        pub fn incr(self: *Id) void {
            const max = std.math.maxInt(u64);
            if (self.seq == max) {
                if (self.ms == max) {
                    self.ms = 0;
                    self.seq = 0;
                } else {
                    self.ms += 1;
                    self.seq = 0;
                }
            } else {
                self.seq += 1;
            }
        }

        /// Compare two stream IDs.
        pub fn compare(self: *const Id, other: *const Id) std.math.Order {
            if (self.ms > other.ms) return .gt;
            if (self.ms < other.ms) return .lt;
            // The ms part is the same. Check the sequence part.
            if (self.seq > other.seq) return .gt;
            if (self.seq < other.seq) return .lt;
            // Everything is the same: IDs are equal.
            return .eq;
        }
    };

    /// Pending (yet not acknowledged) message in a consumer group.
    pub const NACK = struct {
        /// Last unix timestamp in ms this message was delivered.
        delivery_time: i64,
        /// Number of times this message was delivered.
        delivery_count: u64,
        /// The consumer this message was delivered to
        /// in the last delivery.
        consumer: ?*Consumer,

        /// Create a NACK entry setting the delivery count to 1 and the delivery
        /// time to the current time. The NACK consumer will be set to the one
        /// specified as argument of the function.
        pub fn create(consumer: ?*Consumer) *NACK {
            const nack = allocator.create(NACK);
            nack.delivery_time = std.time.milliTimestamp();
            nack.delivery_count = 1;
            nack.consumer = consumer;
            return nack;
        }

        pub inline fn cast(ptr: *anyopaque) *NACK {
            return @ptrCast(@alignCast(ptr));
        }

        /// Destroy a NACK entry.
        pub fn destroy(self: *NACK) void {
            allocator.destroy(self);
        }
    };

    /// A specific consumer in a consumer group.
    pub const Consumer = struct {
        /// Last unix timestamp in ms this consumer was active.
        seen_time: i64,
        /// Consumer name. This is how the consumer
        /// will be identified in the consumer group
        /// protocol. Case sensitive.
        name: sds.String,
        /// Consumer specific pending entries list: all
        /// the pending messages delivered to this
        /// consumer not yet acknowledged. Keys are
        /// big endian message IDs, while values are
        /// the same NACK structure referenced
        /// in the "pel" of the conumser group structure
        /// itself, so the value is shared.
        pel: [*c]raxlib.rax,

        /// Create consumer with the specified name.
        pub fn create(name: sds.String) *Consumer {
            const consumer = allocator.create(Consumer);
            consumer.name = sds.dupe(allocator.child, name);
            consumer.pel = raxlib.raxNew();
            return consumer;
        }

        pub inline fn cast(ptr: *anyopaque) *Consumer {
            return @ptrCast(@alignCast(ptr));
        }

        /// Destroy a consumer and associated data structures. Note that this function
        /// will not reassign the pending messages associated with this consumer
        /// nor will delete them from the stream, so when this function is called
        /// to delete a consumer, and not when the whole stream is destroyed, the caller
        /// should do some work before.
        pub fn destroy(self: *Consumer) void {
            // No value free callback: the PEL entries are shared
            // between the consumer and the main stream PEL.
            raxlib.raxFree(self.pel);

            sds.free(allocator.child, self.name);
            allocator.destroy(self);
        }
    };

    /// Consumer group.
    pub const CG = struct {
        /// Last delivered (not acknowledged) ID for this
        /// group. Consumers that will just ask for more
        /// messages will served with IDs > than this.
        last_id: Id,
        /// Pending entries list. This is a radix tree that
        /// has every message delivered to consumers (without
        /// the NOACK option) that was yet not acknowledged
        /// as processed. The key of the radix tree is the
        /// ID as a 64 bit big endian number, while the
        /// associated value is a NACK structure.
        pel: [*c]raxlib.rax,
        /// A radix tree representing the consumers by name
        /// and their associated representation in the form
        /// of Consumer structures.
        consumers: [*c]raxlib.rax,

        /// Create Consumer Group with the specified id.
        pub fn create(id: Id) *CG {
            const cg = allocator.create(CG);
            cg.pel = raxlib.raxNew();
            cg.consumers = raxlib.raxNew();
            cg.last_id = id;
            return cg;
        }

        pub inline fn cast(ptr: *anyopaque) *CG {
            return @ptrCast(@alignCast(ptr));
        }

        /// Lookup the consumer with the specified name in the group: if the
        /// consumer does not exist it is automatically created as a side effect
        /// of calling this function, otherwise its last seen time is updated and
        /// the existing consumer reference returned.
        pub fn lookupConsumer(
            self: *CG,
            name: sds.String,
            flags: i32,
        ) ?*Consumer {
            const creating = flags & SLC_NOCREAT == 0;
            const refreshing = flags & SLC_NOREFRESH == 0;
            var consumer: *Consumer = undefined;
            const value = raxlib.raxFind(self.consumers, name, sds.getLen(name));
            if (value == raxlib.raxNotFound) {
                if (!creating) return null;
                consumer = Consumer.create(name);
                _ = raxlib.raxInsert(
                    self.consumers,
                    name,
                    sds.getLen(name),
                    consumer,
                    null,
                );
            } else {
                consumer = @ptrCast(@alignCast(value.?));
            }
            if (refreshing) {
                consumer.seen_time = std.time.milliTimestamp();
            }
            return consumer;
        }

        /// Delete the consumer specified in the consumer group. The consumer
        /// may have pending messages: they are removed from the PEL, and the
        /// number of pending messages "lost" is returned.
        fn delConsumer(self: *CG, name: sds.String) u64 {
            const consumer = self.lookupConsumer(
                name,
                SLC_NOCREAT | SLC_NOREFRESH,
            ) orelse {
                return 0;
            };
            const pending = raxlib.raxSize(consumer.pel);

            // Iterate all the consumer pending messages, deleting every
            // corresponding entry from the global entry.
            var ri: raxlib.raxIterator = undefined;
            raxlib.raxStart(&ri, consumer.pel);
            defer raxlib.raxStop(&ri);
            _ = raxlib.raxSeek(&ri, "^", null, 0);
            while (raxlib.raxNext(&ri) != 0) {
                const nack: *NACK = .cast(ri.data.?);
                _ = raxlib.raxRemove(self.pel, ri.key, ri.key_len, null);
                nack.destroy();
            }

            // Deallocate the consumer.
            _ = raxlib.raxRemove(self.consumers, name, sds.getLen(name), null);
            consumer.destroy();
            return pending;
        }

        /// Free a consumer group and all its associated data.
        pub fn destroy(self: *CG) void {
            raxlib.raxFreeWithCallback(self.pel, freeNACKCallback);
            raxlib.raxFreeWithCallback(self.consumers, freeConsumerCallback);
            allocator.destroy(self);
        }

        fn freeNACKCallback(ptr: ?*anyopaque) callconv(.c) void {
            const nack: *NACK = @ptrCast(@alignCast(ptr.?));
            nack.destroy();
        }

        fn freeConsumerCallback(ptr: ?*anyopaque) callconv(.c) void {
            const consumer: *Consumer = @ptrCast(@alignCast(ptr.?));
            consumer.destroy();
        }
    };

    /// We define an iterator to iterate stream items in an abstract way, without
    /// caring about the radix tree + listpack representation. Technically speaking
    /// the iterator is only used inside replyWithRange(), so could just
    /// be implemented inside the function, but practically there is the AOF
    /// rewriting code that also needs to iterate the stream to emit the XADD
    /// commands.
    pub const Iterator = struct {
        /// The stream we are iterating.
        stream: *Stream,
        /// ID of the master entry at listpack head.
        master_id: Id,
        /// Master entries # of fields.
        master_fields_count: u64,
        /// Master entries start in listpack.
        master_fields_start: [*]u8,
        /// Master field to emit next.
        master_fields_ptr: [*]u8,
        /// Flags of entry we are emitting.
        entry_flags: i32,
        /// True if iterating end to start (reverse).
        rev: bool,
        /// Start key as 128 bit big endian.
        start_key: [2]u64,
        /// End key as 128 bit big endian.
        end_key: [2]u64,
        /// Rax iterator.
        ri: raxlib.raxIterator,
        /// Current listpack.
        lp: ?*ListPack,
        /// Current listpack cursor.
        lp_ele: ?[*]u8,
        /// Current entry flags pointer.
        lp_flags: ?[*]u8,
        /// Buffers used to hold the string of ListPack.get() when the element is
        /// integer encoded, so that there is no string representation of the
        /// element inside the listpack itself.
        field_buf: [ListPack.INTBUF_SIZE]u8,
        value_buf: [ListPack.INTBUF_SIZE]u8,

        /// Initialize the stream iterator, so that we can call iterating functions
        /// to get the next items. This requires a corresponding stop() at the end.
        /// The 'rev' parameter controls the direction. If it's false the iteration
        /// is from the start to the end element (inclusive), otherwise if rev is
        /// true, the iteration is reversed.
        pub fn start(
            self: *Iterator,
            stream: *Stream,
            start_id: ?*Id,
            end_id: ?*Id,
            rev: bool,
        ) void {
            // Intialize the iterator and translates the iteration start/stop
            // elements into a 128 big big-endian number.
            if (start_id) |id| {
                id.encode(sliceAsBytes(&self.start_key));
            } else {
                self.start_key[0] = 0;
                self.start_key[1] = 0;
            }
            if (end_id) |id| {
                id.encode(sliceAsBytes(&self.end_key));
            } else {
                self.end_key[0] = std.math.maxInt(u64);
                self.end_key[1] = std.math.maxInt(u64);
            }

            // Seek the correct node in the radix tree.
            raxlib.raxStart(&self.ri, stream.rax);
            if (!rev) {
                if (start_id != null and (start_id.?.ms != 0 or start_id.?.seq != 0)) {
                    const start_key: []u8 = sliceAsBytes(&self.start_key);
                    _ = raxlib.raxSeek(&self.ri, "<=", start_key.ptr, start_key.len);
                    if (raxlib.raxEOF(&self.ri) != 0) {
                        _ = raxlib.raxSeek(&self.ri, "^", null, 0);
                    }
                } else {
                    _ = raxlib.raxSeek(&self.ri, "^", null, 0);
                }
            } else {
                if (end_id != null and (end_id.?.ms != 0 or end_id.?.seq != 0)) {
                    const end_key: []u8 = sliceAsBytes(&self.end_key);
                    _ = raxlib.raxSeek(&self.ri, "<=", end_key.ptr, end_key.len);
                    if (raxlib.raxEOF(&self.ri) != 0) {
                        _ = raxlib.raxSeek(&self.ri, "$", null, 0);
                    }
                } else {
                    _ = raxlib.raxSeek(&self.ri, "$", null, 0);
                }
            }
            self.stream = stream;
            self.lp = null; // There is no current listpack right now.
            self.lp_ele = null; //  Current listpack cursor.
            self.rev = rev; // Direction, if true reversed, from end to start.
        }

        /// Return TRUE and store the current item ID at 'id' if there are still
        /// elements within the iteration range, otherwise return FALSE in order to
        /// signal the iteration terminated.
        pub fn getId(self: *Iterator, id: *Id, numfields: *i64) bool {
            // Will stop when element > stop_key or end of radix tree.
            while (true) {
                // If the current listpack is set to NULL, this is the start of the
                // iteration or the previous listpack was completely iterated.
                // Go to the next node.
                if (self.lp == null or self.lp_ele == null) {
                    if (!self.rev and raxlib.raxNext(&self.ri) == 0) {
                        return false;
                    } else if (self.rev and raxlib.raxPrev(&self.ri) == 0) {
                        return false;
                    }
                    assert(self.ri.key_len == @sizeOf(Id));
                    // Get the master ID.
                    self.master_id.decode(self.ri.key[0..self.ri.key_len]);
                    // Get the master fields count.
                    self.lp = ListPack.cast(self.ri.data.?);
                    self.lp_ele = self.lp.?.first(); // Seek items count
                    self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek deleted count.
                    self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek num fields.
                    self.master_fields_count = @intCast(ListPack.getInteger(self.lp_ele.?));
                    self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek first field.
                    self.master_fields_start = self.lp_ele.?;
                    // We are now pointing to the first field of the master entry.
                    // We need to seek either the first or the last entry depending
                    // on the direction of the iteration.
                    if (!self.rev) {
                        for (0..self.master_fields_count) |_| {
                            self.lp_ele = self.lp.?.next(self.lp_ele.?);
                        }
                    } else {
                        // If we are iterating in reverse direction, just seek the
                        // last part of the last entry in the listpack (that is, the
                        // fields count).
                        self.lp_ele = self.lp.?.last();
                    }
                } else if (self.rev) {
                    // If we are itereating in the reverse order, and this is not
                    // the first entry emitted for this listpack, then we already
                    // emitted the current entry, and have to go back to the previous
                    // one.
                    var lp_count = ListPack.getInteger(self.lp_ele.?);
                    while (lp_count > 0) : (lp_count -= 1) {
                        self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                    }
                    // Seek lp-count of prev entry.
                    self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                }

                // For every radix tree node, iterate the corresponding listpack,
                // returning elements when they are within range.
                while (true) {
                    if (!self.rev) {
                        // If we are going forward, skip the previous entry
                        // lp-count field (or in case of the master entry, the zero
                        // term field)
                        self.lp_ele = self.lp.?.next(self.lp_ele.?);
                        if (self.lp_ele == null) break;
                    } else {
                        // If we are going backward, read the number of elements this
                        // entry is composed of, and jump backward N times to seek
                        // its start.
                        var lp_count = ListPack.getInteger(self.lp_ele.?);
                        if (lp_count == 0) {
                            // We reached the master entry.
                            self.lp = null;
                            self.lp_ele = null;
                            break;
                        }
                        while (lp_count > 0) : (lp_count -= 1) {
                            self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                        }
                    }

                    // Get the flags entry.
                    self.lp_flags = self.lp_ele;
                    const flags: i32 = @intCast(ListPack.getInteger(self.lp_flags.?));
                    self.lp_ele = self.lp.?.next(self.lp_ele.?); // Seek ID.

                    // Get the ID: it is encoded as difference between the master
                    // ID and this entry ID.
                    id.* = self.master_id;
                    id.ms +%= @as(u64, @intCast(ListPack.getInteger(self.lp_ele.?)));
                    self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    id.seq +%= @as(u64, @intCast(ListPack.getInteger(self.lp_ele.?)));
                    self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    var buf: [@sizeOf(Id)]u8 = undefined;
                    id.encode(&buf);

                    // The number of entries is here or not depending on the
                    // flags.
                    if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                        numfields.* = @intCast(self.master_fields_count);
                    } else {
                        numfields.* = ListPack.getInteger(self.lp_ele.?);
                        self.lp_ele = self.lp.?.next(self.lp_ele.?);
                    }

                    // If current >= start, and the entry is not marked as
                    // deleted, emit it.
                    const start_key: []u8 = sliceAsBytes(&self.start_key);
                    const end_key: []u8 = sliceAsBytes(&self.end_key);
                    if (!self.rev) {
                        if (memcmp(&buf, start_key).compare(.gte) and
                            (flags & ITEM_FLAG_DELETED == 0))
                        {
                            if (memcmp(&buf, end_key) == .gt) {
                                // We are already out of range.
                                return false;
                            }
                            self.entry_flags = flags;
                            if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                                self.master_fields_ptr = self.master_fields_start;
                            }
                            // Valid item returned.
                            return true;
                        }
                    } else {
                        if (memcmp(&buf, end_key).compare(.lte) and
                            (flags & ITEM_FLAG_DELETED == 0))
                        {
                            if (memcmp(&buf, start_key) == .lt) {
                                // We are already out of range.
                                return false;
                            }
                            self.entry_flags = flags;
                            if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                                self.master_fields_ptr = self.master_fields_start;
                            }
                            return true;
                        }
                    }

                    // If we do not emit, we have to discard if we are going
                    // forward, or seek the previous entry if we are going
                    // backward.
                    if (!self.rev) {
                        const to_discard = if (flags & ITEM_FLAG_SAMEFIELDS != 0)
                            numfields.*
                        else
                            numfields.* * 2;
                        for (0..@as(u64, @intCast(to_discard))) |_| {
                            self.lp_ele = self.lp.?.next(self.lp_ele.?);
                        }
                    } else {
                        // flag + id ms + id seq + one more to
                        // go back to the previous entry "count"
                        // field.
                        var prev_items: usize = 4;
                        // If the entry was not flagged SAMEFIELD we also read the
                        // number of fields, so go back one more.
                        if (flags & ITEM_FLAG_SAMEFIELDS == 0) prev_items += 1;
                        while (prev_items > 0) : (prev_items -= 1) {
                            self.lp_ele = self.lp.?.prev(self.lp_ele.?);
                        }
                    }
                }

                // End of listpack reached. Try the next/prev radix tree node.
            }
        }

        /// Get the field and value of the current item we are iterating. This should
        /// be called immediately after Iterator.getID(), and for each field according
        /// to the number of fields returned by Iterator.getID().
        /// The function populates the field and value pointers, that are valid until
        /// the next iterator call, assuming no one touches the stream meanwhile.
        pub fn getField(self: *Iterator, field_ptr: *[]u8, value_ptr: *[]u8) void {
            var field_len: i64 = undefined;
            var value_len: i64 = undefined;
            if (self.entry_flags & ITEM_FLAG_SAMEFIELDS != 0) {
                const ptr = ListPack.get(
                    self.master_fields_ptr,
                    &field_len,
                    &self.field_buf,
                ).?;
                field_ptr.* = ptr[0..@intCast(field_len)];
                self.master_fields_ptr = self.lp.?.next(self.master_fields_ptr).?;
            } else {
                const ptr = ListPack.get(
                    self.lp_ele.?,
                    &field_len,
                    &self.field_buf,
                ).?;
                field_ptr.* = ptr[0..@intCast(field_len)];
                self.lp_ele = self.lp.?.next(self.lp_ele.?);
            }
            const ptr = ListPack.get(
                self.lp_ele.?,
                &value_len,
                &self.value_buf,
            ).?;
            value_ptr.* = ptr[0..@intCast(value_len)];
            self.lp_ele = self.lp.?.next(self.lp_ele.?);
        }

        /// Stop the stream iterator. The only cleanup we need is to free the rax
        /// itereator, since the stream iterator itself is supposed to be stack
        /// allocated.
        pub fn stop(self: *Iterator) void {
            raxlib.raxStop(&self.ri);
        }

        /// Remove the current entry from the stream: can be called after the
        /// getId() API or after any detField() call, however we need to iterate
        /// a valid entry while calling this function. Moreover the function
        /// requires the entry ID we are currently iterating, that was previously
        /// returned by getId().
        ///
        /// Note that after calling this function, next calls to getField() can't
        /// be performed: the entry is now deleted. Instead the iterator will
        /// automatically re-seek to the next entry, so the caller should continue
        /// with getId().
        fn removeEntry(self: *Iterator, current: *Id) void {
            var lp = self.lp.?;

            // We do not really delete the entry here. Instead we mark it as
            // deleted flagging it, and also incrementing the count of the
            // deleted entries in the listpack header.
            //
            // We start flagging:
            var flags = ListPack.getInteger(self.lp_flags.?);
            flags |= ITEM_FLAG_DELETED;
            lp = lp.replaceInteger(&self.lp_flags, flags).?;

            // Change the valid/deleted entries count in the master entry.
            var p = lp.first();
            var aux = ListPack.getInteger(p.?);
            if (aux == 1) {
                // If this is the last element in the listpack, we can remove the whole
                // node.
                lp.free();
                _ = raxlib.raxRemove(
                    self.stream.rax,
                    self.ri.key,
                    self.ri.key_len,
                    null,
                );
            } else {
                // In the base case we alter the counters of valid/deleted entries.
                lp = lp.replaceInteger(&p, aux - 1).?;
                p = lp.next(p.?); // Seek deleted field.
                aux = ListPack.getInteger(p.?);
                lp = lp.replaceInteger(&p, aux + 1).?;

                // Update the listpack with the new pointer.
                if (self.lp != lp) {
                    _ = raxlib.raxInsert(
                        self.stream.rax,
                        self.ri.key,
                        self.ri.key_len,
                        lp,
                        null,
                    );
                }
            }

            // Update the number of entries counter.
            self.stream.length -= 1;

            // Re-seek the iterator to fix the now messed up state.
            var start_id: Id = undefined;
            var end_id: Id = undefined;
            if (self.rev) {
                start_id.decode(sliceAsBytes(&self.start_key));
                end_id = current.*;
            } else {
                start_id = current.*;
                end_id.decode(sliceAsBytes(&self.end_key));
            }
            self.stop();
            self.start(self.stream, &start_id, &end_id, self.rev);
        }
    };

    /// Stream propagation informations, passed to functions in order to propagate
    /// XCLAIM commands to AOF and slaves.
    pub const PropInfo = struct {
        keyname: *Object,
        groupname: *Object,
    };

    /// The radix tree holding the stream.
    rax: [*c]raxlib.rax,
    /// Number of elements inside this stream.
    length: u64,
    /// Zero if there are yet no items.
    last_id: Id,
    /// Consumer groups dictionary: name -> CG
    cgroups: ?[*c]raxlib.rax,

    pub fn create() *Stream {
        const s = allocator.create(Stream);
        s.rax = raxlib.raxNew();
        s.length = 0;
        s.last_id.ms = 0;
        s.last_id.seq = 0;
        s.cgroups = null; // Created on demand to save memory when not used.
        return s;
    }

    /// Look the stream at 'key' and return the corresponding stream object.
    /// The function creates a key setting it to an empty stream if needed.
    fn lookupCreateOrReply(cli: *CLient, key: *Object) ?*Stream {
        const o = cli.db.lookupKeyWrite(key) orelse {
            const o = Object.createStream();
            defer o.decrRefCount();
            cli.db.add(key, o);
            return @ptrCast(@alignCast(o.v.ptr));
        };
        if (o.checkTypeOrReply(cli, .stream)) {
            return null;
        }
        return @ptrCast(@alignCast(o.v.ptr));
    }

    /// Get the last valid (non-tombstone) streamID.
    fn lastValidId(self: *Stream, maxid: *Id) void {
        var it: Iterator = undefined;
        it.start(self, null, null, true);
        defer it.stop();
        var numfields: i64 = undefined;
        _ = it.getId(maxid, &numfields);
    }

    /// Adds a new item into the stream 's' having the specified number of
    /// field-value pairs as specified in 'numfields' and stored into 'argv'.
    /// Returns the new entry ID populating the 'added_id' structure.
    ///
    /// If 'use_id' is not NULL, the ID is not auto-generated by the function,
    /// but instead the passed ID is uesd to add the new entry. In this case
    /// adding the entry may fail as specified later in this comment.
    ///
    /// 1. If an ID was given via 'use_id', but adding it failed since the
    ///    current top ID is greater or equal, return error.DOM.
    /// 2. If a size of a single element or the sum of the elements is too big to
    ///    be stored into the stream. return RANGE.
    fn append(
        self: *Stream,
        argv: []*Object,
        numfields: usize,
        added_id: ?*Stream.Id,
        use_id: ?*Stream.Id,
    ) error{ DOM, RANGE }!void {
        const id: Stream.Id = blk: {
            if (use_id) |uid| {
                break :blk uid.*;
            }
            break :blk self.last_id.nextId();
        };

        // Check that the new ID is greater than the last entry ID
        // or return an error. Automatically generated IDs might
        // overflow (and wrap-around) when incrementing the sequence
        // part.
        if (id.compare(&self.last_id) != .gt) {
            return error.DOM;
        }

        // Avoid overflow when trying to add an element to the stream (listpack
        // can only host up to 32bit length sttrings, and also a total listpack size
        // can't be bigger than 32bit length.
        var totelelen: usize = 0;
        for (argv) |field| {
            totelelen += sds.getLen(sds.cast(field.v.ptr));
        }
        if (totelelen > Stream.LISTPACK_MAX_SIZE) {
            return error.RANGE;
        }

        // Add the new entry.
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, self.rax);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "$", null, 0);

        var lp_bytes: usize = 0; // Total bytes in the tail listpack.
        var lp: ?*ListPack = null;

        // Get a reference to the tail node listpack.
        if (raxlib.raxNext(&ri) != 0) {
            lp = ListPack.cast(ri.data.?);
            lp_bytes = lp.?.bytes.get();
        }

        // We have to add the key into the radix tree in lexicographic order,
        // to do so we consider the ID as a single 128 bit number written in
        // big endian, so that the most significant bytes are the first ones.
        var rax_key: [@sizeOf(Id)]u8 = undefined;
        var master_id: Stream.Id = undefined;

        // Create a new listpack and radix tree node if needed. Note that when
        // a new listpack is created, we populate it with a "master entry". This
        // is just a set of fields that is taken as references in order to compress
        // the stream entries that we'll add inside the listpack.
        //
        // Note that while we use the first added entry fields to create
        // the master entry, the first added entry is NOT represented in the master
        // entry, which is a stand alone object. But of course, the first entry
        // will compress well because it's used as reference.
        //
        // The master entry is composed like in the following example:
        //
        // +-------+---------+------------+---------+--/--+---------+---------+-+
        // | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
        // +-------+---------+------------+---------+--/--+---------+---------+-+
        //
        // count and deleted just represent respectively the total number of
        // entries inside the listpack that are valid, and marked as deleted
        // (delted flag in the entry flags set). So the total number of items
        // actually inside the listpack (both deleted and not) is count+deleted.
        //
        // The real entries will be encoded with an ID that is just the
        // millisecond and sequence difference compared to the key stored at
        // the radix tree node containing the listpack (delta encoding), and
        // if the fields of the entry are the same as the master enty fields, the
        // entry flags will specify this fact and the entry fields and number
        // of fields will be omitted (see later in the code of this function).
        //
        // The "0" entry at the end is the same as the 'lp-count' entry in the
        // regular stream entries (see below), and marks the fact that there are
        // no more entries, when we scan the stream from right to left.

        // First of all, check if we can append to the current macro node or
        // if we need to switch to the next one. 'lp' will be set to NULL if
        // the current node is full.
        if (lp != null) {
            var node_max_bytes = server.stream_node_max_bytes;
            if (node_max_bytes == 0 or node_max_bytes > Stream.LISTPACK_MAX_SIZE) {
                node_max_bytes = Stream.LISTPACK_MAX_SIZE;
            }
            if (lp_bytes + totelelen >= node_max_bytes) {
                lp = null;
            } else if (server.stream_node_max_entries != 0) {
                const count = ListPack.getInteger(lp.?.first().?);
                if (count >= server.stream_node_max_entries) {
                    lp = null;
                }
            }
        }

        var flags: i32 = ITEM_FLAG_NONE;
        if (lp == null or lp_bytes >= server.stream_node_max_bytes) {
            master_id = id;
            // Create the listpack having the master entry ID and fields.
            id.encode(&rax_key);
            lp = ListPack.new();
            lp = lp.?.appendInteger(1); // One item, the one we are adding.
            lp = lp.?.appendInteger(0); // Zero deleted so far.
            lp = lp.?.appendInteger(@intCast(numfields));
            for (0..numfields) |i| {
                const field = sds.castBytes(argv[i * 2].v.ptr);
                lp = lp.?.append(field);
            }
            lp = lp.?.appendInteger(0); // Master entry zero terminator.
            _ = raxlib.raxInsert(self.rax, &rax_key, rax_key.len, lp, null);
            // The first entry we insert, has obviously the same fields of the
            // master entry.
            flags |= ITEM_FLAG_SAMEFIELDS;
        } else {
            assert(ri.key_len == rax_key.len);
            memcpy(&rax_key, ri.key, ri.key_len);

            // Read the master ID from the radix tree key.
            master_id.decode(&rax_key);
            var lp_ele = lp.?.first();

            // Update count and skip the deleted fields.
            const count = ListPack.getInteger(lp_ele.?);
            lp = lp.?.replaceInteger(&lp_ele, count + 1);
            lp_ele = lp.?.next(lp_ele.?); // seek deleted.
            lp_ele = lp.?.next(lp_ele.?); // seek master entry num fields.

            // Check if the entry we are adding, have the same fields
            // as the master entry.
            const master_fields_count = ListPack.getInteger(lp_ele.?);
            lp_ele = lp.?.next(lp_ele.?);
            if (numfields == master_fields_count) {
                var i: usize = 0;
                while (i < master_fields_count) : (i += 1) {
                    var ele_len: i64 = undefined;
                    var buf: [ListPack.INTBUF_SIZE]u8 = undefined;
                    const field = sds.castBytes(argv[i * 2].v.ptr);
                    const e = ListPack.get(lp_ele.?, &ele_len, &buf).?;
                    if (ele_len != field.len or
                        memcmp(e[0..@intCast(ele_len)], field) != .eq)
                    {
                        break;
                    }
                    lp_ele = lp.?.next(lp_ele.?);
                }
                // All fields are the same! We can compress the field names
                // setting a single bit in the flags.
                if (i == master_fields_count) {
                    flags |= ITEM_FLAG_SAMEFIELDS;
                }
            }
        }

        // Populate the listpack with the new entry. We use the following
        // encoding:
        //
        // +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
        // |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
        // +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
        //
        // However if the SAMEFIELD flag is set, we have just to populate
        // the entry with the values, so it becomes:
        //
        // +-----+--------+-------+-/-+-------+--------+
        // |flags|entry-id|value-1|...|value-N|lp-count|
        // +-----+--------+-------+-/-+-------+--------+
        //
        // The entry-id field is actually two separated fields: the ms
        // and seq difference compared to the master entry.
        //
        // The lp-count field is a number that states the number of listpack pieces
        // that compose the entry, so that it's possible to travel the entry
        // in reverse order: we can just start from the end of the listpack, read
        // the entry, and jump back N times to seek the "flags" field to read
        // the stream full entry.
        lp = lp.?.appendInteger(flags);
        lp = lp.?.appendInteger(@intCast(id.ms - master_id.ms));
        lp = lp.?.appendInteger(@intCast(id.seq - master_id.seq));
        if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
            lp = lp.?.appendInteger(@intCast(numfields));
        }
        for (0..numfields) |i| {
            const field = sds.castBytes(argv[i * 2].v.ptr);
            const value = sds.castBytes(argv[i * 2 + 1].v.ptr);
            if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
                lp = lp.?.append(field);
            }
            lp = lp.?.append(value);
        }
        // Compute and store the lp-count field.
        var lp_count = numfields;
        // Add the 3 fixed fields flags + ms-diff + seq-diff.
        lp_count += 3;
        if (flags & ITEM_FLAG_SAMEFIELDS == 0) {
            // If the item is not compressed, it also has the fields other than
            // the values, and an additional num-fileds field.
            lp_count += numfields + 1;
        }
        lp = lp.?.appendInteger(@intCast(lp_count));

        // Insert back into the tree in order to update the listpack pointer.
        if (@as(?*ListPack, @ptrCast(@alignCast(ri.data))) != lp) {
            _ = raxlib.raxInsert(self.rax, &rax_key, rax_key.len, lp, null);
        }
        self.length += 1;
        self.last_id = id;
        if (added_id) |aid| aid.* = id;
    }

    /// Delete the specified item ID from the stream, returning TRUE if the item
    /// was deleted FALSE otherwise (if it does not exist).
    fn delete(self: *Stream, id: *Id) bool {
        var deleted = false;

        var it: Iterator = undefined;
        it.start(self, id, id, false);
        defer it.stop();

        var myid: Id = undefined;
        var numfields: i64 = undefined;
        if (it.getId(&myid, &numfields)) {
            it.removeEntry(&myid);
            deleted = true;
        }
        return deleted;
    }

    /// Trim the stream to have no more than maxlen elements, and return the
    /// number of elements removed from the stream. The 'approx' option, if true,
    /// specifies that the trimming must be performed in a approximated way in
    /// order to maximize performances. This means that the stream may contain
    /// more elements than 'maxlen', and elements are only removed if we can remove
    /// a *whole* node of the radix tree. The elements are removed from the head
    /// of the stream (older elements).
    ///
    /// The function may return zero if:
    ///
    /// 1) The stream is already shorter or equal to the specified max length.
    /// 2) The 'approx' option is true and the head node had not enough elements
    ///    to be deleted, leaving the stream with a number of elements >= maxlen.
    fn trim(self: *Stream, maxlen: usize, approx: bool) usize {
        if (self.length <= maxlen) {
            return 0;
        }

        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, self.rax);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "^", null, 0);

        var deleted: usize = 0;
        while (self.length > maxlen and raxlib.raxNext(&ri) != 0) {
            var lp: ?*ListPack = ListPack.cast(ri.data.?);
            var p = lp.?.first();
            const entries: usize = @intCast(ListPack.getInteger(p.?));

            // Check if we can remove the whole node, and still have at
            // least maxlen elements.
            if (self.length - entries >= maxlen) {
                lp.?.free();
                _ = raxlib.raxRemove(self.rax, ri.key, ri.key_len, null);
                _ = raxlib.raxSeek(&ri, ">=", ri.key, ri.key_len);
                self.length -= entries;
                deleted += entries;
                continue;
            }

            // If we cannot remove a whole element, and approx is true,
            // stop here.
            if (approx) break;

            // Otherwise, we have to mark single entries inside the listpack
            // as deleted. We start by updating the entries/deleted counters.
            const to_delete = self.length - maxlen;
            assert(to_delete < entries);
            lp = lp.?.replaceInteger(&p, @intCast(entries - to_delete));
            p = lp.?.next(p.?); // Seek deleted field.
            const marked_deleted: u64 = @intCast(ListPack.getInteger(p.?));
            lp = lp.?.replaceInteger(&p, @intCast(marked_deleted + to_delete));
            p = lp.?.next(p.?); // Seek num-of-fields in the master entry.

            // Skip all the master fields.
            const master_fields_count = ListPack.getInteger(p.?);
            p = lp.?.next(p.?); // Seek the first field.
            for (0..@intCast(master_fields_count)) |_| {
                p = lp.?.next(p.?); // Skip all master fields.
            }
            p = lp.?.next(p.?); // Skip the zero master entry terminator.

            // 'p' is now pointing to the first entry inside the listpack.
            // We have to run entry after entry, marking entries as deleted
            // if they are already not deleted.
            while (p != null) {
                var flags = ListPack.getInteger(p.?);
                var to_skip: i64 = undefined;

                // Mark the entry as deleted.
                if (flags & ITEM_FLAG_DELETED == 0) {
                    flags |= ITEM_FLAG_DELETED;
                    lp = lp.?.replaceInteger(&p, flags);
                    deleted += 1;
                    self.length -= 1;
                    if (self.length <= maxlen) break; // Enough entries deleted.
                }
                p = lp.?.next(p.?); // Skip ID ms delta.
                p = lp.?.next(p.?); // Skip ID seq delta.
                p = lp.?.next(p.?); // Seek num-fields or values (if compressed).
                if (flags & ITEM_FLAG_SAMEFIELDS != 0) {
                    to_skip = master_fields_count;
                } else {
                    to_skip = ListPack.getInteger(p.?);
                    to_skip = to_skip * 2 + 1;
                }
                while (to_skip > 0) : (to_skip -= 1) {
                    p = lp.?.next(p.?); // Skip the whole entry.
                }
                p = lp.?.next(p.?); // Skip the final lp-count field.
            }

            // Update the listpack with the new pointer.
            _ = raxlib.raxInsert(self.rax, ri.key, ri.key_len, lp, null);

            // If we are here, there was enough to delete in the current
            // node, so no need to go to the next node.
            break;
        }
        return deleted;
    }

    /// Create a new consumer group in the context of the stream, having the
    /// specified name and last server ID. If a consumer group with the same name
    /// already existed NULL is returned, otherwise the pointer to the consumer
    /// group is returned.
    pub fn createCG(self: *Stream, name: []u8, id: *Id) ?*CG {
        if (self.cgroups == null) {
            self.cgroups = raxlib.raxNew();
        }
        const cgroups = self.cgroups.?;
        if (raxlib.raxFind(cgroups, name.ptr, name.len) != raxlib.raxNotFound) {
            return null;
        }
        const cg = CG.create(id.*);
        _ = raxlib.raxInsert(cgroups, name.ptr, name.len, cg, null);
        return cg;
    }

    /// Lookup the consumer group in the specified stream and returns its
    /// pointer, otherwise if there is no such group, NULL is returned.
    pub fn lookupCG(self: *Stream, groupname: sds.String) ?*CG {
        const groups = self.cgroups orelse {
            return null;
        };
        const value = raxlib.raxFind(groups, groupname, sds.getLen(groupname));
        if (value == raxlib.raxNotFound) {
            return null;
        }
        return @ptrCast(@alignCast(value.?));
    }

    /// Send the stream items in the specified range to the client 'cli'. The range
    /// the client will receive is between start and end inclusive, if 'count' is
    /// non zero, no more than 'count' elements are sent.
    ///
    /// The 'end' pointer can be NULL to mean that we want all the elements from
    /// 'start' till the end of the stream. If 'rev' is TRUE, elements are
    /// produced in reversed order from end to start.
    ///
    /// The function returns the number of entries emitted.
    ///
    /// If group and consumer are not NULL, the function performs additional work:
    /// 1. It updates the last delivered ID in the group in case we are
    ///    sending IDs greater than the current last ID.
    /// 2. If the requested IDs are already assigned to some other consumer, the
    ///    function will not return it to the client.
    /// 3. An entry in the pending list will be created for every entry delivered
    ///    for the first time to this consumer.
    ///
    /// The behavior may be modified passing non-zero flags:
    ///
    /// RWR_NOACK: Do not craete PEL entries, that is, the point "3" above
    ///            is not performed.
    /// RWR_RAWENTRIES: Do not emit array boundaries, but just the entries,
    ///                 and return the number of entries emitted as usually.
    ///                 This is used when the function is just used in order
    ///                 to emit data and there is some higher level logic.
    ///
    /// The final argument 'spi' (stream propagatino info pointer) is a structure
    /// filled with information needed to propagte the command execution to AOF
    /// and slaves, in the case a consumer group was passed: we need to generate
    /// XCLAIM commands to create the pending list into AOF/slaves in that case.
    ///
    /// If 'spi' is set to NULL no propagation will happen even if the group was
    /// given, but currently such a feature is never used by the code base that
    /// will always pass 'spi' and propagate when a group is passed.
    ///
    /// Note that this function is recursive in certain cases. When it's called
    /// with a non NULL group and consumer argument, it may call
    /// replyWithRangeFromConsumerPEL() in order to get entries from the
    /// consumer pending entries list. However such a function will then call
    /// replyWithRange() in order to emit single entries (found in the
    /// PEL by ID) to the client. This is the use case for the RWR_RAWENTRIES
    /// flag.
    pub fn replyWithRange(
        self: *Stream,
        cli: *CLient,
        start: *Id,
        end: ?*Id,
        count: usize,
        rev: bool,
        group: ?*CG,
        consumer: ?*Consumer,
        flags: i32,
        spi: ?*PropInfo,
    ) usize {
        _ = spi;

        // If the client is asking for some history, we serve it using a
        // different function, so that we return entries *solely* from its
        // own PEL. This ensures each consumer will always and only see
        // the history of messages delivered to it and not yet confirmed
        // as delivered.
        if (group != null and (flags & RWR_HISTORY != 0)) {
            assert(consumer != null);
            return self.replyWithRangeFromConsumerPEL(
                cli,
                start,
                end,
                count,
                consumer.?,
            );
        }

        var replylen: ?*CLient.ReplyBlock = null;
        if (flags & RWR_RAWENTRIES == 0) {
            replylen = cli.addDeferredMultiBulkLength();
        }

        var arraylen: usize = 0;
        var si: Iterator = undefined;
        si.start(self, start, end, rev);
        defer si.stop();
        var id: Id = undefined;
        var numfields: i64 = undefined;
        while (si.getId(&id, &numfields)) {
            // Update the group last_id if needed.
            if (group) |cg| if (id.compare(&cg.last_id) == .gt) {
                cg.last_id = id;
            };

            // Emit a two elements array for each item. The first is
            // the ID, the second is an array of field-value pairs.
            cli.addReplyMultiBulkLen(2);
            cli.addReplyStreamID(&id);
            cli.addReplyMultiBulkLen(numfields * 2);

            // Emit the field-value pairs.
            while (numfields > 0) : (numfields -= 1) {
                var field: []u8 = undefined;
                var value: []u8 = undefined;
                si.getField(&field, &value);
                cli.addReplyBulkString(field);
                cli.addReplyBulkString(value);
            }

            // If a group is passed, we need to create an entry in the
            // PEL (pending entries list) of this group *and* this consumer.
            //
            // Note that we cannot be sure about the fact the message is not
            // already owned by another consumer, because the admin is able
            // to change the consumer group last delivered ID using the
            // XGROUP SETID command. So if we find that there is already
            // a NACK for the entry, we need to associate it to the new
            // consumer.
            if (group != null and flags & RWR_NOACK == 0) {
                assert(consumer != null);
                const cg = group.?;

                var buf: [@sizeOf(Id)]u8 = undefined;
                id.encode(&buf);

                // Try to add a new NACK. Most of the time this will work and
                // will not require extra lookups. We'll fix the problem later
                // if we find that there is already a entry for this ID.
                var nack = NACK.create(consumer);
                const group_inserted = raxlib.raxTryInsert(
                    cg.pel,
                    &buf,
                    buf.len,
                    nack,
                    null,
                );
                const consumer_inserted = raxlib.raxTryInsert(
                    consumer.?.pel,
                    &buf,
                    buf.len,
                    nack,
                    null,
                );

                // Now we can check if the entry was already busy, and
                // in that case reassign the entry to the new consumer,
                // or update it if the consumer is the same as before.
                if (group_inserted == 0) {
                    nack.destroy();
                    nack = .cast(raxlib.raxFind(cg.pel, &buf, buf.len).?);
                    _ = raxlib.raxRemove(
                        nack.consumer.?.pel,
                        &buf,
                        buf.len,
                        null,
                    );
                    // Update the consumer and NACK metadata.
                    nack.consumer = consumer;
                    nack.delivery_time = std.time.milliTimestamp();
                    nack.delivery_count = 1;
                    // Add the entry in the new consumer local PEL.
                    _ = raxlib.raxInsert(
                        consumer.?.pel,
                        &buf,
                        buf.len,
                        nack,
                        null,
                    );
                } else if (group_inserted == 1 and consumer_inserted == 0) {
                    @panic("NACK half-created. Should not be possible.");
                }
            }

            arraylen += 1;
            if (count > 0 and arraylen == count) {
                break;
            }
        }

        if (replylen) |reply| {
            cli.setDeferredMultiBulkLength(reply, @intCast(arraylen));
        }
        return arraylen;
    }

    /// This is an helper function for replyWithRange() when called with
    /// group and consumer arguments, but with a range that is referring to already
    /// delivered messages. In this case we just emit messages that are already
    /// in the history of the consumer, fetching the IDs from its PEL.
    ///
    /// Note that this function does not have a 'rev' argument because it's not
    /// possible to iterate in reverse using a group. Basically this function
    /// is only called as a result of the XREADGROUP command.
    ///
    /// This function is more expensive because it needs to inspect the PEL and then
    /// seek into the radix tree of the messages in order to emit the full message
    /// to the client. However clients only reach this code path when they are
    /// fetching the history of already retrieved messages, which is rare.
    fn replyWithRangeFromConsumerPEL(
        self: *Stream,
        cli: *CLient,
        start: *Id,
        end: ?*Id,
        count: usize,
        consumer: *Consumer,
    ) usize {
        var startkey: [@sizeOf(Id)]u8 = undefined;
        start.encode(&startkey);

        const replylen = cli.addDeferredMultiBulkLength();

        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, consumer.pel);
        _ = raxlib.raxSeek(&ri, ">=", &startkey, startkey.len);
        defer raxlib.raxStop(&ri);

        var arraylen: usize = 0;
        while (raxlib.raxNext(&ri) != 0 and (count == 0 or arraylen < count)) {
            var thisid: Id = undefined;
            thisid.decode(ri.key[0..ri.key_len]);

            if (end != null and thisid.compare(end.?) == .gt) {
                break;
            }

            if (self.replyWithRange(
                cli,
                &thisid,
                &thisid,
                1,
                false,
                null,
                null,
                RWR_RAWENTRIES,
                null,
            ) == 0) {
                // Note that we may have a not acknowledged entry in the PEL
                // about a message that's no longer here because was removed
                // by the user by other means. In that case we signal it emitting
                // the ID but then a NULL entry for the fields.
                cli.addReplyMultiBulkLen(2);
                cli.addReplyStreamID(&thisid);
                cli.addReply(Server.shared.nullmultibulk);
            } else {
                const nack: *NACK = .cast(ri.data.?);
                nack.delivery_time = std.time.milliTimestamp();
                nack.delivery_count +%= 1;
            }
            arraylen += 1;
        }
        cli.setDeferredMultiBulkLength(replylen, @intCast(arraylen));
        return arraylen;
    }

    pub inline fn cast(ptr: *anyopaque) *Stream {
        return @ptrCast(@alignCast(ptr));
    }

    /// Destroy a stream, including the listpacks stored inside the radix tree.
    pub fn destroy(self: *Stream) void {
        raxlib.raxFreeWithCallback(self.rax, freeListPackCallback);
        if (self.cgroups) |cgroups| {
            raxlib.raxFreeWithCallback(cgroups, freeCGCallback);
        }
        allocator.destroy(self);
    }

    fn freeListPackCallback(ptr: ?*anyopaque) callconv(.c) void {
        const lp: *ListPack = ListPack.cast(ptr.?);
        lp.free();
    }

    fn freeCGCallback(ptr: ?*anyopaque) callconv(.c) void {
        const cg: *CG = CG.cast(ptr.?);
        cg.destroy();
    }
};

const std = @import("std");
const CLient = @import("networking.zig").Client;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const eqlCase = std.ascii.eqlIgnoreCase;
const memlib = @import("mem.zig");
const memcpy = memlib.memcpy;
const maxInt = std.math.maxInt;
const util = @import("util.zig");
const Server = @import("Server.zig");
const server = &Server.instance;
const blocked = @import("blocked.zig");
const raxlib = @import("rax.zig").rax;
const ListPack = @import("ListPack.zig");
const assert = std.debug.assert;
const allocator = @import("allocator.zig");
const writeInt = std.mem.writeInt;
const readInt = std.mem.readInt;
const memcmp = memlib.memcmp;
const sliceAsBytes = std.mem.sliceAsBytes;
