pub fn bgrewriteaofCommand(cli: *Client) void {
    if (server.aof_child_pid != -1) {
        cli.addReplyErr(
            "Background append only file rewriting already in progress",
        );
    } else if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = true;
        cli.addReplyStatus("Background append only file rewriting scheduled");
    } else if (rewriteAppendOnlyFileBackground()) {
        cli.addReplyStatus("Background append only file rewriting started");
    } else {
        cli.addReply(Server.shared.err);
    }
}

/// This is how rewriting of the append only file in background works:
///
/// 1) The user calls BGREWRITEAOF
/// 2) Redis calls this function, that forks():
///    2a) the child rewrite the append only file in a temp file.
///    2b) the parent accumulates differences in server.aof_rewrite_buf.
/// 3) When the child finished '2a' exists.
/// 4) The parent will trap the exit code, if it's OK, will append the
///    data accumulated into server.aof_rewrite_buf into the temp file, and
///    finally will rename(2) the temp file in the actual file name.
///    The the new file is reopened as the new append only file. Profit!
pub fn rewriteAppendOnlyFileBackground() bool {
    if (server.aof_child_pid != -1 or server.rdb_child_pid != -1) {
        return false;
    }
    if (!createPipes()) return false;
    childinfo.openPipe();
    const childpid = posix.fork() catch |err| {
        childinfo.closePipe();
        logging.warn(
            "Can't rewrite append only file in background: fork: {}",
            .{err},
        );
        closePipes();
        return false;
    };
    if (childpid == 0) {
        // Child
        var tmpfile: [256]u8 = undefined;
        Server.closeChildUnusedResourceAfterFork();
        Server.setProcTitle("redis-aof-rewrite");
        const filename = std.fmt.bufPrint(
            &tmpfile,
            "temp-rewriteaof-bg-{}.aof",
            .{libc.getpid()},
        ) catch unreachable;
        if (rewriteAppendOnlyFile(filename)) {
            childinfo.send(Server.CHILD_INFO_TYPE_AOF);
            Server.exitFromChild(0);
        } else {
            Server.exitFromChild(1);
        }
    } else {
        // Parent
        logging.notice(
            "Background append only file rewriting started by pid {}",
            .{childpid},
        );
        server.aof_rewrite_scheduled = false;
        server.aof_rewrite_time_start = std.time.timestamp();
        server.aof_child_pid = childpid;
        Server.updateDictResizePolicy();
        // We set appendseldb to -1 in order to force the next call to the
        // feedAppendOnlyFile() to issue a SELECT command, so the differences
        // accumulated by the parent into server.aof_rewrite_buf will start
        // with a SELECT statement and it will be safe to merge.
        server.aof_selected_db = -1;
        return true;
    }
    unreachable;
}

/// Create the pipes used for parent - child process IPC during rewrite.
/// We have a data pipe used to send AOF incremental diffs to the child,
/// and two other pipes used by the children to signal it finished with
/// the rewrite so no more data should be written, and another for the
/// parent to acknowledge it understood this new condition.
fn createPipes() bool {
    var fds: [6]posix.fd_t = .{ -1, -1, -1, -1, -1, -1 };
    var errgot: anyerror = undefined;

    biz: {
        // parent -> children data.
        const data = posix.pipe() catch |err| {
            errgot = err;
            break :biz;
        };
        fds[0] = data[0];
        fds[1] = data[1];

        // children -> parent ack.
        const c2p = posix.pipe() catch |err| {
            errgot = err;
            break :biz;
        };
        fds[2] = c2p[0];
        fds[3] = c2p[1];

        // parent -> children ack.
        const p2c = posix.pipe() catch |err| {
            errgot = err;
            break :biz;
        };
        fds[4] = p2c[0];
        fds[5] = p2c[1];

        // Parent -> children data is non blocking.
        anet.nonBlock(fds[0]) catch |err| {
            errgot = err;
            break :biz;
        };
        anet.nonBlock(fds[1]) catch |err| {
            errgot = err;
            break :biz;
        };
        server.el.createFileEvent(
            fds[2],
            ae.READABLE,
            childPipeReadable,
            null,
        ) catch |err| {
            errgot = err;
            break :biz;
        };
        server.aof_pipe_write_data_to_child = fds[1];
        server.aof_pipe_read_data_from_parent = fds[0];
        server.aof_pipe_write_ack_to_parent = fds[3];
        server.aof_pipe_read_ack_from_child = fds[2];
        server.aof_pipe_write_ack_to_child = fds[5];
        server.aof_pipe_read_ack_from_parent = fds[4];
        server.aof_stop_sending_diff = false;
        return true;
    }

    // handle error
    logging.warn(
        "Error opening /setting AOF rewrite IPC pipes: {}",
        .{errgot},
    );
    for (fds) |fd| if (fd != -1) posix.close(fd);
    return false;
}

fn closePipes() void {
    server.el.deleteFileEvent(
        server.aof_pipe_read_ack_from_child,
        ae.READABLE,
    ) catch {};
    server.el.deleteFileEvent(
        server.aof_pipe_write_data_to_child,
        ae.WRITABLE,
    ) catch {};
    posix.close(server.aof_pipe_read_ack_from_child);
    posix.close(server.aof_pipe_read_ack_from_parent);
    posix.close(server.aof_pipe_read_data_from_parent);
    posix.close(server.aof_pipe_write_ack_to_child);
    posix.close(server.aof_pipe_write_ack_to_parent);
    posix.close(server.aof_pipe_write_data_to_child);
}

/// Write a sequence of commands able to fully rebuild the dataset into
/// "filename". Used both by REWRITEAOF and BGREWRITEAOF.
///
/// In order to minimize the number of commands needed in the rewritten
/// log Redis uses variadic commands when possible, such as RPUSH, SADD
/// and ZADD. However at max AOF_REWRITE_ITEMS_PER_CMD items per time
/// are inserted using a single command.
fn rewriteAppendOnlyFile(filename: []const u8) bool {
    var errgot: anyerror = undefined;

    // Note that we have to use a different temp name here compared to the
    // one used by rewriteAppendOnlyFileBackground() function.
    var tmpbuf: [256]u8 = undefined;
    const tmpfile = std.fmt.bufPrint(
        &tmpbuf,
        "temp-rewriteaof-{}.aof",
        .{libc.getpid()},
    ) catch unreachable;
    const fp = std.fs.cwd().createFile(
        tmpfile,
        .{ .truncate = true },
    ) catch |err| {
        logging.warn(
            "Opening the temp file '{s}' for AOF rewrite in rewriteAppendOnlyFile(): {}",
            .{ tmpfile, err },
        );
        return false;
    };
    server.aof_child_diff = sds.empty(allocator.impl);
    var fio: rio.FileReadWriter = undefined;
    fio.init(fp);

    biz: {
        if (server.aof_rewrite_incremental_fsync) {
            fio.setAutoSync(Server.REDIS_AUTOSYNC_BYTES);
        }
        var aof = fio.readWriter();
        if (server.aof_use_rdb_preamble) {
            rdb.saveRio(&aof, rdb.RDB_SAVE_AOF_PREAMBLE) catch |err| {
                errgot = err;
                break :biz;
            };
        } else {
            rewriteAppendOnlyFileRio(&aof) catch |err| {
                errgot = err;
                break :biz;
            };
        }

        // Do an initial slow fsync here while the parent is still sending
        // data, in order to make the next final fsync faster.
        aof.flush() catch |err| {
            errgot = err;
            break :biz;
        };
        posix.fsync(fp.handle) catch |err| {
            errgot = err;
            break :biz;
        };

        // Read again a few times to get more data from the parent.
        // We can't read forever (the server may receive data from clients
        // faster than it is able to send data to the child), so we try to read
        // some more data in a loop as soon as there is a good chance more data
        // will come. If it looks like we are wasting time, we abort (this
        // happens after 20 ms without new data).
        var nodata: usize = 0;
        const start = std.time.milliTimestamp();
        while (std.time.milliTimestamp() - start < 1000 and nodata < 20) {
            const ready = ae.wait(
                server.aof_pipe_read_data_from_parent,
                ae.READABLE,
                1,
            ) catch 0;
            if (ready <= 0) {
                nodata += 1;
                continue;
            }

            // Start counting from zero, we stop on N *contiguous* timeouts.
            nodata = 0;
            readDiffFromParent();
        }

        // Ask the master to stop sending diffs.
        _ = posix.write(server.aof_pipe_write_ack_to_parent, "!") catch |err| {
            errgot = err;
            break :biz;
        };
        anet.nonBlock(server.aof_pipe_read_ack_from_parent) catch |err| {
            errgot = err;
            break :biz;
        };
        // We read the ACK from the server using a 10 seconds timeout. Normally
        // it should reply ASAP, but just in case we lose its reply, we are sure
        // the child will eventually get terminated.
        var byte: u8 = undefined;
        const nread = syncio.read(
            server.aof_pipe_read_ack_from_parent,
            std.mem.asBytes(&byte),
            5000,
        ) catch |err| {
            errgot = err;
            break :biz;
        };
        if (nread != 1 or byte != '!') {
            errgot = error.InvalidACK;
            break :biz;
        }
        logging.notice(
            "Parent agreed to stop sending diffs. Finalizing AOF...",
            .{},
        );

        // Read the final diff if any.
        readDiffFromParent();

        // Write the received diff to the file.
        logging.notice(
            "Concatenating {d:.2} MB of AOF diff received from parent.",
            .{@as(f64, @floatFromInt(sds.getLen(server.aof_child_diff))) / (1024 / 1024)},
        );
        aof.write(sds.asBytes(server.aof_child_diff)) catch |err| {
            errgot = err;
            break :biz;
        };

        // Make sure data will not remain on the OS's output buffers
        aof.flush() catch |err| {
            errgot = err;
            break :biz;
        };
        posix.fsync(fp.handle) catch |err| {
            errgot = err;
            break :biz;
        };
        fp.close();

        // Use RENAME to make sure the DB file is changed atomically only
        // if the generate DB file is ok.
        std.fs.cwd().rename(tmpfile, filename) catch |err| {
            logging.warn(
                "Error moving temp append only file on the final destination: {}",
                .{err},
            );
            std.fs.cwd().deleteFile(tmpfile) catch {};
            return false;
        };
        logging.notice("SYNC append only file rewrite performed", .{});
        return true;
    }

    // handle error
    logging.warn(
        "Write error writing append only file on disk: {}",
        .{errgot},
    );
    fp.close();
    std.fs.cwd().deleteFile(tmpfile) catch {};
    return false;
}

fn rewriteAppendOnlyFileRio(aof: *rio.ReadWriter) !void {
    var processed: usize = 0;

    for (server.db, 0..) |*db, j| {
        if (db.dict.size() == 0) continue;
        var di = db.dict.iterator(true);
        defer di.release();

        // SELECT the new DB
        const selectcmd = "*2\r\n$6\r\nSELECT\r\n";
        try aof.write(selectcmd);
        try aof.writeBulkLongLong(@intCast(j));

        // Iterate this DB writing every entry
        while (di.next()) |de| {
            var key: Object = undefined;
            key.initStaticStringObject(de.key);
            const val = de.val.?;
            const expiretime = db.getExpire(&key);
            try switch (val.type) {
                .string => rewriteStringObject(aof, &key, val),
                .list => rewriteListObject(aof, &key, val),
                .set => rewriteSetObject(aof, &key, val),
                .zset => rewriteSortedSetObject(aof, &key, val),
                .hash => rewriteHashObject(aof, &key, val),
                .stream => rewriteStreamObject(aof, &key, val),
                else => @panic("Unknown object type"),
            };
            // Save the expire time
            if (expiretime != -1) {
                const cmd = "*3\r\n$9\r\nPEXPIREAT\r\n";
                try aof.write(cmd);
                try aof.writeBulkObject(&key);
                try aof.writeBulkLongLong(expiretime);
            }
            // Read some diff from the parent process from time to time.
            if (aof.processed_bytes > processed + Server.AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = aof.processed_bytes;
                readDiffFromParent();
            }
        }
    }
}

/// Emit the commands needed to rebuild a string object.
fn rewriteStringObject(aof: *rio.ReadWriter, key: *Object, val: *Object) !void {
    // Emit a SET command
    const cmd = "*3\r\n$3\r\nSET\r\n";
    try aof.write(cmd);
    // Key and valud
    try aof.writeBulkObject(key);
    try aof.writeBulkObject(val);
}

/// Emit the commands needed to rebuild a list object.
fn rewriteListObject(aof: *rio.ReadWriter, key: *Object, val: *Object) !void {
    if (val.encoding == .quicklist) {
        var ql = QuickList.cast(val.v.ptr);
        var items = ql.count;
        var count: usize = 0;
        var it = ql.iterator(.head);
        defer it.release();
        var entry: QuickList.Entry = undefined;
        while (it.next(&entry)) {
            if (count == 0) {
                const cmd_items: u64 = @min(
                    items,
                    Server.AOF_REWRITE_ITEMS_PER_CMD,
                );
                try aof.writeBulkCount('*', @intCast(cmd_items + 2));
                try aof.writeBulkString("RPUSH");
                try aof.writeBulkObject(key);
            }
            if (entry.value) |value| {
                try aof.writeBulkString(value[0..entry.sz]);
            } else {
                try aof.writeBulkLongLong(entry.longval);
            }
            count += 1;
            if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
                count = 0;
            }
            items -= 1;
        }
    } else {
        @panic("Unknown list encoding");
    }
}

/// Emit the commands needed to rebuild a set object.
fn rewriteSetObject(aof: *rio.ReadWriter, key: *Object, val: *Object) !void {
    const Set = @import("t_set.zig").Set;
    var items = Set.size(val);
    var count: usize = 0;

    if (val.encoding == .intset) {
        var is = IntSet.cast(val.v.ptr);
        var ii: u32 = 0;
        while (is.get(ii)) |llval| : (ii += 1) {
            if (count == 0) {
                const cmd_items: u64 = @min(
                    items,
                    Server.AOF_REWRITE_ITEMS_PER_CMD,
                );
                try aof.writeBulkCount('*', @intCast(cmd_items + 2));
                try aof.writeBulkString("SADD");
                try aof.writeBulkObject(key);
            }
            try aof.writeBulkLongLong(llval);
            count += 1;
            if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
                count = 0;
            }
            items -= 1;
        }
    } else if (val.encoding == .ht) {
        var set = Set.Hash.cast(val.v.ptr);
        var di = set.iterator(false);
        defer di.release();
        while (di.next()) |entry| {
            const ele = entry.key;
            if (count == 0) {
                const cmd_items: u64 = @min(
                    items,
                    Server.AOF_REWRITE_ITEMS_PER_CMD,
                );
                try aof.writeBulkCount('*', @intCast(cmd_items + 2));
                try aof.writeBulkString("SADD");
                try aof.writeBulkObject(key);
            }
            try aof.writeBulkString(sds.asBytes(ele));
            count += 1;
            if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
                count = 0;
            }
            items -= 1;
        }
    } else {
        @panic("Unknown set encoding");
    }
}

/// Emit the commands needed to rebuild a sorted set object.
fn rewriteSortedSetObject(
    aof: *rio.ReadWriter,
    key: *Object,
    val: *Object,
) !void {
    const ZSet = @import("t_zset.zig").Zset;
    var items = ZSet.length(val);
    var count: usize = 0;

    if (val.encoding == .ziplist) {
        const ZipListSet = @import("t_zset.zig").ZipListSet;
        const zl = ZipListSet.cast(val.v.ptr);
        var eptr: ?[*]u8 = undefined;
        var sptr: ?[*]u8 = undefined;

        eptr = zl.zl.index(0);
        assert(eptr != null);
        sptr = zl.zl.next(eptr.?);
        assert(sptr != null);
        while (eptr) |ep| {
            const member = ZipList.get(ep).?;
            const score = ZipListSet.getScore(sptr.?);
            if (count == 0) {
                const cmd_items: u64 = @min(
                    items,
                    Server.AOF_REWRITE_ITEMS_PER_CMD,
                );
                try aof.writeBulkCount('*', @intCast(cmd_items * 2 + 2));
                try aof.writeBulkString("ZADD");
                try aof.writeBulkObject(key);
            }
            try aof.writeBulkDouble(score);
            try switch (member) {
                .num => |v| aof.writeBulkLongLong(v),
                .str => |v| aof.writeBulkString(v),
            };
            zl.next(&eptr, &sptr);
            count += 1;
            if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
                count = 0;
            }
            items -= 1;
        }
    } else if (val.encoding == .skiplist) {
        const SkipListSet = @import("t_zset.zig").SkipListSet;
        const sl = SkipListSet.cast(val.v.ptr);
        var di = sl.dict.iterator(true);
        defer di.release();
        while (di.next()) |entry| {
            const member = entry.key;
            const score = entry.val;
            if (count == 0) {
                const cmd_items: u64 = @min(
                    items,
                    Server.AOF_REWRITE_ITEMS_PER_CMD,
                );
                try aof.writeBulkCount('*', @intCast(cmd_items * 2 + 2));
                try aof.writeBulkString("ZADD");
                try aof.writeBulkObject(key);
            }
            try aof.writeBulkDouble(score.*);
            try aof.writeBulkString(sds.asBytes(member));
            count += 1;
            if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
                count = 0;
            }
            items -= 1;
        }
    } else {
        @panic("Unknown sorted zset encoding");
    }
}

/// Emit the commands needed to rebuild a hash object.
fn rewriteHashObject(aof: *rio.ReadWriter, key: *Object, val: *Object) !void {
    var items = Hash.length(val);
    var count: usize = 0;

    var hi = Hash.Iterator.create(val);
    defer hi.release();
    while (hi.next()) {
        if (count == 0) {
            const cmd_items: u64 = @min(
                items,
                Server.AOF_REWRITE_ITEMS_PER_CMD,
            );
            try aof.writeBulkCount('*', @intCast(cmd_items * 2 + 2));
            try aof.writeBulkString("HMSET");
            try aof.writeBulkObject(key);
        }
        try rioWriteHashIteratorCursor(aof, &hi, .key);
        try rioWriteHashIteratorCursor(aof, &hi, .val);
        count += 1;
        if (count == Server.AOF_REWRITE_ITEMS_PER_CMD) {
            count = 0;
        }
        items -= 1;
    }
}

/// Write either the key or the value of the currently selected item of a hash.
/// The 'hi' argument passes a valid Redis hash iterator.
///
/// The 'what' filed specifies if to write a key or a value.
fn rioWriteHashIteratorCursor(
    aof: *rio.ReadWriter,
    hi: *Hash.Iterator,
    what: enum { key, val },
) !void {
    if (hi.encoding == .ziplist) {
        if (what == .key) {
            try switch (hi.currentKeyFromZipList()) {
                .num => |v| aof.writeBulkLongLong(v),
                .str => |v| aof.writeBulkString(v),
            };
        } else {
            try switch (hi.currentValFromZipList()) {
                .num => |v| aof.writeBulkLongLong(v),
                .str => |v| aof.writeBulkString(v),
            };
        }
    } else if (hi.encoding == .ht) {
        if (what == .key) {
            const key = hi.currentKeyFromHashMap();
            try aof.writeBulkString(sds.asBytes(key));
        } else {
            const val = hi.currentValFromHashMap();
            try aof.writeBulkString(sds.asBytes(val));
        }
    } else {
        @panic("Unknown hash encoding");
    }
}

/// Emit the commands needed to rebuild a stream object.
fn rewriteStreamObject(aof: *rio.ReadWriter, key: *Object, val: *Object) !void {
    const s = Stream.cast(val.v.ptr);
    var si: Stream.Iterator = undefined;
    si.start(s, null, null, false);
    defer si.stop();

    var id: Stream.Id = undefined;
    var numfields: i64 = undefined;

    if (s.length != 0) {
        // Reconstruct the stream data using XADD commands.
        while (si.getId(&id, &numfields)) {
            // Emit a two elements array for each item. The first is
            // the ID, the second is an array of field-value pairs.
            // Emit the XADD <key> <id> ...fields... command.
            try aof.writeBulkCount('*', numfields * 2 + 3);
            try aof.writeBulkString("XADD");
            try aof.writeBulkObject(key);
            try aof.writeBulkStreamID(&id);
            while (numfields > 0) : (numfields -= 1) {
                var field: []u8 = undefined;
                var value: []u8 = undefined;
                si.getField(&field, &value);
                try aof.writeBulkString(field);
                try aof.writeBulkString(value);
            }
        }
    } else {
        try aof.writeBulkCount('*', 7);
        try aof.writeBulkString("XADD");
        try aof.writeBulkObject(key);
        try aof.writeBulkString("MAXLEN");
        try aof.writeBulkString("0");
        try aof.writeBulkStreamID(&s.last_id);
        try aof.writeBulkString("x");
        try aof.writeBulkString("y");
    }

    // Append XSETID after XADD, make sure lastid is correct,
    // in case of XDEL lastid.
    try aof.writeBulkCount('*', 3);
    try aof.writeBulkString("XSETID");
    try aof.writeBulkObject(key);
    try aof.writeBulkStreamID(&s.last_id);

    // Create all the stream consumer groups.
    if (s.cgroups) |cgroups| {
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, cgroups);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "^", null, 0);
        while (raxlib.raxNext(&ri) != 0) {
            const group = Stream.CG.cast(ri.data.?);
            // Emit the XGROUP CREATE in order to create the group.
            try aof.writeBulkCount('*', 5);
            try aof.writeBulkString("XGROUP");
            try aof.writeBulkString("CREATE");
            try aof.writeBulkObject(key);
            try aof.writeBulkString(ri.key[0..ri.key_len]);
            try aof.writeBulkStreamID(&group.last_id);

            // Generate XCLAIMs for each consumer that happens to
            // have pending entries. Empty consumers have no semantical
            // value so they are discarded.
            var ri_cons: raxlib.raxIterator = undefined;
            raxlib.raxStart(&ri_cons, group.consumers);
            defer raxlib.raxStop(&ri_cons);
            _ = raxlib.raxSeek(&ri_cons, "^", null, 0);
            while (raxlib.raxNext(&ri_cons) != 0) {
                const consumer = Stream.Consumer.cast(ri_cons.data.?);
                // For the current consumer, iterate all the PEL entries
                // to emit the XCLAIM protocol.
                var ri_pel: raxlib.raxIterator = undefined;
                raxlib.raxStart(&ri_pel, consumer.pel);
                defer raxlib.raxStop(&ri_pel);
                _ = raxlib.raxSeek(&ri_pel, "^", null, 0);
                while (raxlib.raxNext(&ri_pel) != 0) {
                    const nack = Stream.NACK.cast(ri_pel.data.?);
                    try rioWriteStreamPendingEntry(
                        aof,
                        key,
                        ri.key[0..ri.key_len],
                        consumer,
                        ri_pel.key[0..ri_pel.key_len],
                        nack,
                    );
                }
            }
        }
    }
}

// Helper for rewriteStreamObject(): emit the XCLAIM needed in order to
// add the message described by 'nack' having the id 'rawid', into the pending
// list of the specified consumer. All this in the context of the specified
// key and group.
fn rioWriteStreamPendingEntry(
    aof: *rio.ReadWriter,
    key: *Object,
    groupname: []const u8,
    consumer: *Stream.Consumer,
    rawid: []const u8,
    nack: *Stream.NACK,
) !void {
    // XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
    // RETRYCOUNT <count> JUSTID FORCE.
    var id: Stream.Id = undefined;
    id.decode(rawid);
    try aof.writeBulkCount('*', 12);
    try aof.writeBulkString("XCLAIM");
    try aof.writeBulkObject(key);
    try aof.writeBulkString(groupname);
    try aof.writeBulkString(sds.asBytes(consumer.name));
    try aof.writeBulkString("0");
    try aof.writeBulkStreamID(&id);
    try aof.writeBulkString("TIME");
    try aof.writeBulkLongLong(nack.delivery_time);
    try aof.writeBulkString("RETRYCOUNT");
    try aof.writeBulkLongLong(@intCast(nack.delivery_count));
    try aof.writeBulkString("JUSTID");
    try aof.writeBulkString("FORCE");
}

/// Append data to the AOF rewrite buffer, allocating new blocks if needed.
fn rewriteBufferAppend(s: []const u8) void {
    const ln = server.aof_rewrite_buf_blocks.?.last;
    var block = if (ln) |l| l.value else null;

    var len = s.len;
    var ptr = s.ptr;

    while (len > 0) {
        // If we already got at least an allocated block, try appending
        // at least some piece into it.
        if (block) |blk| {
            const thislen = @min(blk.free, len);
            if (thislen != 0) { // The current block is not already full.
                memcpy(blk.buf[blk.used..], ptr, thislen);
                blk.used += thislen;
                blk.free -= thislen;
                ptr += thislen;
                len -= thislen;
            }
        }

        // First block to allocate, or need another block.
        if (len > 0) {
            block = RwBlock.create();
            server.aof_rewrite_buf_blocks.?.append(block.?);

            // Log every time we cross more 10 or 100 blocks, respectively
            // as a notice or warning.
            const numbloks = server.aof_rewrite_buf_blocks.?.len;
            if (@rem(numbloks + 1, 10) == 0) {
                const level = if (@rem(numbloks + 1, 100) == 0)
                    logging.Level.warning
                else
                    logging.Level.notice;
                logging.raw(
                    level,
                    "Background AOF buffer size: {} MB",
                    .{@divFloor(rewriteBufferSize(), 1024 * 1024)},
                );
            }
        }
    }

    // Install a file event to send data to the rewrite child if there is
    // not one already.
    if (server.el.getFileEvents(server.aof_pipe_write_data_to_child) == 0) {
        server.el.createFileEvent(
            server.aof_pipe_write_data_to_child,
            ae.WRITABLE,
            childWriteDiffData,
            null,
        ) catch {};
    }
}

/// Return the current size of the AOF rewrite buffer.
fn rewriteBufferSize() u64 {
    var size: u64 = 0;
    var it = server.aof_rewrite_buf_blocks.?.iterator(.forward);
    while (it.next()) |node| {
        size += node.value.used;
    }
    return size;
}

/// This function free the old AOF rewrite buffer if needed, and initialize
/// a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
/// so can be used for the first initialization as well.
pub fn rewriteBufferReset() void {
    if (server.aof_rewrite_buf_blocks) |blocks| {
        blocks.release();
    }
    server.aof_rewrite_buf_blocks = .create(&.{
        .freeVal = RwBlock.destroy,
    });
}

/// This event handler is called when the AOF rewriting child sends us a
/// single '!' char to signal we should stop sending buffer diffs. The
/// parent sends a '!' as well to acknowledge.
fn childPipeReadable(
    el: *ae.EventLoop,
    fd: i32,
    privdata: ?*anyopaque,
    mask: i32,
) anyerror!void {
    _ = el;
    _ = privdata;
    _ = mask;
    var byte: u8 = undefined;
    // Remove the handler since this can be called only one time during a
    // rewrite.
    defer {
        server.el.deleteFileEvent(
            server.aof_pipe_read_ack_from_child,
            ae.READABLE,
        ) catch {};
    }

    const nread = posix.read(fd, std.mem.asBytes(&byte)) catch {
        return;
    };
    if (nread == 1 and byte == '!') {
        logging.notice("AOF rewrite child asks to stop sending diffs.", .{});
        server.aof_stop_sending_diff = true;
        _ = posix.write(server.aof_pipe_write_ack_to_child, &.{'!'}) catch |err| {
            // If we can't send the ack, inform the user, but don't try again
            // since in the other side the children will use a timeout if the
            // kernel can't buffer our write, or, the children was
            // terminated.
            logging.warn("Can't send ACK to AOF child: {}", .{err});
        };
    }
}

/// Event handler used to send data to the child process doing the AOF
/// rewrite. We send pieces of our AOF differences buffer so that the final
/// write when the child finishes the rewrite will be small.
fn childWriteDiffData(
    el: *ae.EventLoop,
    fd: i32,
    privdata: ?*anyopaque,
    mask: i32,
) anyerror!void {
    _ = el;
    _ = fd;
    _ = privdata;
    _ = mask;

    while (true) {
        const ln = server.aof_rewrite_buf_blocks.?.first;
        const block = if (ln) |l| l.value else null;
        if (server.aof_stop_sending_diff or block == null) {
            try server.el.deleteFileEvent(
                server.aof_pipe_write_data_to_child,
                ae.WRITABLE,
            );
            return;
        }
        if (block.?.used > 0) {
            const nwritten = posix.write(
                server.aof_pipe_write_data_to_child,
                block.?.buf[0..block.?.used],
            ) catch {
                return;
            };
            memmove(
                block.?.buf[0..],
                block.?.buf[nwritten..],
                block.?.used - nwritten,
            );
            block.?.used -= nwritten;
            block.?.free += nwritten;
        }
        if (block.?.used == 0) {
            server.aof_rewrite_buf_blocks.?.removeNode(ln.?);
        }
    }
}

/// This function is called by the child rewriting the AOF file to read
/// the difference accumulated from the parent into a buffer, that is
/// concatenated at the end of the rewrite.
pub fn readDiffFromParent() void {
    var buf: [65536]u8 = undefined;
    while (true) {
        const nread = posix.read(
            server.aof_pipe_read_data_from_parent,
            &buf,
        ) catch {
            break;
        };
        if (nread == 0) {
            // EOF
            break;
        }
        server.aof_child_diff = sds.cat(
            allocator.impl,
            server.aof_child_diff,
            buf[0..nread],
        );
    }
}

/// A background append only file rewriting (BGREWRITEAOF) terminated its work.
/// Handle this.
pub fn backgroundRewriteDoneHandler(exitcode: c_int, bysignal: c_int) void {
    // cleanup
    defer {
        closePipes();
        rewriteBufferReset();
        removeTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_last = std.time.timestamp() - server.aof_rewrite_time_start;
        server.aof_rewrite_time_start = -1;
        // Schedule a new rewrite if we are waiting for it to switch the AOF ON.
        if (server.aof_state == Server.AOF_WAIT_REWRITE) {
            server.aof_rewrite_scheduled = true;
        }
    }

    if (bysignal == 0 and exitcode == 0) {
        const now = std.time.microTimestamp();
        var tmpbuf: [256]u8 = undefined;

        logging.notice("Background AOF rewrite terminated with success", .{});
        // Flush the differences accumulated by the parent to the
        // rewritten AOF.
        const tmpfile = std.fmt.bufPrint(
            &tmpbuf,
            "temp-rewriteaof-bg-{}.aof",
            .{server.aof_child_pid},
        ) catch unreachable;

        const newfd = posix.open(
            tmpfile,
            .{
                .ACCMODE = .WRONLY,
                .APPEND = true,
            },
            0o644,
        ) catch |err| {
            logging.warn(
                "Unable to open the temporary AOF produced by the child: {}",
                .{err},
            );
            return;
        };
        rewriteBufferWrite(newfd) catch |err| {
            logging.warn(
                "Error trying to flush the parent diff to the rewritten AOF: {}",
                .{err},
            );
            posix.close(newfd);
            return;
        };

        logging.notice(
            "Residual parent diff successfully flushed to the rewritten AOF ({:.2} MB)",
            .{@as(f64, @floatFromInt(rewriteBufferSize())) / 1024 * 1024},
        );

        // The only remaining thing to do is to rename the temporary file to
        // the configured file and switch the file descriptor used to do AOF
        // writes. We don't want close(2) or rename(2) calls to block the
        // server on old file deletion.
        //
        // There are two possible scenarios:
        //
        // 1) AOF is DISABLED and this was a one time rewrite. The temporary
        // file will be renamed to the configured file. When this file already
        // exists, it will be unlinked, which may block the server.
        //
        // 2) AOF is ENABLED and the rewritten AOF will immediately start
        // receiving writes. After the temporary file is renamed to the
        // configured file, the original AOF file descriptor will be closed.
        // Since this will be the last reference to that file, closing it
        // causes the underlying file to be unlinked, which may block the
        // server.
        //
        // To mitigate the blocking effect of the unlink operation (either
        // caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
        // use a background thread to take care of this. First, we
        // make scenario 1 identical to scenario 2 by opening the target file
        // when it exists. The unlink operation after the rename(2) will then
        // be executed upon calling close(2) for its descriptor. Everything to
        // guarantee atomicity for this switch has already happened by then, so
        // we don't care what the outcome or duration of that close operation
        // is, as long as the file descriptor is released again.
        var oldfd: posix.pid_t = undefined;
        if (server.aof_fd == -1) {
            // AOF disabled
            //
            // Don't care if this fails: oldfd will be -1 and we handle that.
            // One notable case of -1 return is if the old file does
            // not exist.
            oldfd = posix.open(
                server.aof_filename,
                .{
                    .ACCMODE = .RDONLY,
                    .NONBLOCK = true,
                },
                0o644,
            ) catch -1;
        } else {
            // AOF enabled.
            oldfd = -1; // We'll set this to the current AOF filedes later.
        }

        // Rename the temporary file. This will not unlink the target file if
        // it exists, because we reference it with "oldfd".
        posix.rename(tmpfile, server.aof_filename) catch |err| {
            logging.warn(
                "Error trying to rename the temporary AOF file {s} into {s}: {}",
                .{ tmpfile, server.aof_filename, err },
            );
            posix.close(newfd);
            if (oldfd != -1) posix.close(oldfd);
            return;
        };

        if (server.aof_fd == -1) {
            // AOF disabled, we don't need to set the AOF file descriptor
            // to this new file, so we can close it.
            posix.close(newfd);
        } else {
            // AOF enabled, replace the old fd with the new one.
            oldfd = server.aof_fd;
            server.aof_fd = newfd;
            if (server.aof_fsync == Server.AOF_FSYNC_ALWAYS) {
                config.fsync(newfd) catch {};
            } else if (server.aof_fsync == Server.AOF_FSYNC_EVERYSEC) {
                backgroundFsync(newfd);
            }
            server.aof_selected_db = -1; // Make sure SELECT is re-issued
            updateCurrentSize();
            server.aof_rewrite_base_size = server.aof_current_size;
            server.aof_fsync_offset = server.aof_current_size;

            // Clear regular AOF buffer since its contents was just written to
            // the new AOF from the background rewrite buffer.
            sds.free(allocator.impl, server.aof_buf);
            server.aof_buf = sds.empty(allocator.impl);
        }

        server.aof_lastbgrewrite_status = true;

        logging.notice("Background AOF rewrite finished successfully", .{});
        // Change state from WAIT_REWRITE to ON if needed
        if (server.aof_state == Server.AOF_WAIT_REWRITE) {
            server.aof_state = Server.AOF_ON;
        }

        // Asynchronously close the overwritten AOF.
        if (oldfd != -1) {
            bio.createBackgroundJob(
                .closeFile,
                @ptrFromInt(@as(usize, @intCast(oldfd))),
                null,
                null,
            );
        }
        logging.notice(
            "Background AOF rewrite signal handler took {}us",
            .{std.time.microTimestamp() - now},
        );
    } else if (bysignal == 0 and exitcode != 0) {
        server.aof_lastbgrewrite_status = false;
        logging.warn("Background AOF rewrite terminated with error", .{});
    } else {
        // SIGUSR1 is whitelisted, so we have a way to kill a child without
        // tirggering an error condition.
        if (bysignal != posix.SIG.USR1) {
            server.aof_lastbgrewrite_status = false;
        }
        logging.warn(
            "Background AOF rewrite terminated by signal {}",
            .{bysignal},
        );
    }
}

/// Write the buffer (possibly composed of multiple blocks) into the specified
/// fd. If a short write or any other error happens error is returned.
fn rewriteBufferWrite(fd: posix.fd_t) !void {
    var li = server.aof_rewrite_buf_blocks.?.iterator(.forward);
    while (li.next()) |ln| {
        const block = ln.value;
        if (block.used != 0) {
            const nwritten = try posix.write(fd, block.buf[0..block.used]);
            if (nwritten != block.used) {
                return error.ShortWrite;
            }
        }
    }
}

fn removeTempFile(childpid: posix.pid_t) void {
    var tmpbuf: [256]u8 = undefined;
    const tmpfile = std.fmt.bufPrint(
        &tmpbuf,
        "temp-rewriteaof-bg-{}.aof",
        .{childpid},
    ) catch unreachable;
    std.fs.cwd().deleteFile(tmpfile) catch {};
}

/// Replay the append log file. On fatal error an error message is logged and
/// the program exists.
pub fn loadAppendOnlyFile(filename: []const u8) void {
    var fakeClient: ?*Client = null;
    var fp: std.fs.File = undefined;
    // Offset of latest well-formed command loaded.
    var valid_up_to: usize = 0;
    // Offset before MULTI command loaded.
    var valid_before_multi: usize = 0;
    const old_aof_state = server.aof_state;

    var reterr: union(enum) {
        none: void,
        readerr: anyerror,
        fmterr: void,
    } = .{ .none = {} };
    defer switch (reterr) {
        .readerr => |err| {
            if (fakeClient) |cli| freeFakeClient(cli);
            logging.warn(
                "Unrecoverable error reading the append only file: {any}",
                .{
                    err,
                },
            );
            std.process.exit(1);
        },
        .fmterr => {
            if (fakeClient) |cli| freeFakeClient(cli);
            logging.warn(
                "Bad file format reading the append only file: make a " ++
                    "backup of your AOF file, then use " ++
                    "./redis-check-aof --fix <filename>",
                .{},
            );
            std.process.exit(1);
        },
        .none => {},
    };

    fp = std.fs.cwd().openFile(filename, .{}) catch |err| {
        logging.warn(
            "Fatal error: can't open the append log file for reading: {}",
            .{err},
        );
        std.process.exit(1);
    };

    // Handle a zero-length AOF file as a special case. An empty AOF file
    // is a valid AOF because an empty server with AOF enabled will create
    // a zero length file at startup, that will remain like that if no write
    // operation is received.
    if (fp.stat()) |stat| {
        if (stat.size == 0) {
            server.aof_current_size = 0;
            server.aof_fsync_offset = server.aof_current_size;
            fp.close();
            return;
        }
    } else |_| {}

    // Temporarily disable AOF, to prevent EXEC from feeding a MULTI
    // to the same file we're about to read.
    server.aof_state = Server.AOF_OFF;

    fakeClient = createFakeClient();
    rdb.startLoading(fp);

    // Check if this AOF file has an RDB preamble. In that case we need to
    // load the RDB file and later continue loading the AOF tail.
    var sig: [5]u8 = undefined;
    var offset: u64 = 0;
    if ((fp.read(&sig) catch 0) != sig.len or !std.mem.eql(u8, "REDIS", &sig)) {
        // No RDB preamble, seek back at 0 offset.
        offset = 0;
    } else {
        // RDB preamble. Pass loading the RDB functions.
        logging.notice("Reading RDB preamble from AOF file...", .{});
        fp.seekTo(0) catch |err| {
            reterr = .{ .readerr = err };
            return;
        };
        var fio: rio.FileReadWriter = undefined;
        fio.init(fp);
        var rdbfile = fio.readWriter();
        rdb.loadRio(&rdbfile, true) catch |err| {
            logging.warn(
                "Error reading the RDB preamble of the AOF file, AOF loading aborted",
                .{},
            );
            reterr = .{ .readerr = err };
            return;
        };
        logging.notice("Reading the remaining AOF tail...", .{});
        offset = rdbfile.tell() catch |err| {
            reterr = .{ .readerr = err };
            return;
        };
    }

    // Read the actual AOF file, in REPL format, command by command.
    var loops: usize = 0;
    var buf: [128]u8 = undefined;
    var reader = fp.reader(&buf);
    reader.seekTo(offset) catch |err| {
        reterr = .{ .readerr = err };
        return;
    };
    while (true) : (loops += 1) {
        // Serve the clients from time to time
        if (@rem(loops, 1000) == 0) {
            rdb.loadingProgress(fp.getPos() catch |err| {
                reterr = .{ .readerr = err };
                return;
            });
            _ = networking.processEventsWhileBlocked() catch {};
        }
        var read = reader.interface.takeDelimiter('\n') catch |err| {
            reterr = .{ .readerr = err };
            return;
        } orelse {
            break; // EOF
        };
        if (read[0] != '*') {
            reterr = .{ .fmterr = {} };
            return;
        }
        if (read.len == 1) {
            reterr = .{ .fmterr = {} };
            return;
        }
        read = @constCast(std.mem.trimEnd(u8, read[1..], "\r"));
        const argc = std.fmt.parseInt(usize, read, 10) catch {
            reterr = .{ .fmterr = {} };
            return;
        };
        if (argc < 1) {
            reterr = .{ .fmterr = {} };
            return;
        }
        // Load the next command in the AOF as our fake client
        // argv.
        const argv = allocator.alloc(*Object, argc);
        fakeClient.?.argc = 0;
        fakeClient.?.argv = argv;

        for (0..argc) |i| {
            defer if (reterr != .none) freeFakeClientArgv(fakeClient.?);
            // Parse the argument len.
            read = reader.interface.takeDelimiter('\n') catch |err| {
                reterr = .{ .readerr = err };
                return;
            } orelse {
                reterr = .{ .fmterr = {} };
                return;
            };
            if (read[0] != '$') {
                reterr = .{ .fmterr = {} };
                return;
            }
            const len = std.fmt.parseInt(
                usize,
                std.mem.trimEnd(u8, read[1..], "\r"),
                10,
            ) catch {
                reterr = .{ .fmterr = {} };
                return;
            };

            // Read it into a string object.
            const arg = sds.newLen(allocator.impl, sds.NOINIT.ptr, len);
            reader.interface.readSliceAll(sds.asBytes(arg)) catch |err| {
                sds.free(allocator.impl, arg);
                reterr = .{ .readerr = err };
                return;
            };
            argv[i] = Object.create(.string, arg);
            fakeClient.?.argc = i + 1;

            // Discard CRLF.
            reader.interface.discardAll(2) catch |err| {
                reterr = .{ .readerr = err };
                return;
            };
        }

        // Command lookup
        const cmd = server.lookupCommand(sds.cast(argv[0].v.ptr)) orelse {
            logging.warn(
                "Unknown command '{s}' reading the append only file",
                .{sds.castBytes(argv[0].v.ptr)},
            );
            std.process.exit(1);
        };
        if (cmd == server.multiCommand) {
            valid_before_multi = valid_up_to;
        }

        // Run the command in the context of a fake client
        fakeClient.?.cmd = cmd;
        if (fakeClient.?.flags & Server.CLIENT_MULTI != 0 and
            fakeClient.?.cmd.?.proc != multi.execCommand)
        {
            multi.queueMultiCommand(fakeClient.?);
        } else {
            cmd.proc(fakeClient.?);
        }

        // The fake client should not have a reply
        assert(fakeClient.?.bufpos == 0 and fakeClient.?.reply.len == 0);

        // The fake client should never get blocked
        assert(fakeClient.?.flags & Server.CLIENT_BLOCKED == 0);

        // Clean up. Command code may have changed argv/argc so we use the
        // argv/argc of the client instead of the local variables.
        freeFakeClientArgv(fakeClient.?);
        fakeClient.?.cmd = null;
        if (server.aof_load_truncated) {
            valid_up_to = fp.getPos() catch |err| {
                reterr = .{ .readerr = err };
                return;
            };
        }
    }

    // This point can only be reached when EOF is reached without errors.
    // If the client is in the middle of a MULTI/EXEC, handle it as it was
    // a short read, even if technically the protocol is correct: we want
    // to remove the unprocessed tail and continue.
    if (fakeClient.?.flags * Server.CLIENT_MULTI != 0) {
        logging.warn(
            "Revert incomplete MULTI/EXEC transaction in AOF file",
            .{},
        );
        valid_up_to = valid_before_multi;
        handleUnexpectedEOF(valid_up_to);
    }

    // DB loaded, cleanup
    fp.close();
    freeFakeClient(fakeClient.?);
    server.aof_state = old_aof_state;
    rdb.stopLoading();
    updateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    server.aof_fsync_offset = server.aof_current_size;
}

fn handleUnexpectedEOF(valid_up_to: usize) void {
    if (server.aof_load_truncated) {
        logging.warn(
            "!!! Warning: short read while loading the AOF file !!!",
            .{},
        );
        logging.warn(
            "!!! Truncating the AOF at offset {} !!!",
            .{valid_up_to},
        );
        if (posix.ftruncate(server.aof_fd, valid_up_to)) {
            // Make sure the AOF file descriptor points to the end of the
            // file after the truncate call.
            if (posix.lseek_END(server.aof_fd, 0)) {
                logging.warn(
                    "AOF loaded anyway because aof-load-truncated is enabled",
                    .{},
                );
                return;
            } else |err| {
                logging.warn(
                    "Can't seek the end of the AOF file: {}",
                    .{err},
                );
            }
        } else |err| {
            logging.warn("Error truncating the AOF file: {}", .{err});
        }
    }
    logging.warn(
        "Unexpected end of file reading the append only file. " ++
            "You can: 1) Make a backup of your AOF file, then use " ++
            "./redis-check-aof --fix <filename>. 2) Alternatively " ++
            " you can set the 'aof-load-truncated' configuration " ++
            "option to yes and restart the server.",
        .{},
    );
    std.process.exit(1);
}

// In Redis commands are always executed in the context of a client, so in
// order to load the append only file we need to create a fake client.
fn createFakeClient() *Client {
    const cli = allocator.create(Client);
    cli.fd = -1;
    cli.querybuf = sds.empty(allocator.impl);
    cli.argc = 0;
    cli.argv = null;
    cli.bufpos = 0;
    cli.flags = 0;
    cli.btype = Server.BLOCKED_NONE;
    cli.reply = .create(&.{
        .freeVal = Client.ReplyBlock.destroy,
        .dupVal = Client.ReplyBlock.dupe,
    });
    cli.reply_bytes = 0;
    cli.watched_keys = .create(&.{
        .freeVal = multi.WatchedKey.destroy,
    });
    cli.mstate.init();
    return cli;
}

fn freeFakeClient(cli: *Client) void {
    sds.free(allocator.impl, cli.querybuf);
    cli.reply.release();
    cli.watched_keys.release();
    cli.mstate.deinit();
    allocator.destroy(cli);
}

fn freeFakeClientArgv(cli: *Client) void {
    for (0..cli.argc) |i| {
        cli.argv.?[i].decrRefCount();
    }
    allocator.free(cli.argv.?);
}

/// Update the server.aof_current_size field explicitly using stat(2)
/// to check the size of the file. This is useful after a rewrite or after
/// a restart, normally the size is updated just adding the write length
/// to the current length, that is much faster.
fn updateCurrentSize() void {
    var file: std.fs.File = .{ .handle = server.aof_fd };
    const st = file.stat() catch |err| {
        logging.warn("Unable to obtain the AOF file length. stat: {}", .{err});
        return;
    };
    server.aof_current_size = st.size;
}

/// Write the append only file buffer on disk.
///
/// Since we are required to write the AOF before replying to the client,
/// and the only way the client socket can get a write is entering when the
/// the event loop, we accumulate all the AOF writes in a memory
/// buffer and write it on disk using this function just before entering
/// the event loop again.
///
/// About the 'force' argument:
///
/// When the fsync policy is set to 'everysec' we may delay the flush if there
/// is still an fsync() going on in the background thread, since for instance
/// on Linux write(2) will be blocked by the background fsync anyway.
/// When this happens we remember that there is some aof buffer to be
/// flushed ASAP, and will try to do that in the serverCron() function.
///
/// However if force is set to TRUE we'll write regardless of the background
/// fsync.
pub fn flushAppendOnlyFile(force: bool) void {
    // Seconds between errors logging.
    const AOF_WRITE_LOG_ERROR_RATE = 30;

    var sync_in_progress = false;
    write: {
        if (sds.getLen(server.aof_buf) == 0) {
            // Check if we need to do fsync even the aof buffer is empty,
            // because previously in AOF_FSYNC_EVERYSEC mode, fsync is
            // called only when aof buffer is not empty, so if users
            // stop write commands before fsync called in one second,
            // the data in page cache cannot be flushed in time.
            if (server.aof_fsync == Server.AOF_FSYNC_EVERYSEC and
                server.aof_fsync_offset != server.aof_current_size and
                server.unixtime.get() > server.aof_last_fsync and
                !sync_in_progress: {
                    sync_in_progress = fsyncInProgress();
                    break :sync_in_progress sync_in_progress;
                })
            {
                break :write; // go to fsync
            } else {
                return;
            }
        }

        if (server.aof_fsync == Server.AOF_FSYNC_EVERYSEC) {
            sync_in_progress = fsyncInProgress();
        }

        if (server.aof_fsync == Server.AOF_FSYNC_EVERYSEC and !force) {
            // With this append fsync policy we do background fsyncing.
            // If the fsync is still in progress we can try to delay
            // the write for a couple of seconds.
            if (sync_in_progress) {
                if (server.aof_flush_postponed_start == 0) {
                    // No previous write postponing, remember that we are
                    // postponing the flush and return.
                    server.aof_flush_postponed_start = server.unixtime.get();
                    return;
                } else if (server.unixtime.get() - server.aof_flush_postponed_start < 2) {
                    // We were already waiting for fsync to finish, but for less
                    // than two seconds this is still ok. Postpone again.
                    return;
                }
                // Otherwise fall trough, and go write since we can't wait
                // over two seconds.
                server.aof_delayed_fsync +%= 1;
                logging.notice(
                    "Asynchronous AOF fsync is taking too long " ++
                        "(disk is busy?). Writing the AOF buffer without " ++
                        "waiting for fsync to complete, this may slow down Redis.",
                    .{},
                );
            }
        }

        // We want to perform a single write. This should be guaranteed atomic
        // at least if the filesystem we are writing is a real physical one.
        // While this will save us against the server being killed I don't think
        // there is much to do about the whole server stopping for power problems
        // or alike

        var nwritten = write(server.aof_fd, sds.asBytes(server.aof_buf));

        // We performed the write so reset the postponed flush sentinel to zero.
        server.aof_flush_postponed_start = 0;

        if (nwritten.size != sds.getLen(server.aof_buf)) {
            const Static = struct {
                var last_write_error_log: i64 = 0;
            };
            var can_log = false;
            // Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds.
            if ((server.unixtime.get() - Static.last_write_error_log) >
                AOF_WRITE_LOG_ERROR_RATE)
            {
                can_log = true;
                Static.last_write_error_log = server.unixtime.get();
            }
            // Log the AOF write error and record the error code.
            if (nwritten.err) |err| {
                if (can_log) {
                    logging.warn("Error writing to the AOF file: {}", .{err});
                    server.aof_last_write_err = err;
                }
            } else {
                if (can_log) {
                    logging.warn(
                        "Short write while writing to " ++
                            "the AOF file: (nwritten={}, " ++
                            "expected={})",
                        .{
                            nwritten.size,
                            sds.getLen(server.aof_buf),
                        },
                    );
                }
                if (posix.ftruncate(server.aof_fd, server.aof_current_size)) {
                    // If the ftruncate() succeeded we can set nwritten to
                    // 0 since there is no longer partial data into the AOF.
                    nwritten.size = 0;
                } else |err| if (can_log) {
                    logging.warn(
                        "Could not remove short write " ++
                            "from the append-only file.  Redis may refuse " ++
                            "to load the AOF the next time it starts.  " ++
                            "ftruncate: {}",
                        .{err},
                    );
                }
                server.aof_last_write_err = error.NoSpace;
            }
            // Handle the AOF write error.
            if (server.aof_fsync == Server.AOF_FSYNC_ALWAYS) {
                // We can't recover when the fsync policy is ALWAYS since the
                // reply for the client is already in the output buffers, and we
                // have the contract with the user that on acknowledged write data
                // is synced on disk.
                logging.warn(
                    "Can't recover from AOF write error when the AOF fsync " ++
                        "policy is 'always'. Exiting...",
                    .{},
                );
                posix.exit(1);
            } else {
                // Recover from failed write leaving data into the buffer. However
                // set an error to stop accepting writes as long as the error
                // condition is not cleared.
                server.aof_last_write_status = false;

                // Trim the sds buffer if there was a partial write, and there
                // was no way to undo it with ftruncate(2).
                if (nwritten.size > 0) {
                    server.aof_current_size += nwritten.size;
                    sds.range(server.aof_buf, @intCast(nwritten.size), -1);
                }
                return; // We'll try again on the next call...
            }
        } else {
            // Successful write(2). If AOF was in error state, restore the
            // OK state and log the event.
            if (server.aof_last_write_status == false) {
                logging.warn(
                    "AOF write error looks solved, Redis can write again.",
                    .{},
                );
                server.aof_last_write_status = true;
            }
        }
        server.aof_current_size +%= nwritten.size;

        // Re-use AOF buffer when it is small enough. The maximum comes from the
        // arena size of 4k minus some overhead (but is otherwise arbitrary).
        if (sds.getLen(server.aof_buf) + sds.getAvail(server.aof_buf) < 4000) {
            sds.clear(server.aof_buf);
        } else {
            sds.free(allocator.impl, server.aof_buf);
            server.aof_buf = sds.empty(allocator.impl);
        }
    }

    // fsync
    {
        // Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
        // children doing I/O in the background.
        if (server.aof_no_fsync_on_rewrite and
            (server.aof_child_pid != -1 or server.rdb_child_pid != -1))
        {
            return;
        }

        // Perform the fsync if needed.
        if (server.aof_fsync == Server.AOF_FSYNC_ALWAYS) {
            // Let's try to get this data on the disk
            config.fsync(server.aof_fd) catch {};
            server.aof_fsync_offset = server.aof_current_size;
            server.aof_last_fsync = server.unixtime.get();
        } else if (server.aof_fsync == Server.AOF_FSYNC_EVERYSEC and
            server.unixtime.get() > server.aof_last_fsync)
        {
            if (!sync_in_progress) {
                backgroundFsync(server.aof_fd);
                server.aof_fsync_offset = server.aof_current_size;
            }
            server.aof_last_fsync = server.unixtime.get();
        }
    }
}

/// This is a wrapper to the write syscall in order to retry on short writes
/// or if the syscall gets interrupted. It could look strange that we retry
/// on short writes given that we are writing to a block device: normally if
/// the first call is short, there is a end-of-space condition, so the next
/// is likely to fail. However apparently in modern systems this is no longer
/// true, and in general it looks just more resilient to retry the write. If
/// there is an actual error condition we'll get it at the next try.
fn write(fd: posix.fd_t, buf: []const u8) struct {
    size: usize,
    err: ?anyerror,
} {
    var nwritten: usize = 0;
    var totwritten: usize = 0;

    var ptr = buf.ptr;
    var len = buf.len;
    while (len > 0) {
        nwritten = posix.write(fd, ptr[0..len]) catch |err| {
            return .{ .size = totwritten, .err = err };
        };
        len -= nwritten;
        totwritten += nwritten;
        ptr += nwritten;
    }
    return .{ .size = totwritten, .err = null };
}

/// Return true if an AOf fsync is currently already in progress in a
/// BIO thread.
fn fsyncInProgress() bool {
    return bio.pendingJobsOfType(bio.Job.Type.aofFsync) != 0;
}

/// Starts a background task that performs fsync() against the specified
/// file descriptor (the one of the AOF file) in another thread.
fn backgroundFsync(fd: posix.fd_t) void {
    bio.createBackgroundJob(
        bio.Job.Type.aofFsync,
        @ptrFromInt(@as(usize, @intCast(fd))),
        null,
        null,
    );
}

pub fn feedAppendOnlyFile(
    cmd: *const Command,
    dbid: i32,
    argv: []const *Object,
    argc: usize,
) void {
    var buf = sds.empty(allocator.impl);
    defer sds.free(allocator.impl, buf);

    // The DB this command was targeting is not the same as the last command
    // we appended. To issue a SELECT command is needed.
    if (dbid != server.aof_selected_db) {
        var seldb: [64]u8 = undefined;
        const id = std.fmt.bufPrint(&seldb, "{}", .{dbid}) catch unreachable;
        buf = sds.catPrintf(
            allocator.impl,
            buf,
            "*2\r\n$6\r\nSELECT\r\n${}\r\n{s}\r\n",
            .{ id.len, id },
        );
        server.aof_selected_db = dbid;
    }

    if (cmd.proc == expire.expireCommand or
        cmd.proc == expire.pexpireCommand or
        cmd.proc == expire.expireatCommand)
    {
        // Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
    } else if (cmd.proc == tstring.setexCommand or
        cmd.proc == tstring.psetexCommand)
    {
        // Translate SETEX/PSETEX to SET and PEXPIREAT
        var tmpargv: [3]*Object = undefined;
        tmpargv[0] = Object.createString("SET");
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        defer tmpargv[0].decrRefCount();
        buf = catAppendOnlyGenericCommand(buf, &tmpargv);
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
    } else if (cmd.proc == tstring.setCommand and argc > 3) {
        var exarg: ?*Object = null;
        var pxarg: ?*Object = null;
        // Translate SET [EX seconds][PX milliseconds] to SET and PEXPIREAT
        buf = catAppendOnlyGenericCommand(buf, argv[0..3]);
        for (argv[3..argc]) |arg| {
            if (eqlCase(sds.castBytes(arg.v.ptr), "ex")) {
                exarg = arg;
            }
            if (eqlCase(sds.castBytes(arg.v.ptr), "px")) {
                pxarg = arg;
            }
        }
        assert(!(exarg != null and pxarg != null));
        if (exarg) |ex| {
            buf = catAppendOnlyExpireAtCommand(
                buf,
                server.expireCommand,
                argv[1],
                ex,
            );
        }
        if (pxarg) |px| {
            buf = catAppendOnlyExpireAtCommand(
                buf,
                server.pexpireCommand,
                argv[1],
                px,
            );
        }
    } else {
        // All the other commands don't need translation or need the
        // same translation already operated in the command vector
        // for the replication itself.
        buf = catAppendOnlyGenericCommand(buf, argv[0..argc]);
    }

    // Append to the AOF buffer. This will be flushed on disk just before
    // of re-entering the event loop, so before the client will get a
    // positive reply about the operation performed.
    if (server.aof_state == Server.AOF_ON) {
        server.aof_buf = sds.cat(
            allocator.impl,
            server.aof_buf,
            sds.asBytes(buf),
        );
    }

    // If a background append only file rewriting is in progress we want to
    // accumulate the differences between the child DB and the current one
    // in a buffer, so that when the child process will do its work we
    // can append the differences to the new append only file.
    if (server.aof_child_pid != -1) {
        rewriteBufferAppend(sds.asBytes(buf));
    }
}

/// Create the sds representation of an PEXPIREAT command, using
/// 'seconds' as time to live and 'cmd' to understand what command
/// we are translating into a PEXPIREAT.
///
/// This command is used in order to translate EXPIRE and PEXPIRE commands
/// into PEXPIREAT command so that we retain precision in the append only
/// file, and the time is always absolute and not relative.
fn catAppendOnlyExpireAtCommand(
    buf: sds.String,
    cmd: *const Command,
    key: *Object,
    seconds: *Object,
) sds.String {
    // Make sure we can use util.string2ll()
    const sec = seconds.getDecoded();
    defer sec.decrRefCount();
    var when = util.string2ll(sds.castBytes(sec.v.ptr)).?;

    // Convert argument into milliseconds for EXPIRE, EXPIREAT, SETEX
    if (cmd.proc == expire.expireCommand or
        cmd.proc == expire.expireatCommand or
        cmd.proc == tstring.setexCommand)
    {
        when *= std.time.ms_per_s;
    }

    // Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX
    if (cmd.proc == expire.expireCommand or
        cmd.proc == expire.pexpireCommand or
        cmd.proc == tstring.setexCommand or
        cmd.proc == tstring.psetexCommand)
    {
        when += std.time.milliTimestamp();
    }

    var argv: [3]*Object = undefined;
    argv[0] = Object.createString("PEXPIREAT");
    argv[1] = key;
    argv[2] = Object.createStringFromLonglong(when);
    const cat = catAppendOnlyGenericCommand(buf, &argv);
    argv[0].decrRefCount();
    argv[2].decrRefCount();
    return cat;
}

fn catAppendOnlyGenericCommand(
    dst: sds.String,
    argv: []const *Object,
) sds.String {
    var cat = dst;
    var buf: [32]u8 = undefined;
    buf[0] = '*';
    var len = util.ll2string(buf[1..], @intCast(argv.len)).len + 1;
    buf[len] = '\r';
    len += 1;
    buf[len] = '\n';
    len += 1;
    cat = sds.cat(allocator.impl, cat, buf[0..len]);

    for (argv) |arg| {
        const o = arg.getDecoded();
        defer o.decrRefCount();
        buf[0] = '$';
        len = util.ll2string(
            buf[1..],
            @intCast(sds.getLen(sds.cast(o.v.ptr))),
        ).len + 1;
        buf[len] = '\r';
        len += 1;
        buf[len] = '\n';
        len += 1;
        cat = sds.cat(allocator.impl, cat, buf[0..len]);
        cat = sds.cat(allocator.impl, cat, sds.castBytes(o.v.ptr));
        cat = sds.cat(allocator.impl, cat, "\r\n");
    }
    return cat;
}

pub const RwBlock = struct {
    // 10 MB per block
    const BUF_BLOCK_SIZE = (1024 * 1024 * 10);

    used: u64,
    free: u64,
    buf: [BUF_BLOCK_SIZE]u8,

    fn create() *RwBlock {
        const b = allocator.create(RwBlock);
        b.free = BUF_BLOCK_SIZE;
        b.used = 0;
        return b;
    }

    fn destroy(blok: *RwBlock) void {
        allocator.destroy(blok);
    }
};

const Server = @import("Server.zig");
const server = &Server.instance;
const Command = Server.Command;
const Object = @import("Object.zig");
const sds = @import("sds.zig");
const allocator = @import("allocator.zig");
const std = @import("std");
const expire = @import("expire.zig");
const util = @import("util.zig");
const tstring = @import("t_string.zig");
const eqlCase = std.ascii.eqlIgnoreCase;
const assert = std.debug.assert;
const bio = @import("bio.zig");
const posix = std.posix;
const config = @import("config.zig");
const logging = @import("logging.zig");
const networking = @import("networking.zig");
const Client = networking.Client;
const rdb = @import("rdb.zig");
const rio = @import("rio.zig");
const multi = @import("multi.zig");
const childinfo = @import("childinfo.zig");
const libc = @cImport({
    @cInclude("unistd.h");
});
const anet = @import("anet.zig");
const ae = @import("ae.zig");
const syncio = @import("syncio.zig");
const QuickList = @import("QuickList.zig");
const IntSet = @import("IntSet.zig");
const ZipList = @import("ZipList.zig");
const Stream = @import("t_stream.zig").Stream;
const raxlib = @import("rax.zig").rax;
const Hash = @import("t_hash.zig").Hash;
const memlib = @import("mem.zig");
const memcpy = memlib.memcpy;
const memmove = memlib.memmove;
