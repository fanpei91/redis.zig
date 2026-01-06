/// The current RDB version. When the format changes in a way that is no longer
/// backward compatible this number gets incremented.
pub const RDB_VERSION = 9;

// Defines related to the dump file format. To store 32 bits lengths for short
// keys requires a lot of space, so we check the most significant 2 bits of
// the first byte to interpreter the length:
//
// 00|XXXXXX => if the two MSB are 00 the len is the 6 bits of this byte
// 01|XXXXXX XXXXXXXX =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
// 10|000000 [32 bit integer] => A full 32 bit len in net byte order will follow
// 10|000001 [64 bit integer] => A full 64 bit len in net byte order will follow
// 11|OBKIND this means: specially encoded object will follow. The six bits
//           number specify the kind of object that follows.
//           See the RDB_ENC_* defines.
//
// Lengths up to 63 are stored using a single byte, most DB keys, and may
// values, will fit inside.
pub const RDB_6BITLEN = 0;
pub const RDB_14BITLEN = 1;
pub const RDB_32BITLEN = 0x80;
pub const RDB_64BITLEN = 0x81;
pub const RDB_ENCVAL = 3;
pub const RDB_LENERR = std.math.maxInt(u64);

// When a length of a string object stored on disk has the first two bits
// set, the remaining six bits specify a special encoding for the object
// accordingly to the following defines:
pub const RDB_ENC_INT8 = 0; // 8 bit signed integer
pub const RDB_ENC_INT16 = 1; // 16 bit signed integer
pub const RDB_ENC_INT32 = 2; // 32 bit signed integer
pub const RDB_ENC_LZF = 3; // string compressed with FASTLZ

// Map object types to RDB object types. Macros starting with OBJ_ are for
// memory storage and may change. Instead RDB types must be fixed because
// we store them on disk.
pub const RDB_TYPE_STRING = 0;
pub const RDB_TYPE_LIST = 1;
pub const RDB_TYPE_SET = 2;
pub const RDB_TYPE_ZSET = 3;
pub const RDB_TYPE_HASH = 4;
/// ZSET version 2 with doubles stored in binary.
pub const RDB_TYPE_ZSET_2 = 5;

// Object types for encoded objects.
pub const RDB_TYPE_HASH_ZIPMAP = 9;
pub const RDB_TYPE_LIST_ZIPLIST = 10;
pub const RDB_TYPE_SET_INTSET = 11;
pub const RDB_TYPE_ZSET_ZIPLIST = 12;
pub const RDB_TYPE_HASH_ZIPLIST = 13;
pub const RDB_TYPE_LIST_QUICKLIST = 14;
pub const RDB_TYPE_STREAM_LISTPACKS = 15;

// Special RDB opcodes (saved/loaded with saveType/loadType).
pub const RDB_OPCODE_IDLE = 248; // LRU idle time.
pub const RDB_OPCODE_FREQ = 249; // LFU frequency.
pub const RDB_OPCODE_AUX = 250; // RDB aux field.
pub const RDB_OPCODE_RESIZEDB = 251; // Hash table resize hint.
pub const RDB_OPCODE_EXPIRETIME_MS = 252; // Expire time in milliseconds.
pub const RDB_OPCODE_EXPIRETIME = 253; // Old expire time in seconds.
pub const RDB_OPCODE_SELECTDB = 254; // DB number of the following keys.
pub const RDB_OPCODE_EOF = 255; // End of the RDB file.

// load...() functions flags.
pub const RDB_LOAD_NONE = 0;
pub const RDB_LOAD_ENC = (1 << 0);
pub const RDB_LOAD_PLAIN = (1 << 1);
pub const RDB_LOAD_SDS = (1 << 2);

pub const RDB_SAVE_NONE = 0;
pub const RDB_SAVE_AOF_PREAMBLE = (1 << 0);

/// SAVE
pub fn saveCommand(cli: *Client) void {
    if (server.rdb_child_pid != -1) {
        cli.addReplyErr("Background save already in progress");
        return;
    }
    var rsi: SaveInfo = undefined;
    const rsiptr = rsi.populate();
    if (save(server.rdb_filename, rsiptr)) {
        cli.addReply(Server.shared.ok);
    } else {
        cli.addReply(Server.shared.err);
    }
}

/// BGSAVE [SCHEDULE]
pub fn bgsaveCommand(cli: *Client) void {
    const argv = cli.argv.?;

    // The SCHEDULE option changes the behavior of BGSAVE when an AOF rewrite
    // is in progress. Instead of returning an error a BGSAVE gets scheduled.
    var schedule = false;
    if (cli.argc > 1) {
        if (cli.argc == 2 and eqlCase(sds.castBytes(argv[1].v.ptr), "SCHEDULE")) {
            schedule = true;
        } else {
            cli.addReply(Server.shared.syntaxerr);
            return;
        }
    }

    var rsi: SaveInfo = undefined;
    const rsiptr = rsi.populate();

    if (server.rdb_child_pid != -1) {
        cli.addReplyErr("Background save already in progress");
    } else if (server.aof_child_pid != -1) {
        if (schedule) {
            server.rdb_bgsave_scheduled = true;
            cli.addReplyStatus("Background saving scheduled");
        } else {
            cli.addReplyErr(
                "An AOF log rewriting in progress: can't BGSAVE right now. " ++
                    "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever " ++
                    "possible.",
            );
        }
    } else if (saveBackground(server.rdb_filename, rsiptr)) {
        cli.addReplyStatus("Background saving started");
    } else {
        cli.addReply(Server.shared.err);
    }
}

pub fn saveBackground(filename: []const u8, rsi: ?*SaveInfo) bool {
    if (server.aof_child_pid != -1 or server.rdb_child_pid != -1) {
        return false;
    }

    server.dirty_before_bgsave = server.dirty;
    server.lastbgsave_try = std.time.timestamp();
    childinfo.openPipe();

    const childpid = posix.fork() catch |err| {
        childinfo.closePipe();
        server.lastbgsave_status = false;
        logging.warn("Can't save in background: fork: {}", .{err});
        return false;
    };
    if (childpid == 0) {
        // Child
        Server.closeChildUnusedResourceAfterFork();
        Server.redisSetProcTitle("redis-rdb-bgsave");
        const retval = save(filename, rsi);
        if (retval) {
            // TODO: Copy On Write size
            childinfo.send(Server.CHILD_INFO_TYPE_RDB);
        }
        Server.exitFromChild(if (retval) 0 else 1);
    } else {
        // Parent
        // TODO: server stat
        logging.notice("Background saving started by pid {}", .{childpid});
        server.rdb_save_time_start = std.time.timestamp();
        server.rdb_child_pid = childpid;
        server.rdb_child_type = Server.RDB_CHILD_TYPE_DISK;
        Server.updateDictResizePolicy();
        return true;
    }
    unreachable;
}

/// When a background RDB saving/transfer terminates, call the right handler.
pub fn backgroundSaveDoneHandler(exitcode: c_int, bysignal: c_int) void {
    switch (server.rdb_child_type) {
        Server.RDB_CHILD_TYPE_DISK => {
            backgroundSaveDoneHandlerDisk(exitcode, bysignal);
        },
        Server.RDB_CHILD_TYPE_SOCKET => {
            // TODO:
        },
        else => {
            @panic("Unknown RDB child type");
        },
    }
}

/// A background saving child (BGSAVE) terminated its work. Handle this.
/// This function covers the case of actual BGSAVEs.
fn backgroundSaveDoneHandlerDisk(exitcode: c_int, bysignal: c_int) void {
    if (bysignal == 0 and exitcode == 0) {
        logging.notice("Background saving terminated with success", .{});
        server.dirty = server.dirty -% server.dirty_before_bgsave;
        server.lastsave = std.time.timestamp();
        server.lastbgsave_status = true;
    } else if (bysignal == 0 and exitcode != 0) {
        logging.warn("Background saving error", .{});
        server.lastbgsave_status = false;
    } else {
        logging.warn("Background saving terminated by signal {}", .{bysignal});
        rdbRemoveTempFile(server.rdb_child_pid);
        // SIGUSR1 is whitelisted, so we have a way to kill a child without
        // tirggering an error condition.
        if (bysignal != posix.SIG.USR1) {
            server.lastbgsave_status = false;
        }
    }
    server.rdb_child_pid = -1;
    server.rdb_child_type = Server.RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;
    // TODO: Replication Slaves
}

/// Like loadRio() but takes a filename instead of a rio stream. The
/// filename is open for reading and a rio stream object created in order
/// to do the actual loading. Moreover the ETA displayed in the INFO
/// output is initialized and finalized.
///
/// If you pass an 'rsi' structure initialied with RDB_SAVE_OPTION_INIT, the
/// loading code will fiil the information fields in the structure.
pub fn load(filename: []const u8, rsi: ?*SaveInfo) !void {
    var cwd = std.fs.cwd();

    const fp = try cwd.openFile(filename, .{ .mode = .read_only });
    startLoading(fp);
    defer stopLoading();

    var fio: rio.FileReadWriter = undefined;
    fio.init(fp);
    defer fio.file.close();

    var rdb = fio.readWriter();
    try loadRio(&rdb, rsi, false);
}

///  Mark that we are loading in the global state and setup the fields
/// needed to provide loading stats
pub fn startLoading(fp: std.fs.File) void {
    server.loading = true;
    server.loading_start_time = std.time.timestamp();
    server.loading_loaded_bytes = 0;
    if (fp.stat()) |st| {
        server.loading_total_bytes = st.size;
    } else |_| {
        server.loading_total_bytes = 0;
    }
}

/// Load an RDB file from the rio stream 'rdb'.
pub fn loadRio(rdb: *rio.ReadWriter, rsi: ?*SaveInfo, loading_aof: bool) !void {
    _ = rsi;

    var db = &server.db[0];

    rdb.updateChecksum = loadProgressCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    biz: {
        var buf: [1024]u8 = undefined;
        rdb.read(buf[0..9]) catch break :biz;
        if (!std.mem.eql(u8, "REDIS", buf[0..5])) {
            logging.warn("Wrong signature trying to load DB from file", .{});
            return error.InvalidSignature;
        }
        const rdbver = std.fmt.parseInt(u32, buf[5..9], 10) catch 0;
        if (rdbver < 1 or rdbver > RDB_VERSION) {
            logging.warn("Can't handle RDB format version {}", .{rdbver});
            return error.InvalidFormatVersion;
        }

        // Key-specific attributes, set by opcodes before the key type.
        var lru_idle: i64 = -1;
        var lfu_freq: i64 = -1;
        var expiretime: i64 = -1;
        const now = std.time.milliTimestamp();
        const lru_clock = evict.LRUClock();
        while (true) {
            // Read type.
            const @"type" = loadType(rdb) catch break :biz;
            // Handle special types.
            switch (@"type") {
                RDB_OPCODE_EXPIRETIME => {
                    // EXPIRETIME: load an expire associated with the next key
                    // to load. Note that after loading an expire we need to
                    // load the actual type, and continue.
                    expiretime = loadTime(rdb);
                    expiretime *= std.time.ms_per_s;
                    continue; // Read next opcode.
                },
                RDB_OPCODE_EXPIRETIME_MS => {
                    // EXPIRETIME_MS: milliseconds precision expire times introduced
                    // with RDB v3. Like EXPIRETIME but no with more precision.
                    expiretime = loadMillisecondTime(rdb, rdbver);
                    continue; // Read next opcode.
                },
                RDB_OPCODE_FREQ => {
                    // FREQ: LFU frequency.
                    var byte: u8 = undefined;
                    rdb.read(std.mem.asBytes(&byte)) catch break :biz;
                    lfu_freq = @intCast(byte);
                    continue; // Read next opcode.
                },
                RDB_OPCODE_IDLE => {
                    // IDLE: LRU idle time.
                    const qword = loadLen(rdb, null) catch break :biz;
                    lru_idle = @intCast(qword);
                    continue; // Read next opcode.
                },
                RDB_OPCODE_EOF => {
                    // EOF: End of file, exit the main loop.
                    break;
                },
                RDB_OPCODE_SELECTDB => {
                    // SELECTDB: Select the specified database.
                    const dbid = try loadLen(rdb, null);
                    if (dbid >= server.db.len) {
                        exitReportCorruptRDB(
                            "FATAL: Data file was created with a Redis " ++
                                "server configured to handle more than {} " ++
                                "databases. Exiting",
                            .{server.dbnum},
                        );
                    }
                    db = &server.db[dbid];
                    continue; // Read next opcode.
                },
                RDB_OPCODE_RESIZEDB => {
                    // RESIZEDB: Hint about the size of the keys in the currently
                    // selected data base, in order to avoid useless rehashing.
                    const db_size = loadLen(rdb, null) catch break :biz;
                    const expires_size = loadLen(rdb, null) catch break :biz;
                    _ = db.dict.expand(db_size);
                    _ = db.expires.expand(expires_size);
                    continue; // Read next opcode.
                },
                RDB_OPCODE_AUX => {
                    // AUX: generic string-string fields. Use to add state to RDB
                    // which is backward compatible. Implementations of RDB loading
                    // are requierd to skip AUX fields they don't understand.
                    //
                    // An AUX field is composed of two strings: key and value.
                    const auxkey = loadStringObject(rdb) catch break :biz;
                    defer auxkey.decrRefCount();
                    const auxval = loadStringObject(rdb) catch break :biz;
                    defer auxval.decrRefCount();
                    const key = sds.castBytes(auxkey.v.ptr);
                    const val = sds.castBytes(auxval.v.ptr);
                    if (key[0] == '%') {
                        // All the fields with a name staring with '%' are considered
                        // information fields and are logged at startup with a log
                        // level of NOTICE.
                        logging.notice(
                            "RDB '{s}': {s}",
                            .{ key, val },
                        );
                    }
                    // TODO: repl-stream-db, repl-id, repl-offset
                    else if (eqlCase(key, "lua")) {
                        // Load the script back in memory.
                        if (server.lua.createFunction(null, auxval) == null) {
                            exitReportCorruptRDB(
                                "Can't load Lua script from RDB file! BODY: {s}",
                                .{val},
                            );
                        }
                    } else {
                        // We ignore fields we don't understand, as by AUX field
                        // contract.
                        logging.debug(
                            "Unrecognized RDB AUX field: '{s}'",
                            .{key},
                        );
                    }
                    continue; // Read type again.
                },
                // TODO: Module
                else => {},
            }

            // Read key and value.
            const key = try loadStringObject(rdb);
            defer key.decrRefCount();
            const val = try loadObject(@"type", rdb, key);
            defer val.decrRefCount();

            // Check if the key already expired. This function is used when loading
            // an RDB file from disk, either at startup, or when an RDB was
            // received from the master. In the latter case, the master is
            // responsible for key expiry. If we would expire keys here, the
            // snapshot taken by the master may not be reflected on the slave.
            if (!loading_aof and expiretime != -1 and expiretime < now) {
                // TODO: server.masterhost == null
                // Do nothing, key and val will be freed.
            } else {
                // Add the new object in the hash table
                db.add(key, val);

                // Set the expire time if needed
                if (expiretime != -1) {
                    db.setExpire(null, key, expiretime);
                }

                // Set usage information (for eviction).
                Object.setLRUOrLFU(val, lfu_freq, lru_idle, lru_clock);
            }

            // Reset the state that is key-specified and is populated by
            // opcodes before the key, so that we start from scratch again.
            expiretime = -1;
            lfu_freq = -1;
            lru_idle = -1;
        }

        // Verify the checksum if RDB version is >= 5
        if (rdbver >= 5) {
            const expected = rdb.cksum.crc;
            var cksum: u64 = undefined;
            try rdb.read(std.mem.asBytes(&cksum));
            if (server.rdb_checksum) {
                cksum = std.mem.littleToNative(@TypeOf(cksum), cksum);
                if (cksum == 0) {
                    logging.warn(
                        "RDB file was saved with checksum disabled: no check performed.",
                        .{},
                    );
                } else if (cksum != expected) {
                    logging.warn("Wrong RDB checksum. Aborting now.", .{});
                    exitReportCorruptRDB("RDB CRC error", .{});
                }
            }
        }

        return;
    }

    // Unexpected end of file is handled here with a fatal exit
    logging.warn(
        "Short read or OOM loading DB. Unrecoverable error, aborting now.",
        .{},
    );
    exitReportCorruptRDB("Unexpected EOF reading RDB file", .{});
}

/// Track loading progress in order to serve client's from time to time
/// and if needed calculate rdb checksum
pub fn loadProgressCallback(rdb: *rio.ReadWriter, buf: []const u8) void {
    if (server.rdb_checksum) {
        rio.genericUpdateChecksum(rdb, buf);
    }
    if (server.loading_process_events_interval_bytes > 0 and
        @divFloor(
            rdb.processed_bytes + buf.len,
            server.loading_process_events_interval_bytes,
        ) >
            @divFloor(
                rdb.processed_bytes,
                server.loading_process_events_interval_bytes,
            ))
    {
        // The DB can take some non trivial amount of time to load. Update
        // our cached time since it is used to create and update the last
        // interaction time with clients and for other important things.
        server.updateCachedTime();
        // TODO: replication
        loadingProgress(rdb.processed_bytes);
        _ = networking.processEventsWhileBlocked() catch {};
    }
}

///  Refresh the loading progress info
pub fn loadingProgress(pos: usize) void {
    server.loading_loaded_bytes = pos;
    // TODO: server stat
}

/// Loading finished
pub fn stopLoading() void {
    server.loading = false;
}

pub fn rdbRemoveTempFile(childpid: posix.pid_t) void {
    var tmpbuf: [255:0]u8 = undefined;
    var cwd = std.fs.cwd();
    const tmpfile = std.fmt.bufPrint(
        &tmpbuf,
        "tmp-{}.rdb",
        .{childpid},
    ) catch unreachable;
    cwd.deleteFile(tmpfile) catch {};
}

/// Save the DB on disk. Return FALSE on error, TRUE on success.
pub fn save(filename: []const u8, rsi: ?*SaveInfo) bool {
    var cwdbuf: [libc.MAXPATHLEN]u8 = undefined;
    var tmpbuf: [255:0]u8 = undefined;

    var cwd = std.fs.cwd();
    const tmpfile = std.fmt.bufPrint(
        &tmpbuf,
        "tmp-{}.rdb",
        .{libc.getpid()},
    ) catch unreachable;
    const fp = cwd.createFile(tmpfile, .{ .truncate = true }) catch |e| {
        logging.warn(
            "Failed opening the RDB file {s} (in server root dir {s}) for saving: {}",
            .{
                filename,
                cwd.realpath(".", &cwdbuf) catch "unknown",
                e,
            },
        );
        return false;
    };

    var fio: rio.FileReadWriter = undefined;
    fio.init(fp);

    if (server.rdb_save_incremental_fsync) { // Server, config
        fio.setAutoSync(Server.REDIS_AUTOSYNC_BYTES);
    }

    var err: anyerror = undefined;
    biz: {
        var rdb = fio.readWriter();
        saveRio(&rdb, RDB_SAVE_NONE, rsi) catch |e| {
            err = e;
            break :biz;
        };

        // Make sure data will not remain on the OS's output buffers
        fio.writer.interface.flush() catch |e| {
            err = e;
            break :biz;
        };
        fio.file.sync() catch |e| {
            err = e;
            break :biz;
        };
        fio.file.close();

        // Use RENAME to make sure the DB file is changed atomically only
        // if the generate DB file is ok.
        cwd.rename(tmpfile, filename) catch |e| {
            logging.warn(
                "Error moving temp DB file {s} on the final " ++
                    "destination {s} (in server root dir {s}): {}",
                .{
                    tmpfile,
                    filename,
                    cwd.realpath(".", &cwdbuf) catch "unknown",
                    e,
                },
            );
            cwd.deleteFile(tmpfile) catch {};
            return false;
        };

        logging.notice("DB saved on disk", .{});
        server.dirty = 0;
        server.lastsave = std.time.timestamp();
        server.lastbgsave_status = true;
        return true;
    }

    // handle failure
    logging.warn("Write error saving DB on disk: {}", .{err});
    fio.file.close();
    cwd.deleteFile(tmpfile) catch {};
    return false;
}

/// Produces a dump of the database in RDB format sending it to the specified
/// Redis I/O channel. On failure error is returned and part of the output,
/// or all the output, can be missing because of I/O errors.
pub fn saveRio(rdb: *rio.ReadWriter, flags: i32, rsi: ?*SaveInfo) !void {
    if (server.rdb_checksum) {
        rdb.updateChecksum = rio.genericUpdateChecksum;
    }
    var magic: [10]u8 = undefined;
    try writeRaw(
        rdb,
        std.fmt.bufPrint(&magic, "REDIS{:0>4}", .{RDB_VERSION}) catch {
            unreachable;
        },
    );
    try saveInfoAuxFields(rdb, flags, rsi);
    // TODO: Modules AUX
    for (server.db, 0..) |*db, i| {
        if (db.dict.size() == 0) continue;
        var di = db.dict.iterator(true);
        defer di.release();

        // Write the SELECT DB opcode
        try saveType(rdb, RDB_OPCODE_SELECTDB);
        try saveLen(rdb, i);

        // Write the RESIZE DB opcode. We trim the size to UINT32_MAX, which
        // is currently the largest type we are able to represent in RDB sizes.
        // However this does not limit the actual size of the DB to load since
        // these sizes are just hints to resize the hash tables.
        try saveType(rdb, RDB_OPCODE_RESIZEDB);
        try saveLen(rdb, db.dict.size());
        try saveLen(rdb, db.expires.size());

        // Iterate this DB writing every entry
        var processed: usize = 0;
        while (di.next()) |de| {
            var key: Object = undefined;
            key.initStaticStringObject(de.key);
            const val = de.val.?;
            const expire = db.getExpire(&key);
            try saveKeyValuePair(rdb, &key, val, expire);

            // When this RDB is produced as part of an AOF rewrite, move
            // accumulated diff from parent to child while rewriting in
            // order to have a smaller final write.
            if (flags & RDB_SAVE_AOF_PREAMBLE != 0 and
                rdb.processed_bytes > processed + Server.AOF_READ_DIFF_INTERVAL_BYTES)
            {
                processed = rdb.processed_bytes;
                _ = aof.readDiffFromParent() catch {};
            }
        }
    }

    // If we are storing the replication information on disk, persist
    // the script cache as well: on successful PSYNC after a restart, we need
    // to be able to process any EVALSHA inside the replication backlog the
    // master will send us.
    if (rsi != null and server.lua.scripts.size() != 0) {
        var di = server.lua.scripts.iterator(false);
        defer di.release();
        while (di.next()) |de| {
            const body = de.val;
            try saveAuxFieldStrStr(rdb, "lua", sds.castBytes(body.v.ptr));
        }
    }

    // EOF opcode
    try saveType(rdb, RDB_OPCODE_EOF);

    // CRC64 checksum. It will be zero if checksum computation is disabled, the
    // loading code skips the check in this case.

    const cksum = std.mem.nativeToLittle(@TypeOf(rdb.cksum.crc), rdb.cksum.crc);
    try rdb.write(std.mem.asBytes(&cksum));
}

/// Save a key-value pair, with expire time, type, key, value.
fn saveKeyValuePair(
    rdb: *rio.ReadWriter,
    key: *Object,
    val: *Object,
    expiretime: i64,
) !void {
    const savelru = server.maxmemory_policy & Server.MAXMEMORY_FLAG_LRU != 0;
    const savelfu = server.maxmemory_policy & Server.MAXMEMORY_FLAG_LFU != 0;

    // Save the expire time.
    if (expiretime != -1) {
        try saveType(rdb, RDB_OPCODE_EXPIRETIME_MS);
        try saveMillisecondTime(rdb, expiretime);
    }

    // Save the LRU info.
    if (savelru) {
        var idletime = evict.estimateObjectIdleTime(val);
        // Using seconds is enough and requires less space.
        idletime = @divFloor(idletime, 1000);
        try saveType(rdb, RDB_OPCODE_IDLE);
        try saveLen(rdb, idletime);
    }

    // Save the LFU info.
    if (savelfu) {
        var buf: [1]u8 = undefined;
        buf[0] = evict.LFUDecrAndReturn(val);
        // We can encode this in exactly two bytes: the opcode and an 8
        // bit counter, since the frequency is logarithmic with a 0-255 range.
        // Note that we do not store the halving time because to reset it
        // a single time when loading does not affect the frequency much.
        try saveType(rdb, RDB_OPCODE_FREQ);
        try writeRaw(rdb, &buf);
    }

    // Save type, key, value.
    try saveObjectType(rdb, val);
    try saveStringObject(rdb, key);
    try saveObject(rdb, val, key);
}

/// Save the object type of object 'obj'.
pub fn saveObjectType(rdb: *rio.ReadWriter, obj: *Object) !void {
    switch (obj.type) {
        .string => {
            try saveType(rdb, RDB_TYPE_STRING);
        },
        .list => {
            if (obj.encoding == .quicklist) {
                try saveType(rdb, RDB_TYPE_LIST_QUICKLIST);
            } else {
                @panic("Unknown list encoding");
            }
        },
        .set => {
            if (obj.encoding == .intset) {
                try saveType(rdb, RDB_TYPE_SET_INTSET);
            } else if (obj.encoding == .ht) {
                try saveType(rdb, RDB_TYPE_SET);
            } else {
                @panic("Unknown set encoding");
            }
        },
        .zset => {
            if (obj.encoding == .ziplist) {
                try saveType(rdb, RDB_TYPE_ZSET_ZIPLIST);
            } else if (obj.encoding == .skiplist) {
                try saveType(rdb, RDB_TYPE_ZSET_2);
            } else {
                @panic("Unknown sorted set encoding");
            }
        },
        .hash => {
            if (obj.encoding == .ziplist) {
                try saveType(rdb, RDB_TYPE_HASH_ZIPLIST);
            } else if (obj.encoding == .ht) {
                try saveType(rdb, RDB_TYPE_HASH);
            } else {
                @panic("Unknown hash encoding");
            }
        },
        // TODO: MODULE
        .stream => {
            try saveType(rdb, RDB_TYPE_STREAM_LISTPACKS);
        },
        else => {
            @panic("Unknown object type");
        },
    }
}

/// Like saveRawString() gets a Redis object instead.
pub fn saveStringObject(rdb: *rio.ReadWriter, obj: *Object) !void {
    if (obj.encoding == .int) {
        try saveLongLongAsStringObject(rdb, obj.v.int);
    } else {
        assert(obj.sdsEncoded());
        try saveRawString(rdb, sds.castBytes(obj.v.ptr));
    }
}

pub fn loadStringObject(rdb: *rio.ReadWriter) !*Object {
    const ptr = try genericLoadStringObject(rdb, RDB_LOAD_NONE, null);
    return @ptrCast(@alignCast(ptr));
}

fn loadEncodedStringObject(rdb: *rio.ReadWriter) !*Object {
    const ptr = try genericLoadStringObject(rdb, RDB_LOAD_ENC, null);
    return @ptrCast(@alignCast(ptr));
}

/// Load a string object from an RDB file according to flags:
///
/// RDB_LOAD_NONE (no flags): load an RDB object, unencoded.
/// RDB_LOAD_ENC: If the returned type is a Redis object, try to
///               encode it in a special way to be more memory
///               efficient. When this flag is passed the function
///               no longer guarantees that obj->ptr is an SDS string.
/// RDB_LOAD_PLAIN: Return a plain string allocated with zmalloc()
///                 instead of a Redis object with an sds in it.
/// RDB_LOAD_SDS: Return an SDS string instead of a Redis object.
pub fn genericLoadStringObject(
    rdb: *rio.ReadWriter,
    flags: i32,
    lenptr: ?*u64,
) !*anyopaque {
    const encode = flags & RDB_LOAD_ENC != 0;
    const plain = flags & RDB_LOAD_PLAIN != 0;
    const string = flags & RDB_LOAD_SDS != 0;

    var isencoded: bool = undefined;
    const len = try loadLen(rdb, &isencoded);
    if (isencoded) {
        switch (len) {
            RDB_ENC_INT8, RDB_ENC_INT16, RDB_ENC_INT32 => {
                return loadIntegerObject(rdb, len, flags, lenptr);
            },
            RDB_ENC_LZF => {
                return loadLzfStringObject(rdb, flags, lenptr);
            },
            else => {
                exitReportCorruptRDB(
                    "Unknown RDB string encoding type {}",
                    .{len},
                );
            },
        }
    }
    if (plain or string) {
        const buf = if (plain)
            allocator.alignedAlloc(u8, std.mem.Alignment.@"16", len)
        else
            sds.asBytes(sds.newLen(allocator.child, sds.NOINIT.ptr, len));
        if (lenptr) |ptr| ptr.* = len;
        errdefer if (plain) {
            allocator.free(buf);
        } else {
            sds.free(allocator.child, buf.ptr);
        };
        try rdb.read(buf);
        return buf.ptr;
    } else {
        var str = sds.NOINIT;
        str.len = len;
        const o = if (encode)
            Object.createString(str)
        else
            Object.createRawString(str);
        errdefer o.decrRefCount();
        try rdb.read(sds.castBytes(o.v.ptr));
        return o;
    }
}

/// Save a long long value as either an encoded string or a string.
fn saveLongLongAsStringObject(rdb: *rio.ReadWriter, value: i64) !void {
    var buf: [32]u8 = undefined;
    const enc = encodeInteger(value, &buf);
    if (enc.len > 0) {
        return writeRaw(rdb, enc);
    }
    // Encode as string
    const s = util.ll2string(&buf, value);
    try saveLen(rdb, s.len);
    try writeRaw(rdb, s);
}

/// Loads an integer-encoded object with the specified encoding type "enctype".
/// The returned value changes according to the flags, see
/// generincLoadStringObject() for more info.
fn loadIntegerObject(
    rdb: *rio.ReadWriter,
    enctype: u64,
    flags: i32,
    lenptr: ?*u64,
) !*anyopaque {
    const plain = flags & RDB_LOAD_PLAIN != 0;
    const string = flags & RDB_LOAD_SDS != 0;
    const encode = flags & RDB_LOAD_ENC != 0;
    var val: i64 = undefined;

    var enc: [4]u8 = undefined;
    if (enctype == RDB_ENC_INT8) {
        try rdb.read(enc[0..1]);
        val = enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        try rdb.read(enc[0..2]);
        val = std.mem.readInt(i16, enc[0..2], .little);
    } else if (enctype == RDB_ENC_INT32) {
        try rdb.read(enc[0..4]);
        val = std.mem.readInt(i32, &enc, .little);
    } else {
        val = 0;
        exitReportCorruptRDB(
            "Unknown RDB integer encoding type {}",
            .{enctype},
        );
    }

    if (plain or string) {
        var buf: [21]u8 = undefined;
        const vs = util.ll2string(&buf, val);
        if (lenptr) |ptr| ptr.* = vs.len;
        var p: []u8 = undefined;
        if (plain) {
            p = allocator.alloc(u8, vs.len);
        } else {
            p = sds.asBytes(sds.newLen(
                allocator.child,
                sds.NOINIT.ptr,
                vs.len,
            ));
        }
        @memcpy(p, vs);
        return p.ptr;
    } else if (encode) {
        return Object.createStringFromLonglongForValue(val);
    } else {
        return Object.create(.string, sds.fromLonglong(allocator.child, val));
    }
}

/// Save a string object as [len][data] on disk. If the object is a string
/// representation of an integer value we try to save it in a special form
fn saveRawString(rdb: *rio.ReadWriter, s: []const u8) !void {
    // Try integer encoding
    if (s.len <= 11) {
        var buf: [5]u8 = undefined;
        const enc = tryIntegerEncoding(s, &buf);
        if (enc.len > 0) {
            return writeRaw(rdb, enc);
        }
    }

    // Try LZF compression - under 20 bytes it's unable to compress even
    // aaaaaaaaaaaaaaaaaa so skip it
    if (server.rdb_compression and s.len > 20) {
        if (try saveLzfString(rdb, s)) {
            return;
        }
        // Return FALSE means data can't be compressed, save the old way
    }

    // Store verbatim
    try saveLen(rdb, s.len);
    try writeRaw(rdb, s);
}

/// Save a Redis object.
pub fn saveObject(rdb: *rio.ReadWriter, obj: *Object, key: *Object) !void {
    _ = key;
    if (obj.type == .string) {
        try saveStringObject(rdb, obj);
    } else if (obj.type == .list) {
        if (obj.encoding == .quicklist) {
            const ql = QuickList.cast(obj.v.ptr);
            try saveLen(rdb, ql.len);
            var node = ql.head;
            while (node) |n| {
                if (n.isCompressed()) {
                    const compressed = n.getLzf();
                    try saveLzfBlob(rdb, compressed, n.sz);
                } else {
                    const zl = ZipList.cast(n.zl.?);
                    try saveRawString(rdb, zl.toBytes());
                }
                node = n.next;
            }
        } else {
            @panic("Unknown list encoding");
        }
    } else if (obj.type == .set) {
        if (obj.encoding == .ht) {
            const Set = @import("t_set.zig").Set;
            const set = Set.Hash.cast(obj.v.ptr);
            var di = set.iterator(false);
            defer di.release();
            try saveLen(rdb, set.size());
            while (di.next()) |de| {
                const ele = de.key;
                try saveRawString(rdb, sds.asBytes(ele));
            }
        } else if (obj.encoding == .intset) {
            const is = IntSet.cast(obj.v.ptr);
            try saveRawString(rdb, is.toBytes());
        } else {
            @panic("Unknown set encoding");
        }
    } else if (obj.type == .zset) {
        if (obj.encoding == .ziplist) {
            const zl: *ZipList = ZipListSet.cast(obj.v.ptr).zl;
            try saveRawString(rdb, zl.toBytes());
        } else if (obj.encoding == .skiplist) {
            const sl = SkipListSet.cast(obj.v.ptr).sl;
            try saveLen(rdb, sl.length);
            // We save the skiplist elements from the greatest to the smallest
            // (that's trivial since the elements are already ordered in the
            // skiplist): this improves the load process, since the next loaded
            // element will always be the smaller, so adding to the skiplist
            // will always immediately stop at the head, making the insertion
            // O(1) instead of O(log(N)).
            var zn = sl.tail;
            while (zn) |n| {
                try saveRawString(rdb, sds.asBytes(n.ele.?));
                try saveBinaryDoubleValue(rdb, n.score);
                zn = n.backward;
            }
        }
    } else if (obj.type == .hash) {
        if (obj.encoding == .ziplist) {
            const zl = ZipList.cast(obj.v.ptr);
            try saveRawString(rdb, zl.toBytes());
        } else if (obj.encoding == .ht) {
            const HashMap = @import("t_hash.zig").Hash.Map;
            const h = HashMap.cast(obj.v.ptr);
            try saveLen(rdb, h.size());
            var di = h.iterator(false);
            defer di.release();
            while (di.next()) |de| {
                const field = de.key;
                const value = de.val;
                try saveRawString(rdb, sds.asBytes(field));
                try saveRawString(rdb, sds.asBytes(value));
            }
        } else {
            @panic("Unknown hash encoding");
        }
    } else if (obj.type == .stream) {
        const s = Stream.cast(obj.v.ptr);
        const rax = s.rax;
        try saveLen(rdb, raxlib.raxSize(rax));
        // Serialize all the listpacks inside the radix tree as they are,
        // when loading back, we'll use the first entry of each listpack
        // to insert it back into the radix tree.
        var ri: raxlib.raxIterator = undefined;
        raxlib.raxStart(&ri, rax);
        defer raxlib.raxStop(&ri);
        _ = raxlib.raxSeek(&ri, "^", null, 0);
        while (raxlib.raxNext(&ri) != 0) {
            const lp = ListPack.cast(ri.data.?);
            try saveRawString(rdb, ri.key[0..ri.key_len]);
            try saveRawString(rdb, lp.asBytes());
        }
        // Save the number of elements inside the stream. We cannot obtain
        // this easily later, since our macro nodes should be checked for
        // number of items: not a great CPU / space tradeoff.
        try saveLen(rdb, s.length);
        // Save the last entry ID.
        try saveLen(rdb, s.last_id.ms);
        try saveLen(rdb, s.last_id.seq);

        // The consumer groups and their clients are part of the stream
        // type, so serialize every consumer group.

        // Save the number of groups.
        const num_cgroups = if (s.cgroups) |groups| raxlib.raxSize(groups) else 0;
        try saveLen(rdb, num_cgroups);
        if (s.cgroups) |cgroups| {
            // Serialize each consumer group.
            var it: raxlib.raxIterator = undefined;
            raxlib.raxStart(&it, cgroups);
            defer raxlib.raxStop(&it);
            _ = raxlib.raxSeek(&it, "^", null, 0);
            while (raxlib.raxNext(&it) != 0) {
                const cg = Stream.CG.cast(ri.data.?);
                // Save the group name.
                try saveRawString(rdb, it.key[0..it.key_len]);
                // Last ID.
                try saveLen(rdb, cg.last_id.ms);
                try saveLen(rdb, cg.last_id.seq);
                // Save the global PEL.
                try saveStreamPEL(rdb, cg.pel, true);
                // Save the consumers of this group.
                try saveStreamConsumers(rdb, cg.consumers);
            }
        }
    }
    // TODO: Module
    else {
        @panic("Unknown object type");
    }
}

/// Load a Redis object of the specified type from the specified file.
pub fn loadObject(rdbtype: u8, rdb: *rio.ReadWriter, key: *Object) !*Object {
    _ = key;
    var o: *Object = undefined;
    switch (rdbtype) {
        RDB_TYPE_STRING => {
            o = try loadEncodedStringObject(rdb);
            o = o.tryEncoding();
        },
        RDB_TYPE_LIST => {
            const len = try loadLen(rdb, null);
            o = Object.createQuickList();
            var ql = QuickList.cast(o.v.ptr);
            ql.setOptions(
                server.list_max_ziplist_size,
                server.list_compress_depth,
            );
            // Load every single element of the list
            for (0..len) |_| {
                const ele = try loadEncodedStringObject(rdb);
                const dec = ele.getDecoded();
                _ = ql.pushTail(sds.castBytes(o.v.ptr));
                dec.decrRefCount();
                ele.decrRefCount();
            }
        },
        RDB_TYPE_LIST_QUICKLIST => {
            const len = try loadLen(rdb, null);
            o = Object.createQuickList();
            var ql = QuickList.cast(o.v.ptr);
            ql.setOptions(
                server.list_max_ziplist_size,
                server.list_compress_depth,
            );
            // Load every single element of the list
            for (0..len) |_| {
                const zl = try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_PLAIN,
                    null,
                );
                ql.appendZiplist(ZipList.cast(zl));
            }
        },
        RDB_TYPE_SET => {
            const Set = @import("t_set.zig").Set;
            const len = try loadLen(rdb, null);
            // Use a regular set when there are too many entries.
            var max_entries = server.set_max_intset_entries;
            if (max_entries >= 1 << 30) max_entries = 1 << 30;
            if (len > max_entries) {
                o = Object.createSet();
                // It's faster to expand the dict to the right size asap in order
                // to avoid rehashing
                if (len > dict.HT_INITIAL_SIZE) {
                    _ = Set.Hash.cast(o.v.ptr).expand(len);
                }
            } else {
                o = Object.createIntSet();
            }

            // Load every single element of the set
            for (0..len) |_| {
                const sdsele = try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                );
                if (o.encoding == .intset) {
                    if (util.string2ll(sds.castBytes(sdsele))) |llval| {
                        const ret = IntSet.cast(o.v.ptr).add(llval);
                        o.v = .{ .ptr = ret.set };
                    } else {
                        Set.convert(o, .ht);
                        _ = Set.Hash.cast(o.v.ptr).expand(len);
                    }
                }
                // This will also be called when the set was just converted
                // to a regular hash table encoded set.
                if (o.encoding == .ht) {
                    _ = Set.Hash.cast(o.v.ptr).add(sds.cast(sdsele), {});
                } else {
                    sds.free(allocator.child, sds.cast(sdsele));
                }
            }
        },
        RDB_TYPE_ZSET, RDB_TYPE_ZSET_2 => {
            var maxelelen: usize = 0;
            var totelelen: usize = 0;

            const zsetlen = try loadLen(rdb, null);
            o = Object.createZset();
            const zs = SkipListSet.cast(o.v.ptr);
            if (zsetlen > dict.HT_INITIAL_SIZE) {
                _ = zs.dict.expand(zsetlen);
            }

            // Load every single element of the sorted set.
            for (0..zsetlen) |_| {
                const sdsele = sds.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ));
                const score = if (rdbtype == RDB_TYPE_ZSET_2)
                    try loadBinaryDoubleValue(rdb)
                else
                    try loadDoubleValue(rdb);

                // Don't care about integer-encoded strings.
                if (sds.getLen(sdsele) > maxelelen) {
                    maxelelen = sds.getLen(sdsele);
                }
                totelelen += sds.getLen(sdsele);

                const node = zs.sl.insert(score, sdsele);
                _ = zs.dict.add(sdsele, &node.score);
            }

            // Convert *after* loading, since sorted sets are not stored ordered.
            if (Zset.length(o) <= server.zset_max_ziplist_entries and
                maxelelen <= server.zset_max_ziplist_value and
                ZipList.safeToAdd(null, totelelen))
            {
                Zset.convert(o, .ziplist);
            }
        },
        RDB_TYPE_HASH => {
            const Hash = @import("t_hash.zig").Hash;
            var len = try loadLen(rdb, null);
            o = Object.createHash();

            // Too many entries? Use a hash table.
            if (len > server.hash_max_ziplist_entries) {
                Hash.convert(o, .ht);
            }

            // Load every field and value into the ziplist
            while (o.encoding == .ziplist and len > 0) {
                len -= 1;
                const field = sds.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ));
                errdefer sds.free(allocator.child, field);
                const value = sds.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ));

                // Convert to hash table if size threshold is exceeded
                if (sds.getLen(field) > server.hash_max_ziplist_value or
                    sds.getLen(value) > server.hash_max_ziplist_value or
                    !ZipList.safeToAdd(
                        ZipList.cast(o.v.ptr),
                        sds.getLen(field) + sds.getLen(value),
                    ))
                {
                    Hash.convert(o, .ht);
                    if (!Hash.Map.cast(o.v.ptr).add(field, value)) {
                        exitReportCorruptRDB(
                            "Duplicate hash fields detected",
                            .{},
                        );
                    }
                    break;
                }

                // Add pair to ziplist
                o.v = .{ .ptr = ZipList.cast(o.v.ptr).push(
                    sds.asBytes(field),
                    .tail,
                ) };
                o.v = .{ .ptr = ZipList.cast(o.v.ptr).push(
                    sds.asBytes(value),
                    .tail,
                ) };
                sds.free(allocator.child, field);
                sds.free(allocator.child, value);
            }

            if (o.encoding == .ht and len > dict.HT_INITIAL_SIZE) {
                _ = Hash.Map.cast(o.v.ptr).expand(len);
            }

            // Load remaining fields and values into the hash table
            while (o.encoding == .ht and len > 0) {
                len -= 1;
                const field = sds.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ));
                errdefer sds.free(allocator.child, field);
                const value = sds.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ));
                // Add pair to hash table
                if (!Hash.Map.cast(o.v.ptr).add(field, value)) {
                    exitReportCorruptRDB(
                        "Duplicate hash fields detected",
                        .{},
                    );
                }
            }

            // All pairs should be read by now
            assert(len == 0);
        },
        // zig fmt: off
        // I don't want to implement zipmap for supporting load
        // RDB_TYPE_HASH_ZIPMAP
        RDB_TYPE_LIST_ZIPLIST,
        RDB_TYPE_SET_INTSET,
        RDB_TYPE_ZSET_ZIPLIST,
        RDB_TYPE_HASH_ZIPLIST => {
            // zig fmt: on
            const encoded = try genericLoadStringObject(
                rdb,
                RDB_LOAD_PLAIN,
                null,
            );
            o = Object.create(.string, encoded); // Obj type fixed below.
            // Fix the object encoding, and make sure to convert the encoded
            // data type into the base type if accordingly to the current
            // configuration there are too many elements in the encoded data
            // type. Note that we only check the length and not max element
            // size as this is an O(N) scan. Eventually everything will get
            // converted.
            switch (rdbtype) {
                RDB_TYPE_LIST_ZIPLIST => {
                    const List = @import("t_list.zig").List;
                    o.type = .list;
                    o.encoding = .ziplist;
                    List.convert(o, .quicklist);
                },
                RDB_TYPE_SET_INTSET => {
                    const Set = @import("t_set.zig").Set;
                    o.type = .set;
                    o.encoding = .intset;
                    if (IntSet.cast(o.v.ptr).length.get() > server.set_max_intset_entries) {
                        Set.convert(o, .ht);
                    }
                },
                RDB_TYPE_ZSET_ZIPLIST => {
                    o.type = .zset;
                    o.encoding = .ziplist;
                    o.v = .{ .ptr = ZipListSet.fromZipList(
                        ZipList.cast(o.v.ptr),
                    ) };
                    if (Zset.length(o) > server.zset_max_ziplist_entries) {
                        Zset.convert(o, .skiplist);
                    }
                },
                RDB_TYPE_HASH_ZIPLIST => {
                    const Hash = @import("t_hash.zig").Hash;
                    o.type = .hash;
                    o.encoding = .ziplist;
                    if (Hash.length(o) > server.hash_max_ziplist_entries) {
                        Hash.convert(o, .ht);
                    }
                },
                else => {
                    exitReportCorruptRDB("Unknown RDB encoding type {}", .{rdbtype});
                },
            }
        },
        RDB_TYPE_STREAM_LISTPACKS => {
            o = Object.createStream();
            const s = Stream.cast(o.v.ptr);
            const listpacks = try loadLen(rdb, null);
            for (0..listpacks) |_| {
                // Get the master ID, the one we'll use as key of the radix tree
                // node: the entries inside the listpack itself are delta-encoded
                // relatively to this ID.
                const nodekey = sds.cast(genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ) catch {
                    exitReportCorruptRDB(
                        "Stream master ID loading failed: invalid encoding or I/O error.",
                        .{},
                    );
                });
                defer sds.free(allocator.child, nodekey);
                if (sds.getLen(nodekey) != @sizeOf(Stream.Id)) {
                    exitReportCorruptRDB(
                        "Stream node key entry is not the size of a stream ID",
                        .{},
                    );
                }
                // Load the listpack.
                const lp = ListPack.cast(try genericLoadStringObject(
                    rdb,
                    RDB_LOAD_PLAIN,
                    null,
                ));
                if (lp.first() == null) {
                    // Serialized listpacks should never be empty, since on
                    // deletion we should remove the radix tree key if the
                    // resulting listpack is empty.
                    exitReportCorruptRDB("Empty listpack inside stream", .{});
                }
                if (raxlib.raxInsert(
                    s.rax,
                    nodekey,
                    sds.getLen(nodekey),
                    lp,
                    null,
                ) == 0) {
                    exitReportCorruptRDB(
                        "Listpack re-added with existing key",
                        .{},
                    );
                }
            }

            // Load total number of items inside the stream.
            s.length = try loadLen(rdb, null);
            // Load the last entry ID.
            s.last_id.ms = try loadLen(rdb, null);
            s.last_id.seq = try loadLen(rdb, null);

            // Consumer groups loading
            const cgroups_count = try loadLen(rdb, null);
            for (0..cgroups_count) |_| {
                // Get the consumer group name and ID. We can then create the
                // consumer group ASAP and populate its structure as
                // we read more data.
                const cgname = sds.cast(genericLoadStringObject(
                    rdb,
                    RDB_LOAD_SDS,
                    null,
                ) catch {
                    exitReportCorruptRDB(
                        "Error reading the consumer group name from Stream",
                        .{},
                    );
                });
                defer sds.free(allocator.child, cgname);
                var cg_id: Stream.Id = undefined;
                cg_id.ms = try loadLen(rdb, null);
                cg_id.seq = try loadLen(rdb, null);
                const cgroup = s.createCG(sds.asBytes(cgname), &cg_id) orelse {
                    exitReportCorruptRDB(
                        "Duplicated consumer group name {s}",
                        .{sds.asBytes(cgname)},
                    );
                };

                // Load the global PEL for this consumer group, however we'll
                // not yet populate the NACK structures with the message
                // owner, since consumers for this group and their messages will
                // be read as a next step. So for now leave them not resolved
                // and later populate it.
                var pel_size = try loadLen(rdb, null);
                for (0..pel_size) |_| {
                    var rawid: [@sizeOf(Stream.Id)]u8 = undefined;
                    loadRaw(rdb, &rawid);
                    const nack = Stream.NACK.create(null);
                    nack.delivery_time = loadMillisecondTime(rdb, RDB_VERSION);
                    nack.delivery_count = try loadLen(rdb, null);
                    if (raxlib.raxInsert(
                        cgroup.pel,
                        &rawid,
                        rawid.len,
                        nack,
                        null,
                    ) == 0) {
                        exitReportCorruptRDB(
                            "Duplicated gobal PEL entry loading stream consumer group",
                            .{},
                        );
                    }
                }

                // Now that we loaded our global PEL, we need to load the
                // consumers and their local PELs.
                const consumer_num = try loadLen(rdb, null);
                for (0..consumer_num) |_| {
                    const cname = sds.cast(genericLoadStringObject(
                        rdb,
                        RDB_LOAD_SDS,
                        null,
                    ) catch {
                        exitReportCorruptRDB(
                            "Error reading the consumer name from Stream group",
                            .{},
                        );
                    });
                    defer sds.free(allocator.child, cname);
                    const consumer = cgroup.lookupConsumer(
                        cname,
                        Stream.SLC_NONE,
                    ).?;
                    consumer.seen_time = loadMillisecondTime(rdb, RDB_VERSION);

                    // Load the PEL about entries owned by this specific
                    // consumer.
                    pel_size = try loadLen(rdb, null);
                    for (0..pel_size) |_| {
                        var rawid: [@sizeOf(Stream.Id)]u8 = undefined;
                        loadRaw(rdb, &rawid);
                        const nack = raxlib.raxFind(cgroup.pel, &rawid, rawid.len);
                        if (nack == raxlib.raxNotFound) {
                            exitReportCorruptRDB(
                                "Consumer entry not found in group global PEL",
                                .{},
                            );
                        }
                        // Set the NACK consumer, that was left to NULL when
                        // loading the global PEL. Then set the same shared
                        // NACK structure also in the consumer-specific PEL.
                        Stream.NACK.cast(nack.?).consumer = consumer;
                        if (raxlib.raxInsert(
                            consumer.pel,
                            &rawid,
                            rawid.len,
                            Stream.NACK.cast(nack.?),
                            null,
                        ) == 0) {
                            exitReportCorruptRDB(
                                "Duplicated consumer PEL entry loading a " ++
                                    "stream consumer group",
                                .{},
                            );
                        }
                    }
                }
            }
        },

        // TODO: Module
        else => {
            exitReportCorruptRDB("Unknown RDB encoding type {}", .{rdbtype});
        },
    }
    return o;
}

/// This helper function serializes a consumer group Pending Entries List (PEL)
/// into the RDB file. The 'nacks' argument tells the function if also persist
/// the informations about the not acknowledged message, or if to persist
/// just the IDs: this is useful because for the global consumer group PEL
/// we serialized the NACKs as well, but when serializing the local consumer
/// PELs we just add the ID, that will be resolved inside the global PEL to
/// put a reference to the same structure.
fn saveStreamPEL(rdb: *rio.ReadWriter, pel: *raxlib.rax, nacks: bool) !void {
    // Number of entries in the PEL.
    try saveLen(rdb, raxlib.raxSize(pel));
    // Save each entry.
    var ri: raxlib.raxIterator = undefined;
    raxlib.raxStart(&ri, pel);
    defer raxlib.raxStop(&ri);
    _ = raxlib.raxSeek(&ri, "^", null, 0);
    while (raxlib.raxNext(&ri) != 0) {
        // We store IDs in raw form as 128 big big endian numbers, like
        // they are inside the radix tree key.
        try writeRaw(rdb, ri.key[0..@sizeOf(Stream.Id)]);
        if (nacks) {
            const nack = Stream.NACK.cast(ri.data.?);
            try saveMillisecondTime(rdb, nack.delivery_time);
            try saveLen(rdb, nack.delivery_count);
            // We don't save the consumer name: we'll save the pending IDs
            // for each consumer in the consumer PEL, and resolve the consumer
            // at loading time.
        }
    }
}

/// Serialize the consumers of a stream consumer group into the RDB. Helper
/// function for the stream data type serialization. What we do here is to
/// persist the consumer metadata, and it's PEL, for each consumer.
fn saveStreamConsumers(rdb: *rio.ReadWriter, consumers: *raxlib.rax) !void {
    //  Number of consumers in this consumer group.
    try saveLen(rdb, raxlib.raxSize(consumers));

    var ri: raxlib.raxIterator = undefined;
    raxlib.raxStart(&ri, consumers);
    defer raxlib.raxStop(&ri);
    _ = raxlib.raxSeek(&ri, "^", null, 0);
    while (raxlib.raxNext(&ri) != 0) {
        const consumer = Stream.Consumer.cast(ri.data.?);
        // Consumer name.
        try saveRawString(rdb, ri.key[0..ri.key_len]);
        // Last seen time.
        try saveMillisecondTime(rdb, consumer.seen_time);
        // Consumer PEL, without the ACKs (see last parameter of the function
        // passed with value of FALSE), at loading time we'll lookup the ID
        // in the consumer group global PEL and will put a reference in the
        // consumer local PEL.
        try saveStreamPEL(rdb, consumer.pel, false);
    }
}

/// Save a few default AUX fields with information about the RDB generated.
fn saveInfoAuxFields(rdb: *rio.ReadWriter, flags: i32, rsi: ?*SaveInfo) !void {
    const bits = @bitSizeOf(*anyopaque);
    const aof_preamble = flags & RDB_SAVE_AOF_PREAMBLE != 0;

    // Add a few fields about the state when the RDB was created.
    try saveAuxFieldStrStr(rdb, "redis-ver", REDIS_VERSION);
    try saveAuxFieldStrInt(rdb, "redis-bits", bits);
    try saveAuxFieldStrInt(rdb, "ctime", std.time.timestamp());
    try saveAuxFieldStrInt(rdb, "used-mem", @intCast(allocator.usedMemory()));

    // Handle saving options that generate aux fields.
    if (rsi) |_| {
        // TODO: repl-stream-db, repl-id, repl-offset
    }
    try saveAuxFieldStrInt(rdb, "aof-preamble", @intFromBool(aof_preamble));
}

fn saveAuxFieldStrInt(rdb: *rio.ReadWriter, key: []const u8, val: i64) !void {
    var buf: [32]u8 = undefined;
    const vlen = util.ll2string(&buf, val);
    try saveAuxFieldStrStr(rdb, key, vlen);
}

fn saveAuxFieldStrStr(
    rdb: *rio.ReadWriter,
    key: []const u8,
    val: []const u8,
) !void {
    try saveType(rdb, RDB_OPCODE_AUX);
    try saveRawString(rdb, key);
    try saveRawString(rdb, val);
}

fn saveType(rdb: *rio.ReadWriter, @"type": u8) !void {
    try writeRaw(rdb, &.{@"type"});
}

/// Load a "type" in RDB format, that is a one byte unsigned integer.
/// This function is not only used to load object types, but also special
/// "types" like the end-of-file type, the EXPIRE type, and so forth.
pub fn loadType(rdb: *rio.ReadWriter) !u8 {
    var t: u8 = undefined;
    try rdb.read(std.mem.asBytes(&t));
    return t;
}

fn saveMillisecondTime(rdb: *rio.ReadWriter, t: i64) !void {
    // Store in little endian.
    const ms = std.mem.nativeToLittle(@TypeOf(t), t);
    try writeRaw(rdb, std.mem.asBytes(&ms));
}

/// This function loads a time from the RDB file. It gets the version of the
/// RDB because, unfortunately, before Redis 5 (RDB version 9), the function
/// failed to convert data to/from little endian, so RDB files with keys having
/// expires could not be shared between big endian and little endian systems
/// (because the expire time will be totally wrong). The fix for this is just
/// to call littleToNative(), however if we fix this for all the RDB versions,
/// this call will introduce an incompatibility for big endian systems:
/// after upgrading to Redis version 5 they will no longer be able to load their
/// own old RDB files. Because of that, we instead fix the function only for new
/// RDB versions, and load older RDB versions as we used to do in the past,
/// allowing big endian systems to load their own old RDB files.
fn loadMillisecondTime(rdb: *rio.ReadWriter, rdbver: u32) i64 {
    var t: i64 = undefined;
    loadRaw(rdb, std.mem.asBytes(&t));
    if (rdbver >= 9) {
        t = std.mem.littleToNative(@TypeOf(t), t);
    }
    return t;
}

/// This is only used to load old databases stored with the RDB_OPCODE_EXPIRETIME
/// opcode. New versions of Redis store using the RDB_OPCODE_EXPIRETIME_MS
/// opcode.
pub fn loadTime(rdb: *rio.ReadWriter) i64 {
    var t: i32 = undefined;
    loadRaw(rdb, std.mem.asBytes(&t));
    return t;
}

fn saveLzfString(rdb: *rio.ReadWriter, s: []const u8) !bool {
    if (s.len <= 4) {
        // We require at least four bytes compression for this to be worth it
        return false;
    }

    const out = allocator.alloc(u8, s.len - 4 + 1);
    defer allocator.free(out);
    const comprlen = lzf.compress(s, out);
    if (comprlen == 0) {
        return false;
    }
    try saveLzfBlob(rdb, out[0..comprlen], s.len);
    return true;
}

fn saveLzfBlob(
    rdb: *rio.ReadWriter,
    data: []const u8,
    original_len: usize,
) !void {
    try writeRaw(rdb, &.{(@as(u8, RDB_ENCVAL) << 6) | RDB_ENC_LZF});
    try saveLen(rdb, data.len);
    try saveLen(rdb, original_len);
    try writeRaw(rdb, data);
}

/// Load an LZF compressed string in RDB format. The returned value
/// changes according to 'flags'. For more info check the
/// rdbGenericLoadStringObject() function.
fn loadLzfStringObject(
    rdb: *rio.ReadWriter,
    flags: i32,
    lenptr: ?*u64,
) !*anyopaque {
    const plain = flags & RDB_LOAD_PLAIN != 0;
    const string = flags & RDB_LOAD_SDS != 0;

    const clen = try loadLen(rdb, null);
    const len = try loadLen(rdb, null);
    const cbuf = allocator.alloc(u8, clen);
    defer allocator.free(cbuf);

    // Allocate our target according to the uncompressed size.
    var val: []u8 = undefined;
    errdefer if (plain) {
        allocator.free(val);
    } else {
        sds.free(allocator.child, val.ptr);
    };

    if (plain) {
        val = allocator.alloc(u8, len);
    } else {
        val = sds.asBytes(sds.newLen(allocator.child, sds.NOINIT.ptr, len));
    }
    if (lenptr) |ptr| ptr.* = len;

    // Load the compressed representation and uncompress it to target.
    try rdb.read(cbuf);
    if (lzf.decompress(cbuf, val) == 0) {
        return error.InvalidLZF;
    }

    if (plain or string) {
        return val.ptr;
    }
    return Object.create(.string, val.ptr);
}

/// Saves an encoded length. The first two bits in the first byte are used to
/// hold the encoding type. See the RDB_* definitions for more information
/// on the types of encoding.
fn saveLen(rdb: *rio.ReadWriter, len: u64) !void {
    var buf: [2]u8 = undefined;
    if (len <= maxInt(u6)) {
        // Save a 6 bit len
        buf[0] = (@as(u8, RDB_6BITLEN) << 6) | @as(u6, @intCast(len));
        try writeRaw(rdb, buf[0..1]);
    } else if (len <= maxInt(u14)) {
        buf[0] = (@as(u8, RDB_14BITLEN) << 6) | @as(u6, @intCast(len >> 8));
        buf[1] = @intCast(len & 0xff);
        try writeRaw(rdb, buf[0..2]);
    } else if (len <= maxInt(u32)) {
        buf[0] = RDB_32BITLEN;
        try writeRaw(rdb, buf[0..1]);
        const len32 = std.mem.nativeToBig(u32, @intCast(len));
        try writeRaw(rdb, std.mem.asBytes(&len32));
    } else {
        buf[0] = RDB_64BITLEN;
        try writeRaw(rdb, buf[0..1]);
        const len64 = std.mem.nativeToBig(u64, len);
        try writeRaw(rdb, std.mem.asBytes(&len64));
    }
}

/// Load an encoded length. If the loaded length is a normal length as stored
/// with saveLen(), the read length is set to '*lenptr'. If instead the
/// loaded length describes a special encoding that follows, then '*isencoded'
/// is set to TRUE and the encoding format is stored at '*lenptr'.
///
/// See the RDB_ENC_* definitions for more information on special encodings.
pub fn loadLenByRef(
    rdb: *rio.ReadWriter,
    isencoded: ?*bool,
    lenptr: *u64,
) !void {
    var buf: [2]u8 = undefined;
    if (isencoded) |enc| enc.* = false;
    try rdb.read(buf[0..1]);
    const typo = (buf[0] & 0b11000000) >> 6;
    if (typo == RDB_ENCVAL) {
        if (isencoded) |enc| enc.* = true;
        // RDB_ENC_INT8, RDB_ENC_INT16, RDB_ENC_INT32, RDB_ENC_LZF
        lenptr.* = buf[0] & 0b00111111;
    } else if (typo == RDB_6BITLEN) {
        lenptr.* = buf[0] & 0b00111111;
    } else if (typo == RDB_14BITLEN) {
        try rdb.read(buf[1..2]);
        lenptr.* = (@as(u64, buf[0] & 0b00111111) << 8) | buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        var len: u32 = undefined;
        try rdb.read(std.mem.asBytes(&len));
        lenptr.* = std.mem.bigToNative(@TypeOf(len), len);
    } else if (buf[0] == RDB_64BITLEN) {
        var len: u64 = undefined;
        try rdb.read(std.mem.asBytes(&len));
        lenptr.* = std.mem.bigToNative(@TypeOf(len), len);
    } else {
        exitReportCorruptRDB(
            "Unknown length encoding {} in rdb.loadLenByRef()",
            .{typo},
        );
    }
}

/// This is like loadLenByRef() but directly returns the value read
// from the RDB stream.
pub fn loadLen(rdb: *rio.ReadWriter, isencoded: ?*bool) !u64 {
    var len: u64 = undefined;
    try loadLenByRef(rdb, isencoded, &len);
    return len;
}

/// Saves a double for RDB 8 or greater, where IE754 binary64 format is assumed.
/// We just make sure the integer is always stored in little endian, otherwise
/// the value is copied verbatim from memory to disk.
pub fn saveBinaryDoubleValue(rdb: *rio.ReadWriter, val: f64) !void {
    const rev = std.mem.nativeToLittle(@TypeOf(val), val);
    try writeRaw(rdb, std.mem.asBytes(&rev));
}

/// Loads a double from RDB 8 or greater. See saveBinaryDoubleValue() for
/// more info.
pub fn loadBinaryDoubleValue(rdb: *rio.ReadWriter) !f64 {
    var v: f64 = undefined;
    try rdb.read(std.mem.asBytes(&v));
    return std.mem.littleToNative(@TypeOf(v), v);
}

fn loadDoubleValue(rdb: *rio.ReadWriter) !f64 {
    const math = @import("math.zig");
    var buf: [256]u8 = undefined;
    var len: u8 = undefined;
    try rdb.read(std.mem.asBytes(&len));
    switch (len) {
        255 => return math.NegInf,
        254 => return math.PosInf,
        253 => return math.Nan,
        else => {
            try rdb.read(buf[0..len]);
            buf[len] = 0;
            return std.fmt.parseFloat(f64, buf[0..len]);
        },
    }
}

/// String objects in the form "2391" "-100" without any space and with a
/// range of values that can fit in an 8, 16 or 32 bit signed value can be
/// encoded as integers to save space
fn tryIntegerEncoding(s: []const u8, enc: []u8) []u8 {
    var buf: [32]u8 = undefined;
    // Check if it's possible to encode this value as a number
    const value = util.string2ll(s) orelse {
        return s[0..0];
    };
    const ls = util.ll2string(&buf, value);
    if (!std.mem.eql(u8, ls, s)) {
        return s[0..0];
    }
    return encodeInteger(value, enc);
}

/// Encodes the "value" argument as integer when it fits in the supported ranges
/// for encoded types. If the function successfully encodes the integer, the
/// representation is stored in the buffer pointer to by "enc" and the string is
/// returned. Otherwise empty string is returned.
fn encodeInteger(value: i64, enc: []u8) []u8 {
    if (value >= minInt(i8) and value <= maxInt(i8)) {
        enc[0] = (@as(u8, RDB_ENCVAL) << 6) | RDB_ENC_INT8;
        enc[1] = @bitCast(@as(i8, @intCast(value)));
        return enc[0..2];
    } else if (value >= minInt(i16) and value <= maxInt(i16)) {
        enc[0] = (@as(u8, RDB_ENCVAL) << 6) | RDB_ENC_INT16;
        std.mem.writeInt(i16, enc[1..3], @intCast(value), .little);
        return enc[0..3];
    } else if (value >= minInt(i32) and value <= maxInt(i32)) {
        enc[0] = (@as(u8, RDB_ENCVAL) << 6) | RDB_ENC_INT32;
        std.mem.writeInt(i32, enc[1..5], @intCast(value), .little);
        return enc[0..5];
    } else {
        return enc[0..0];
    }
}

fn writeRaw(rdb: *rio.ReadWriter, data: []const u8) !void {
    try rdb.write(data);
}

/// This is just a wrapper for the low level function rio.ReaderWriter.read()
/// that will automatically abort if it is not possible to read the specified
/// amount of bytes.
fn loadRaw(rdb: *rio.ReadWriter, buf: []u8) void {
    rdb.read(buf) catch |err| {
        exitReportCorruptRDB(
            "Impossible to read {} bytes in rdb.loadRaw(): {}",
            .{ buf.len, err },
        );
    };
}

fn exitReportCorruptRDB(comptime format: []const u8, args: anytype) noreturn {
    logging.warn(
        format,
        args,
    );
    std.process.exit(1);
}

// TODO:
pub const SaveInfo = struct {
    pub fn populate(self: *SaveInfo) ?*SaveInfo {
        _ = self;
        return null;
    }
};

const REDIS_VERSION = @import("version.zig").REDIS_VERSION;
const Object = @import("Object.zig");
const Client = @import("networking.zig").Client;
const Server = @import("Server.zig");
const server = &Server.instance;
const std = @import("std");
const posix = std.posix;
const libc = @cImport({
    @cInclude("unistd.h");
    @cInclude("sys/param.h");
    @cInclude("sys/signal.h");
    @cInclude("stdio.h");
});
const logging = @import("logging.zig");
const rio = @import("rio.zig");
const util = @import("util.zig");
const allocator = @import("allocator.zig");
const aof = @import("aof.zig");
const sds = @import("sds.zig");
const evict = @import("evict.zig");
const assert = std.debug.assert;
const maxInt = std.math.maxInt;
const minInt = std.math.minInt;
const lzf = @import("lzf.zig");
const QuickList = @import("QuickList.zig");
const ZipList = @import("ZipList.zig");
const IntSet = @import("IntSet.zig");
const zset = @import("t_zset.zig");
const Zset = zset.Zset;
const SkipListSet = zset.SkipListSet;
const ZipListSet = zset.ZipListSet;
const Stream = @import("t_stream.zig").Stream;
const raxlib = @import("rax.zig").rax;
const ListPack = @import("ListPack.zig");
const eqlCase = std.ascii.eqlIgnoreCase;
const childinfo = @import("childinfo.zig");
const networking = @import("networking.zig");
const dict = @import("dict.zig");
