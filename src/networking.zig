const MAX_ACCEPTS_PER_CALL = 1000;

const ERR_DENIED =
    "-DENIED Redis is running in protected mode because protected " ++
    "mode is enabled, no bind address was specified, no " ++
    "authentication password is requested to clients. In this mode " ++
    "connections are only accepted from the loopback interface. " ++
    "If you want to connect from external computers to Redis you " ++
    "may adopt one of the following solutions: " ++
    "1) Just disable protected mode sending the command " ++
    "'CONFIG SET protected-mode no' from the loopback interface " ++
    "by connecting to Redis from the same host the server is " ++
    "running, however MAKE SURE Redis is not publicly accessible " ++
    "from internet if you do so. Use CONFIG REWRITE to make this " ++
    "change permanent. " ++
    "2) Alternatively you can just disable the protected mode by " ++
    "editing the Redis configuration file, and setting the protected " ++
    "mode option to 'no', and then restarting the server. " ++
    "3) If you started the server manually just for testing, restart " ++
    "it with the '--protected-mode no' option. " ++
    "4) Setup a bind address or an authentication password. " ++
    "NOTE: You only need to do one of the above things in order for " ++
    "the server to start accepting connections from the outside.\r\n";

pub const Client = struct {
    const BlockingState = struct {
        const Keys = struct {
            const HashMap = dict.Dict(*Object, *blocked.BlockInfo);

            const vtable: *const HashMap.VTable = &.{
                .hash = hash,
                .eql = eql,
                .dupeKey = dupeKey,
                .freeKey = freeKey,
                .freeVal = freeVal,
            };

            fn hash(key: *Object) dict.Hash {
                var o = key;
                if (o.sdsEncoded()) {
                    return dict.genHash(sds.asBytes(sds.cast(o.v.ptr)));
                }
                if (o.encoding == .int) {
                    var buf: [32]u8 = undefined;
                    const s = util.ll2string(&buf, o.v.int);
                    return dict.genHash(s);
                }
                o = o.getDecoded();
                defer o.decrRefCount();
                return dict.genHash(sds.asBytes(sds.cast(o.v.ptr)));
            }

            fn eql(k1: *Object, k2: *Object) bool {
                var o1 = k1;
                var o2 = k2;

                if (o1.encoding == .int and o2.encoding == .int) {
                    return o1.v.int == o2.v.int;
                }

                o1 = o1.getDecoded();
                defer o1.decrRefCount();
                o2 = o2.getDecoded();
                defer o2.decrRefCount();

                return sds.cmp(sds.cast(o1.v.ptr), sds.cast(o2.v.ptr)) == .eq;
            }

            fn dupeKey(key: *Object) *Object {
                key.incrRefCount();
                return key;
            }

            fn freeKey(key: *Object) void {
                key.decrRefCount();
            }

            fn freeVal(val: *blocked.BlockInfo) void {
                allocator.destroy(val);
            }
        };

        // Blocking operation timeout(ms). If UNIX current time
        // is > timeout then the operation timed out
        timeout: i64,

        // The keys we are waiting to terminate a blocking
        // operation such as BLPOP or XREAD. Or NULL.
        keys: *Keys.HashMap,

        // The key that should receive the element,
        // for BRPOPLPUSH.
        target: ?*Object,

        fn create() BlockingState {
            return .{
                .timeout = 0,
                .keys = Keys.HashMap.create(Keys.vtable),
                .target = null,
            };
        }
    };
    const ReplyBlock = struct {
        size: usize,
        used: usize,
        buffer: [0]u8,

        fn create(size: usize) *ReplyBlock {
            const ptr = allocator.alignedAlloc(
                u8,
                .of(ReplyBlock),
                @sizeOf(ReplyBlock) + size,
            );
            const reply: *ReplyBlock = @ptrCast(@alignCast(ptr));
            reply.size = size;
            reply.used = 0;
            return reply;
        }

        fn dupe(reply: *ReplyBlock) *ReplyBlock {
            const old_bytes = reply.asBytes();
            const new = ReplyBlock.create(reply.size);
            const new_bytes = new.asBytes();
            @memcpy(new_bytes, old_bytes);
            return @ptrCast(@alignCast(new_bytes));
        }

        fn free(reply: *ReplyBlock) void {
            allocator.free(reply.asBytes());
        }

        fn asBytes(self: *ReplyBlock) []u8 {
            const ptr: [*]align(@alignOf(ReplyBlock)) u8 = @ptrCast(@alignCast(self));
            return ptr[0 .. @sizeOf(ReplyBlock) + self.size];
        }

        fn buf(self: *ReplyBlock) []u8 {
            const hd = @sizeOf(ReplyBlock);
            return self.asBytes()[hd..];
        }
    };

    id: usize, // Client incremental unique ID.
    fd: i32, // Client socket.
    flags: i32, // Client flags: Server.CLIENT_* constants.
    db: *db.Database,
    client_list_node: ?*Server.ClientList.Node,
    reqtype: i32, // Request protocol type: Server.PROTO_REQ_*
    querybuf_peak: usize, // Recent (100ms or more) peak of querybuf size.
    querybuf: sds.String, // Buffer we use to accumulate client queries.
    qb_pos: usize, // The position we have read in querybuf.
    argv: ?[]*Object, // Arguments of current command.
    argc: usize, // Num of arguments of current command.
    cmd: ?*const Server.Command,
    lastcmd: ?*const Server.Command,
    multibulklen: i32, // Number of multi bulk arguments left to read.
    bulklen: i64, // Length of bulk argument in multi bulk request.
    authenticated: bool, // When Server.requirepass is non-NULL.
    lastinteraction: i64, // Time of the last interaction, used for timeout
    // Response
    reply: *LinkedList(void, *ReplyBlock), // List of reply objects to send to the client.
    reply_bytes: usize, // Total bytes of objects in reply list.
    sentlen: usize, // Amount of bytes already sent in the current buffer or object being sent.
    buf: [Server.PROTO_REPLY_CHUNK_BYTES]u8,
    bufpos: usize,
    // Blocking
    btype: i32, // Type of blocking op if Server.CLIENT_BLOCKED.
    bpop: BlockingState, // blocking state

    pub fn create(fd: i32, flags: i32) !*Client {
        const cli = allocator.create(Client);
        errdefer allocator.destroy(cli);

        // passing -1 as fd it is possible to create a non connected client.
        // This is useful since all the commands needs to be executed
        // in the context of a client. When commands are executed in other
        // contexts (for instance a Lua script) we need a non connected client.
        if (fd != -1) {
            try anet.nonBlock(fd);
            if (flags & Server.CLIENT_UNIX_SOCKET == 0) {
                try anet.enableTcpNoDelay(fd);
            }
            if (server.tcpkeepalive != 0) {
                try anet.keepAlive(fd, server.tcpkeepalive);
            }
            try server.el.createFileEvent(
                fd,
                ae.READABLE,
                readQueryFromClient,
                cli,
            );
        }
        _ = db.select(cli, 0);
        cli.id = server.next_client_id.getIncr(1);
        cli.fd = fd;
        cli.flags = flags;
        cli.client_list_node = null;
        cli.reqtype = 0;
        cli.querybuf_peak = 0;
        cli.querybuf = sds.empty();
        cli.qb_pos = 0;
        cli.argv = null;
        cli.argc = 0;
        cli.cmd = null;
        cli.lastcmd = null;
        cli.multibulklen = 0;
        cli.bulklen = -1;
        cli.authenticated = false;
        cli.lastinteraction = server.unixtime.get();
        cli.reply = .create(&.{
            .freeVal = ReplyBlock.free,
            .dupVal = ReplyBlock.dupe,
        });
        cli.reply_bytes = 0;
        cli.sentlen = 0;
        cli.buf = undefined;
        cli.bufpos = 0;
        cli.btype = Server.BLOCKED_NONE;
        cli.bpop = BlockingState.create();
        if (fd != -1) cli.link();
        return cli;
    }

    /// This function links the client to the global linked list of clients.
    /// unlink() does the opposite, among other things.
    pub fn link(self: *Client) void {
        server.clients.append(self);
        self.client_list_node = server.clients.last;
        _ = server.clients_index.insert(std.mem.asBytes(&self.id), self);
    }

    pub fn unlink(self: *Client) void {
        // Certain operations must be done only if the client has an active socket.
        // If the client was already unlinked or if it's a "fake client" the
        // fd is already set to -1.
        if (self.fd != -1) {
            // Remove from the list of active clients.
            if (self.client_list_node) |node| {
                _ = server.clients_index.remove(std.mem.asBytes(&self.id));
                server.clients.removeNode(node);
                self.client_list_node = null;
            }
            // Unregister async I/O handlers and close the socket.
            server.el.deleteFileEvent(
                self.fd,
                ae.READABLE | ae.WRITABLE,
            ) catch {};
            posix.close(self.fd);
            self.fd = -1;
        }

        // Remove from the list of pending writes if needed.
        if (self.flags & Server.CLIENT_PENDING_WRITE != 0) {
            const ln = server.clients_pending_write.search(self) orelse {
                unreachable;
            };
            server.clients_pending_write.removeNode(ln);
            self.flags &= ~@as(i32, Server.CLIENT_PENDING_WRITE);
        }

        // When client was just unblocked because of a blocking operation,
        // remove it from the list of unblocked clients.
        if (self.flags & Server.CLIENT_UNBLOCKED != 0) {
            const ln = server.unblocked_clients.search(self) orelse {
                unreachable;
            };
            server.unblocked_clients.removeNode(ln);
            self.flags &= ~@as(i32, Server.CLIENT_UNBLOCKED);
        }

        // If this client was scheduled for async freeing we need to remove it
        // from the queue.
        if (self.flags & Server.CLIENT_CLOSE_ASAP != 0) {
            const ln = server.clients_to_close.search(self) orelse {
                unreachable;
            };
            server.clients.removeNode(ln);
        }
    }

    /// This is a wrapper for processInputBuffer that also cares about handling
    /// the replication forwarding to the sub-slaves, in case the client 'cli'
    /// is flagged as master. Usually you want to call this instead of the
    /// raw processInputBuffer().
    pub fn processInputBufferAndReplicate(self: *Client) void {
        self.processInputBuffer();
    }

    /// This function is called every time, in the client structure 'c', there is
    /// more query buffer to process, because we read more data from the socket
    /// or because a client was blocked and later reactivated, so there could be
    /// pending query buffer, already representing a full command, to process.
    pub fn processInputBuffer(self: *Client) void {
        while (self.qb_pos < sds.getLen(self.querybuf)) {
            // Immediately abort if the client is in the middle of something.
            if (self.flags & Server.CLIENT_BLOCKED != 0) {
                break;
            }

            // CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
            // written to the client. Make sure to not let the reply grow after
            // this flag has been set (i.e. don't process more commands).
            //
            // The same applies for clients we want to terminate ASAP.
            if (self.flags & (Server.CLIENT_CLOSE_ASAP | Server.CLIENT_CLOSE_AFTER_REPLY) != 0) {
                break;
            }

            // Determine request type when unknown.
            if (self.reqtype == 0) {
                if (self.querybuf[self.qb_pos] == '*') {
                    self.reqtype = Server.PROTO_REQ_MULTIBULK;
                } else {
                    self.reqtype = Server.PROTO_REQ_INLINE;
                }
            }

            if (self.reqtype == Server.PROTO_REQ_INLINE) {
                const ok = self.processInlineBuffer();
                if (!ok) break;
            } else if (self.reqtype == Server.PROTO_REQ_MULTIBULK) {
                const ok = self.processMultibulkBuffer();
                if (!ok) break;
            } else {
                @panic("Unknown request type");
            }

            if (self.argc == 0) {
                self.reset();
            } else {
                if (server.processCommand(self)) {
                    // Don't reset the client structure for clients blocked in a
                    // module blocking command, so that the reply callback will
                    // still be able to access the client argv and argc field.
                    if (self.flags & Server.CLIENT_BLOCKED == 0 or
                        self.btype != Server.BLOCKED_MODULE)
                    {
                        self.reset();
                    }
                }
            }
        }

        // Trim to pos
        if (self.qb_pos != 0) {
            sds.range(self.querybuf, @intCast(self.qb_pos), -1);
            self.qb_pos = 0;
        }
    }

    /// Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
    /// this function consumes the client query buffer and creates a command ready
    /// to be executed inside the client structure. Returns true if the command
    /// is ready to be executed, or false if there is still protocol to read to
    /// have a well formed command. The function also returns false when there is
    /// a protocol error: in such a case the client structure is setup to reply
    /// with the error and close the connection.
    fn processInlineBuffer(self: *Client) bool {
        const querybuf = sds.asBytes(self.querybuf);

        var newline = std.mem.indexOfScalar(u8, querybuf[self.qb_pos..], '\n') orelse {
            if (querybuf.len - self.qb_pos > Server.PROTO_INLINE_MAX_SIZE) {
                self.addReplyErr(
                    "Protocol error: too big inline request",
                );
                self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
            }
            return false;
        };
        var linefeed: usize = 1;
        if (newline != 0 and querybuf[self.qb_pos + newline - 1] == '\r') {
            newline -= 1;
            linefeed += 1;
        }
        const query = querybuf[self.qb_pos .. self.qb_pos + newline];
        const args = sds.splitArgs(query) orelse {
            self.addReplyErr(
                "Protocol error: unbalanced quotes in request",
            );
            self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
            return false;
        };
        defer allocator.free(args); // don't free sds.String

        // Move querybuffer position to the next query in the buffer.
        self.qb_pos += newline + linefeed;

        // Create redis objects for all arguments.
        if (args.len != 0) {
            self.argv = allocator.alloc(*Object, args.len);
            self.argc = 0;
            for (args, 0..) |arg, i| {
                self.argv.?[i] = Object.create(.string, arg);
                self.argc += 1;
            }
        }
        return true;
    }

    /// Process the query buffer for client, setting up the client argument
    /// vector for command execution. Returns true if after running the function
    /// the client has a well-formed ready to be processed command, otherwise
    /// false if there is still to read more buffer to get the full command.
    /// The function also returns false when there is a protocol error: in such a
    /// case the client structure is setup to reply with the error and close
    /// the connection.
    ///
    /// This function is called if processInputBuffer() detects that the next
    /// command is in RESP format, so the first byte in the command is found
    /// to be '*'. Otherwise for inline commands processInlineBuffer() is called.
    fn processMultibulkBuffer(self: *Client) bool {
        var querybuf = sds.asBytes(self.querybuf);
        if (self.multibulklen == 0) {
            assert(self.argc == 0);
            // Multi bulk length cannot be read without a \r\n
            const newline = std.mem.indexOfScalar(
                u8,
                querybuf[self.qb_pos..],
                '\r',
            ) orelse {
                if (querybuf.len - self.qb_pos > Server.PROTO_INLINE_MAX_SIZE) {
                    self.addReplyErr(
                        "Protocol error: too big mbulk count string",
                    );
                    self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                }
                return false;
            };
            // Buffer should also contain \n
            if (newline > querybuf.len - self.qb_pos - 2) {
                return false;
            }

            assert(querybuf[self.qb_pos] == '*');
            var multibulklen: i64 = undefined;
            const ok = util.string2ll(
                querybuf[self.qb_pos + 1 .. self.qb_pos + newline],
                &multibulklen,
            );
            if (!ok or multibulklen > 1024 * 1024) {
                self.addReplyErr(
                    "Protocol error: invalid multibulk length",
                );
                self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                return false;
            } else if (multibulklen > 10 and
                server.requirepass != null and !self.authenticated)
            {
                self.addReplyErr(
                    "Protocol error: unauthenticated multibulk length",
                );
                self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                return false;
            }

            self.qb_pos += newline + 2;
            if (multibulklen <= 0) return true;

            self.multibulklen = @intCast(multibulklen);

            self.argv = allocator.alloc(
                *Object,
                @intCast(self.multibulklen),
            );
        }

        const argv = self.argv.?;

        assert(self.multibulklen > 0);
        while (self.multibulklen > 0) {
            // Read bulk length if unknown
            if (self.bulklen == -1) {
                const newline = std.mem.indexOfScalar(
                    u8,
                    querybuf[self.qb_pos..],
                    '\r',
                ) orelse {
                    if (querybuf.len - self.qb_pos > Server.PROTO_INLINE_MAX_SIZE) {
                        self.addReplyErr(
                            "Protocol error: too big bulk count string",
                        );
                        self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                        return false;
                    }
                    break;
                };
                // Buffer should also contain \n
                if (newline > querybuf.len - self.qb_pos - 2) {
                    break;
                }

                if (querybuf[self.qb_pos] != '$') {
                    self.addReplyErrFormat(
                        "Protocol error: expected '$', got '{c}'",
                        .{querybuf[self.qb_pos]},
                    );
                    self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                    return false;
                }

                var bulklen: i64 = undefined;
                const ok = util.string2ll(
                    querybuf[self.qb_pos + 1 .. self.qb_pos + newline],
                    &bulklen,
                );
                if (!ok or bulklen < 0 or bulklen > server.proto_max_bulk_len) {
                    self.addReplyErr(
                        "Protocol error: invalid bulk length",
                    );
                    self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                    return false;
                } else if (bulklen > 16384 and
                    server.requirepass != null and !self.authenticated)
                {
                    self.addReplyErr(
                        "Protocol error: unauthenticated bulk length",
                    );
                    self.flags |= Server.CLIENT_CLOSE_AFTER_REPLY;
                    return false;
                }
                self.qb_pos += newline + 2;
                if (bulklen >= Server.PROTO_MBULK_BIG_ARG) {
                    // If we are going to read a large object from network
                    // try to make it likely that it will start at c->querybuf
                    // boundary so that we can optimize object creation
                    // avoiding a large copy of data.
                    //
                    // But only when the data we have not parsed is less than
                    // or equal to ll+2. If the data length is greater than
                    // ll+2, trimming querybuf is just a waste of time, because
                    // at this time the querybuf contains not only our bulk.
                    if (querybuf.len - self.qb_pos <= bulklen + 2) {
                        sds.range(self.querybuf, @intCast(self.qb_pos), -1);
                        self.qb_pos = 0;
                        // Hint the sds library about the amount of bytes this
                        // string is going to contain.
                        self.querybuf = sds.makeRoomFor(
                            self.querybuf,
                            @intCast(bulklen + 2),
                        );
                        querybuf = sds.asBytes(self.querybuf);
                    }
                }
                self.bulklen = bulklen;
            }

            // Read bulk argument
            if (querybuf.len - self.qb_pos < self.bulklen + 2) {
                // Not enough data (+2 == trailing \r\n)
                break;
            }
            // Optimization: if the buffer contains JUST our bulk element
            // instead of creating a new object by *copying* the sds we
            // just use the current sds string.
            if (self.qb_pos == 0 and
                self.bulklen >= Server.PROTO_MBULK_BIG_ARG and
                querybuf.len == self.bulklen + 2)
            {
                argv[self.argc] = Object.create(
                    .string,
                    self.querybuf,
                );
                sds.incrLen(self.querybuf, -2); // remove CRLF
                self.argc += 1;
                // Assume that if we saw a fat argument we'll see another one
                // likely...
                self.querybuf = sds.newLen(
                    null,
                    @intCast(self.bulklen + 2),
                );
                sds.clear(self.querybuf);
                querybuf = sds.asBytes(self.querybuf);
            } else {
                argv[self.argc] = Object.createString(
                    self.querybuf[self.qb_pos .. self.qb_pos + @as(usize, @intCast(self.bulklen))],
                );
                self.qb_pos += @as(usize, @intCast(self.bulklen)) + 2;
                self.argc += 1;
            }
            self.bulklen = -1;
            self.multibulklen -= 1;
        }

        //We're done when c->multibulk == 0
        if (self.multibulklen == 0) return true;

        // Still not ready to process the command
        return false;
    }

    /// Prepare the client to process the next command
    fn reset(self: *Client) void {
        self.freeArgv();
        self.reqtype = 0;
        self.multibulklen = 0;
        self.bulklen = -1;
    }

    fn freeArgv(self: *Client) void {
        if (self.argv) |argv| {
            for (0..self.argc) |i| {
                argv[i].decrRefCount();
            }
            allocator.free(argv);
        }
        self.argv = null;
        self.argc = 0;
        self.cmd = null;
    }

    pub fn addReplyHelp(self: *Client, help: []const []const u8) void {
        const argv = self.argv orelse unreachable;
        self.addReplyMultiBulkLen(1 + help.len + 2);

        const cmd = sds.new(sds.asBytes(sds.cast(argv[0].v.ptr)));
        defer sds.free(cmd);
        sds.toUpper(cmd);
        self.addReplyStatusFormat(
            "{s} <subcommand> arg arg ... arg. Subcommands are:",
            .{sds.asBytes(cmd)},
        );

        for (help) |h| {
            self.addReplyStatus(h);
        }

        self.addReplyStatus("HELP");
        self.addReplyStatus("    Print this help.");
    }

    pub fn addReplyStatusFormat(
        self: *Client,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        var status = sds.empty();
        defer sds.free(status);
        status = sds.catPrintf(status, fmt, args);
        self.addReplyStatus(sds.asBytes(status));
    }

    pub fn addReplyStatus(self: *Client, status: []const u8) void {
        self.addReplyString("+");
        self.addReplyString(status);
        self.addReplyString("\r\n");
    }

    /// Add a suggestive error reply.
    /// This function is typically invoked by from commands that support
    /// subcommands in response to an unknown subcommand or argument error.
    pub fn addReplySubcommandSyntaxError(self: *Client) void {
        const argv = self.argv orelse unreachable;
        const cmd = sds.new(sds.asBytes(sds.cast(argv[0].v.ptr)));
        defer sds.free(cmd);
        sds.toUpper(cmd);
        // zig fmt: off
        self.addReplyErrFormat(
            "Unknown subcommand or wrong number of arguments for '{s}'. " ++
            "Try {s} HELP.",
            .{
                sds.asBytes(sds.cast(argv[1].v.ptr)),
                sds.asBytes(cmd),
            },
        );
        // zig fmt: on
    }

    pub fn addReplyLongLong(self: *Client, ll: i64) void {
        switch (ll) {
            0 => self.addReply(Server.shared.czero),
            1 => self.addReply(Server.shared.cone),
            else => self.addReplyLongLongWithPrefix(ll, ':'),
        }
    }

    pub fn addReplyBulkLongLong(self: *Client, ll: i64) void {
        var buf: [64]u8 = undefined;
        const str = util.ll2string(&buf, ll);
        self.addReplyBulkString(str);
    }

    pub fn addReplyMultiBulkLen(self: *Client, len: usize) void {
        if (len < Server.shared.mbulkhdr.len) {
            self.addReply(Server.shared.mbulkhdr[len]);
        } else {
            self.addReplyLongLongWithPrefix(@intCast(len), '*');
        }
    }

    /// Add a Redis Object as a bulk reply
    pub fn addReplyBulk(self: *Client, obj: *Object) void {
        self.addReplyBulkLen(obj);
        self.addReply(obj);
        self.addReply(Server.shared.crlf);
    }

    pub fn addReplyBulkString(self: *Client, buf: []const u8) void {
        self.addReplyLongLongWithPrefix(@intCast(buf.len), '$');
        self.addReplyString(buf);
        self.addReply(Server.shared.crlf);
    }

    /// Create the length prefix of a bulk reply, example: $2234
    fn addReplyBulkLen(self: *Client, obj: *Object) void {
        var len: usize = 0;
        if (obj.sdsEncoded()) {
            len = sds.getLen(sds.cast(obj.v.ptr));
        } else {
            len = util.sdigits10(obj.v.int);
        }
        if (len < Server.shared.bulkhdr.len) {
            self.addReply(Server.shared.bulkhdr[len]);
        } else {
            self.addReplyLongLongWithPrefix(@intCast(len), '$');
        }
    }

    /// Add a long long as integer reply or bulk len / multi bulk count.
    /// Basically this is used to output <prefix><long long><crlf>.
    fn addReplyLongLongWithPrefix(self: *Client, len: i64, prefix: u8) void {
        if (prefix == '*' and len < Server.shared.mbulkhdr.len and len > 0) {
            self.addReply(Server.shared.mbulkhdr[@intCast(len)]);
            return;
        }
        if (prefix == '$' and len < Server.shared.bulkhdr.len and len > 0) {
            self.addReply(Server.shared.bulkhdr[@intCast(len)]);
            return;
        }
        var buf: [128]u8 = undefined;
        buf[0] = prefix;
        const bytes = util.ll2string(buf[1..], len);
        buf[bytes.len + 1] = '\r';
        buf[bytes.len + 2] = '\n';
        self.addReplyString(buf[0 .. bytes.len + 3]);
    }

    /// Add the object 'obj' string representation to the client output buffer.
    pub fn addReply(self: *Client, obj: *const Object) void {
        var buf: [32]u8 = undefined;
        var s: []u8 = undefined;

        if (obj.sdsEncoded()) {
            s = sds.asBytes(sds.cast(obj.v.ptr));
        } else if (obj.encoding == .int) {
            s = util.ll2string(&buf, obj.v.int);
        } else {
            @panic("Wrong obj.encoding in addReply()");
        }

        self.addReplyString(s);
    }

    pub fn addReplyErrFormat(
        self: *Client,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        var s = sds.empty();
        defer sds.free(s);
        s = sds.catPrintf(s, fmt, args);
        const bytes = sds.asBytes(s);
        // Make sure there are no newlines in the string, otherwise invalid protocol
        // is emitted.
        for (0..bytes.len) |i| {
            if (bytes[i] == '\r' or bytes[i] == '\n') {
                bytes[i] = ' ';
            }
        }
        self.addReplyErr(bytes);
    }

    pub fn addReplyErr(self: *Client, err: []const u8) void {
        if (err.len == 0 or err[0] != '-') {
            self.addReplyString("-ERR ");
        }
        self.addReplyString(err);
        self.addReplyString("\r\n");
    }

    fn addReplyString(self: *Client, s: []const u8) void {
        if (!self.prepareClientToWrite()) return;
        if (self.addReplyStringToBuffer(s)) {
            return;
        }
        self.addReplyStringToList(s);
    }

    fn addReplyStringToBuffer(self: *Client, s: []const u8) bool {
        if (self.flags & Server.CLIENT_CLOSE_AFTER_REPLY != 0) {
            return true;
        }

        // If there already are entries in the reply list, we cannot
        // add anything more to the static buffer.
        if (self.reply.len > 0) {
            return false;
        }

        // Check that the buffer has enough space available for this string.
        const avail = self.buf.len - self.bufpos;
        if (s.len > avail) {
            return false;
        }

        memcpy(self.buf[self.bufpos..], s, s.len);
        self.bufpos += s.len;
        return true;
    }

    fn addReplyStringToList(self: *Client, s: []const u8) void {
        var len = s.len;
        var pos: usize = 0;
        const tail = if (self.reply.last) |last| last.value else null;

        if (tail) |rb| {
            // Copy the part we can fit into the tail, and leave the rest for a
            // new node
            const avail = rb.size - rb.used;
            const copy = if (avail >= len) len else avail;
            const buf = rb.buf();
            memcpy(buf[rb.used..], s, copy);
            rb.used += copy;
            len -= copy;
            pos += copy;
        }
        if (len > 0) {
            // Create a new node, make sure it is allocated to at
            // least PROTO_REPLY_CHUNK_BYTES
            const size = if (len < Server.PROTO_REPLY_CHUNK_BYTES)
                Server.PROTO_REPLY_CHUNK_BYTES
            else
                len;
            const rb = ReplyBlock.create(size);
            const buf = rb.buf();
            memcpy(buf, s[pos..], len);
            rb.used = len;
            self.reply.append(rb);
            self.reply_bytes += rb.size;
        }
    }

    fn prepareClientToWrite(self: *Client) bool {
        // Schedule the client to write the output buffers to the socket, unless
        // it should already be setup to do so (it has already pending data).
        if (!self.hasPendingReplies()) {
            self.installWriteHandler();
        }
        return true;
    }

    fn hasPendingReplies(self: *Client) bool {
        return self.bufpos > 0 or self.reply.len > 0;
    }

    fn installWriteHandler(self: *Client) void {
        if (self.flags & Server.CLIENT_PENDING_WRITE == 0) {
            // Here instead of installing the write handler, we just flag the
            // client and put it into a list of clients that have something
            // to write to the socket. This way before re-entering the event
            // loop, we can try to directly write to the client sockets avoiding
            // a system call. We'll only really install the write handler if
            // we'll not be able to write the whole reply at once.
            self.flags |= Server.CLIENT_PENDING_WRITE;
            server.clients_pending_write.append(self);
        }
    }

    /// Write data in output buffers to client. Return true if the client
    /// is still valid after the call, false if it was freed.
    fn write(self: *Client, handler_installed: bool) !bool {
        var totwritten: usize = 0;
        while (self.hasPendingReplies()) {
            if (self.bufpos > 0) {
                const bytes = self.buf[self.sentlen..self.bufpos];
                const nwritten = posix.write(self.fd, bytes) catch |err| {
                    if (err != posix.WriteError.WouldBlock) {
                        log.debug("Error writing to client: {}", .{err});
                        self.free();
                        return false;
                    }
                    break;
                };
                if (nwritten == 0) break;
                self.sentlen += nwritten;
                totwritten += nwritten;

                // If the buffer was sent, set bufpos to zero to continue with
                // the remainder of the reply.
                if (self.sentlen == self.bufpos) {
                    self.bufpos = 0;
                    self.sentlen = 0;
                }
            } else {
                const first = self.reply.first.?;
                const obj = first.value;
                const objlen = obj.used;
                if (objlen == 0) {
                    self.reply_bytes -= obj.size;
                    self.reply.removeNode(first);
                    continue;
                }
                const bytes = obj.buf()[self.sentlen..objlen];
                const nwritten = posix.write(self.fd, bytes) catch |err| {
                    if (err != posix.WriteError.WouldBlock) {
                        log.debug("Error writing to client: {}", .{err});
                        self.free();
                        return false;
                    }
                    break;
                };
                if (nwritten == 0) break;
                self.sentlen += nwritten;
                totwritten += nwritten;
                // If we fully sent the object on head go to the next one
                if (self.sentlen == objlen) {
                    self.reply_bytes -= obj.size;
                    self.reply.removeNode(first);
                    self.sentlen = 0;
                    // If there are no longer objects in the list, we expect
                    // the count of reply bytes to be exactly zero.
                    if (self.reply.len == 0) {
                        assert(self.reply_bytes == 0);
                    }
                }
            }

            // Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
            // bytes, in a single threaded server it's a good idea to serve
            // other clients as well, even if a very large request comes from
            // super fast link that is always able to accept data (in real world
            // scenario think about 'KEYS *' against the loopback interface).
            //
            // However if we are over the maxmemory limit we ignore that and
            // just deliver as much data as it is possible to deliver.
            const used_memory = 0; // TODO: allocator.used;
            if (totwritten > Server.NET_MAX_WRITES_PER_EVENT and
                (server.maxmemory == 0 or used_memory < server.maxmemory))
            {
                break;
            }
        }
        if (totwritten > 0) {
            self.lastinteraction = server.unixtime.get();
        }
        if (!self.hasPendingReplies()) {
            self.sentlen = 0;
            if (handler_installed) {
                try server.el.deleteFileEvent(self.fd, ae.WRITABLE);
            }
            // Close connection after entire reply has been sent.
            if (self.flags & Server.CLIENT_CLOSE_AFTER_REPLY != 0) {
                self.free();
                return false;
            }
        }
        return true;
    }

    pub fn free(self: *Client) void {
        sds.free(self.querybuf);
        self.querybuf = undefined;

        if (self.flags & Server.CLIENT_BLOCKED != 0) {
            blocked.unblockClient(self);
        }
        self.bpop.keys.destroy();

        self.reply.release();
        self.freeArgv();
        self.unlink();

        allocator.destroy(self);
    }

    /// Schedule a client to free it at a safe time in the Server.serverCron()
    /// function. This function is useful when we need to terminate a client
    /// but we are in a context where calling Client.free() is not possible,
    /// because the client should be valid for the continuation of the flow
    /// of the program.
    pub fn freeAsync(self: *Client) void {
        if (self.flags & Server.CLIENT_CLOSE_ASAP != 0) return;
        self.flags |= Server.CLIENT_CLOSE_ASAP;
        server.clients_to_close.append(self);
    }
};

pub fn acceptHandler(
    el: *ae.EventLoop,
    fd: i32,
    client_data: ae.ClientData,
    mask: i32,
) !void {
    _ = el;
    _ = client_data;
    _ = mask;

    var calls: usize = MAX_ACCEPTS_PER_CALL;
    while (calls > 0) : (calls -= 1) {
        const csk = anet.accept(fd) catch |err| {
            if (err != posix.AcceptError.WouldBlock) {
                log.warn("Accepting client connection: {}", .{err});
            }
            return;
        };
        var buffer: [64]u8 = undefined;
        const addr = try csk.getAddr(&buffer);
        var flags: i32 = 0;
        if (csk.addr.any.family == posix.AF.UNIX) {
            flags |= Server.CLIENT_UNIX_SOCKET;
            log.info("Accepted connection to {s}", .{server.unixsocket.?});
        } else {
            log.info("Accepted {s}", .{addr});
        }

        const c = Client.create(
            csk.fd,
            flags,
        ) catch |err| {
            log.warn(
                "Error registering fd event for the new client: {} (fd={})",
                .{ err, fd },
            );
            posix.close(csk.fd);
            return;
        };

        // If maxclient directive is set and this is one client more... close the
        // connection. Note that we create the client instead to check before
        // for this condition, since now the socket is already set in non-blocking
        // mode and we can send an error for free using the Kernel I/O
        if (server.clients.len > server.maxclients) {
            // That's a best effort error message, don't check write errors
            _ = posix.write(
                c.fd,
                "-ERR max number of clients reached\r\n",
            ) catch {};
            c.free();
            return;
        }

        // If the server is running in protected mode (the default) and there
        // is no password set, nor a specific interface is bound, we don't accept
        // requests from non loopback interfaces. Instead we try to explain the
        // user what to do to fix it if needed.
        if (server.protected_mode and
            server.bindaddr_count == 0 and
            server.requirepass == null and
            (c.flags & Server.CLIENT_UNIX_SOCKET == 0))
        {
            if (!std.mem.eql(u8, "127.0.0.1", addr) and !std.mem.eql(u8, "::1", addr)) {
                _ = posix.write(c.fd, ERR_DENIED) catch {};
                c.free();
                return;
            }
        }
    }
}

pub fn freeClientsInAsyncFreeQueue() void {
    var it = server.clients_to_close.iterator(.forward);
    while (it.next()) |ln| {
        const cli = ln.value;
        cli.flags &= ~@as(i32, Server.CLIENT_CLOSE_ASAP);
        cli.free();
        server.clients_to_close.removeNode(
            ln,
        );
    }
}

/// This function is called just before entering the event loop, in the hope
/// we can just write the replies to the client output buffer without any
/// need to use a syscall in order to install the writable event handler,
/// get it called, and so forth.
pub fn handleClientsWithPendingWrites() !usize {
    const processed = server.clients_pending_write.len;

    var iter = server.clients_pending_write.iterator(.forward);
    while (iter.next()) |ln| {
        const cli = ln.value;
        cli.flags &= ~@as(i32, Server.CLIENT_PENDING_WRITE);
        server.clients_pending_write.removeNode(
            ln,
        );

        // Try to write buffers to the client socket.
        if (!try cli.write(false)) continue;

        // If after the synchronous writes above we still have data to
        // output to the client, we need to install the writable handler.
        if (cli.hasPendingReplies()) {
            server.el.createFileEvent(
                cli.fd,
                ae.WRITABLE,
                sendReplyToClient,
                cli,
            ) catch {
                cli.freeAsync();
            };
        }
    }

    return processed;
}

fn readQueryFromClient(
    el: *ae.EventLoop,
    fd: i32,
    client_data: ae.ClientData,
    mask: i32,
) !void {
    _ = el;
    _ = mask;

    const cli: *Client = @ptrCast(@alignCast(client_data.?));

    var readlen: usize = Server.PROTO_IOBUF_LEN;

    // If this is a multi bulk request, and we are processing a bulk reply
    // that is large enough, try to maximize the probability that the query
    // buffer contains exactly the SDS string representing the object, even
    // at the risk of requiring more read(2) calls. This way the function
    // Client.processMultiBulkBuffer() can avoid copying buffers to create the
    // Redis Object representing the argument.
    if (cli.reqtype == Server.PROTO_REQ_MULTIBULK and cli.multibulklen != 0 and
        cli.bulklen != -1 and cli.bulklen >= Server.PROTO_MBULK_BIG_ARG)
    {
        const remaining = (cli.bulklen + 2) - @as(i64, @intCast(sds.getLen(cli.querybuf)));

        // Note that the 'remaining' variable may be zero in some edge case,
        // for example once we resume a blocked client after CLIENT PAUSE.
        if (remaining > 0 and remaining < readlen) {
            readlen = @intCast(remaining);
        }
    }

    const qblen = sds.getLen(cli.querybuf);
    if (cli.querybuf_peak < qblen) {
        cli.querybuf_peak = qblen;
    }
    cli.querybuf = sds.makeRoomFor(cli.querybuf, readlen);
    const nread = posix.read(
        fd,
        cli.querybuf[qblen .. qblen + readlen],
    ) catch |err| switch (err) {
        posix.ReadError.WouldBlock => return,
        else => {
            log.info("Reading from client: {}", .{err});
            cli.free();
            return;
        },
    };
    if (nread == 0) {
        log.info("Client closed connection", .{});
        cli.free();
        return;
    }

    sds.incrLen(cli.querybuf, @intCast(nread));
    cli.lastinteraction = server.unixtime.get();
    if (sds.getLen(cli.querybuf) > server.client_max_querybuf_len) {
        defer cli.free();
        var bytes = sds.empty();
        defer sds.free(bytes);
        bytes = sds.catRepr(bytes, cli.querybuf[0..64]);
        log.warn(
            "Closing client that reached max query buffer length (qbuf initial bytes: {s})",
            .{sds.asBytes(bytes)},
        );
        return;
    }

    cli.processInputBuffer();
}

/// Write event handler. Just send data to the client.
fn sendReplyToClient(
    el: *ae.EventLoop,
    fd: i32,
    client_data: ae.ClientData,
    mask: i32,
) !void {
    _ = el;
    _ = fd;
    _ = mask;
    const cli: *Client = @ptrCast(@alignCast(client_data.?));
    _ = try cli.write(true);
}

const std = @import("std");
const allocator = @import("allocator.zig");
const ae = @import("ae.zig");
const posix = std.posix;
const anet = @import("anet.zig");
const log = std.log.scoped(.networking);
const Server = @import("Server.zig");
const db = @import("db.zig");
const sds = @import("sds.zig");
const Object = @import("Object.zig");
const server = &Server.instance;
const util = @import("util.zig");
const LinkedList = @import("adlist.zig").List;
const memzig = @import("mem.zig");
const memcpy = memzig.memcpy;
const blocked = @import("blocked.zig");
const dict = @import("dict.zig");
const assert = std.debug.assert;
