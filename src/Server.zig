// Static server configuration
pub const CONFIG_DEFAULT_DYNAMIC_HZ = true; // Adapt hz to # of clients.
pub const CONFIG_DEFAULT_HZ = 10; // Time interrupt calls/sec.
pub const CONFIG_MIN_HZ = 1;
pub const CONFIG_MAX_HZ = 500;
pub const CONFIG_DEFAULT_TCP_BACKLOG = 511; // TCP listen backlog.
pub const CONFIG_DEFAULT_CLIENT_TIMEOUT = 0; // Default client timeout: infinite
pub const CONFIG_DEFAULT_SERVER_PORT = 6379; // TCP port.
pub const CONFIG_DEFAULT_MAX_CLIENTS = 10000;
pub const CONFIG_DEFAULT_DBNUM = 16;
pub const CONFIG_DEFAULT_MAXMEMORY = 0;
pub const CONFIG_DEFAULT_MAXMEMORY_SAMPLES = 5;
pub const CONFIG_DEFAULT_LFU_LOG_FACTOR = 10;
pub const CONFIG_DEFAULT_LFU_DECAY_TIME = 1;
pub const CONFIG_MIN_RESERVED_FDS = 32;
pub const CONFIG_DEFAULT_UNIX_SOCKET_PERM = 0;
pub const CONFIG_BINDADDR_MAX = 16;
pub const CONFIG_MAX_LINE = 1024;
pub const CONFIG_DEFAULT_PROTECTED_MODE = true;
pub const CONFIG_DEFAULT_TCP_KEEPALIVE = 300;
pub const CONFIG_DEFAULT_PROTO_MAX_BULK_LEN = (512 * 1024 * 1024); // Bulk request max size
pub const CONFIG_AUTHPASS_MAX_LEN = 512;
pub const CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE = false;
pub const MAX_CLIENTS_PER_CLOCK_TICK = 200; // HZ is adapted based on that.
pub const OBJ_SHARED_INTEGERS = 10000;
pub const OBJ_SHARED_BULKHDR_LEN = 32;
pub const NET_MAX_WRITES_PER_EVENT = (1024 * 64);

// Protocol and I/O related defines
pub const PROTO_MAX_QUERYBUF_LEN = 1024 * 1024 * 1024; // 1GB max query buffer.
pub const PROTO_IOBUF_LEN = 1024 * 16; // Generic I/O buffer size
pub const PROTO_MBULK_BIG_ARG = (1024 * 32);
pub const PROTO_INLINE_MAX_SIZE = (1024 * 64); // Max size of inline reads
pub const PROTO_REPLY_CHUNK_BYTES = 16 * 1024; // 16k output buffer

// Client request types
pub const PROTO_REQ_INLINE = 1;
pub const PROTO_REQ_MULTIBULK = 2;

// Client flags
pub const CLIENT_CLOSE_AFTER_REPLY = (1 << 6); // Close after writing entire reply.
pub const CLIENT_CLOSE_ASAP = (1 << 10); // Close this client ASAP
pub const CLIENT_UNIX_SOCKET = (1 << 11); // Client connected via Unix domain socket
pub const CLIENT_PENDING_WRITE = (1 << 21); // Client has output to send but a write handler is yet not installed.

// Units
pub const UNIT_SECONDS = 0;
pub const UNIT_MILLISECONDS = 1;

// SHUTDOWN flags
pub const SHUTDOWN_NOFLAGS = 0; // No flags.

pub const LRU_BITS = 24;
pub const LRU_CLOCK_MAX = ((1 << LRU_BITS) - 1); // Max value of obj->lru
pub const LRU_CLOCK_RESOLUTION = 1000; // LRU clock resolution in ms

// Redis maxmemory strategies. Instead of using just incremental number
// for this defines, we use a set of flags so that testing for certain
// properties common to multiple policies is faster.
pub const MAXMEMORY_FLAG_LRU = (1 << 0);
pub const MAXMEMORY_FLAG_LFU = (1 << 1);
pub const MAXMEMORY_FLAG_ALLKEYS = (1 << 2);
pub const MAXMEMORY_FLAG_NO_SHARED_INTEGERS = MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU;

pub const MAXMEMORY_VOLATILE_LRU = ((0 << 8) | MAXMEMORY_FLAG_LRU);
pub const MAXMEMORY_VOLATILE_LFU = ((1 << 8) | MAXMEMORY_FLAG_LFU);
pub const MAXMEMORY_VOLATILE_TTL = (2 << 8);
pub const MAXMEMORY_VOLATILE_RANDOM = (3 << 8);
pub const MAXMEMORY_ALLKEYS_LRU = ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_ALLKEYS_LFU = ((5 << 8) | MAXMEMORY_FLAG_LFU | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_ALLKEYS_RANDOM = ((6 << 8) | MAXMEMORY_FLAG_ALLKEYS);
pub const MAXMEMORY_NO_EVICTION = 7 << 8;

pub const CONFIG_DEFAULT_MAXMEMORY_POLICY = MAXMEMORY_NO_EVICTION;

pub const LOOKUP_NONE = 0;
pub const LOOKUP_NOTOUCH = (1 << 0);

// When configuring the server eventloop, we setup it so that the total number
// of file descriptors we can handle are server.maxclients + RESERVED_FDS +
// a few more to stay safe. Since RESERVED_FDS defaults to 32, we add 96
// in order to make sure of not over provisioning more than 128 fds.
pub const CONFIG_FDSET_INCR = CONFIG_MIN_RESERVED_FDS + 96;

pub const Command = struct {
    pub const Proc = *const fn (cli: *Client) void;

    name: []const u8,
    proc: Proc,
    arity: i32,
};

/// Our command table.
///
/// Every entry is composed of the following fields:
///
/// name: a string representing the command name.
/// proc: pointer to the function implementing the command.
/// arity: number of arguments, it is possible to use -N to say >= N.
const commandTable = [_]Command{
    .{ .name = "ping", .proc = pingCommand, .arity = -1 },
    .{ .name = "auth", .proc = authCommand, .arity = 2 },
    .{ .name = "select", .proc = selectCommand, .arity = 2 },
    .{ .name = "ttl", .proc = ttlCommand, .arity = 2 },
    .{ .name = "pttl", .proc = pttlCommand, .arity = 2 },
    .{ .name = "set", .proc = setCommand, .arity = -3 },
    .{ .name = "get", .proc = getCommand, .arity = 2 },
};

pub var shared: SharedObjects = undefined;

const SharedObjects = struct {
    crlf: *Object,
    ok: *Object,
    err: *Object,
    emptybulk: *Object,
    czero: *Object,
    cone: *Object,
    cnegone: *Object,
    nullbulk: *Object,
    nullmultibulk: *Object,
    emptymultibulk: *Object,
    pong: *Object,
    queued: *Object,
    emptyscan: *Object,
    wrongtypeerr: *Object,
    nokeyerr: *Object,
    syntaxerr: *Object,
    sameobjecterr: *Object,
    outofrangeerr: *Object,
    noautherr: *Object,
    oomerr: *Object,
    execaborterr: *Object,
    busykeyerr: *Object,
    space: *Object,
    colon: *Object,
    plus: *Object,
    integers: [OBJ_SHARED_INTEGERS]*Object,
    bulkhdr: [OBJ_SHARED_BULKHDR_LEN]*Object, // $<value>\r\n
    mbulkhdr: [OBJ_SHARED_BULKHDR_LEN]*Object, // *<value>\r\n
    minstring: sds.String,
    maxstring: sds.String,

    fn create() void {
        shared.crlf = Object.create(
            .string,
            sds.new("\r\n"),
        );
        shared.ok = Object.create(
            .string,
            sds.new("+OK\r\n"),
        );
        shared.err = Object.create(
            .string,
            sds.new("-ERR\r\n"),
        );
        shared.emptybulk = Object.create(
            .string,
            sds.new("$0\r\n\r\n"),
        );
        shared.czero = Object.create(
            .string,
            sds.new(":0\r\n"),
        );
        shared.cone = Object.create(
            .string,
            sds.new(":1\r\n"),
        );
        shared.cnegone = Object.create(
            .string,
            sds.new(":-1\r\n"),
        );
        shared.nullbulk = Object.create(
            .string,
            sds.new("$-1\r\n"),
        );
        shared.nullmultibulk = Object.create(
            .string,
            sds.new("*-1\r\n"),
        );
        shared.emptymultibulk = Object.create(
            .string,
            sds.new("*0\r\n"),
        );
        shared.pong = Object.create(
            .string,
            sds.new("+PONG\r\n"),
        );
        shared.queued = Object.create(
            .string,
            sds.new("+QUEUED\r\n"),
        );
        shared.emptyscan = Object.create(
            .string,
            sds.new("*2\r\n$1\r\n0\r\n*0\r\n"),
        );
        shared.wrongtypeerr = Object.create(
            .string,
            sds.new(
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            ),
        );
        shared.nokeyerr = Object.create(
            .string,
            sds.new("-ERR no such key\r\n"),
        );
        shared.syntaxerr = Object.create(
            .string,
            sds.new("-ERR syntax error\r\n"),
        );
        shared.sameobjecterr = Object.create(
            .string,
            sds.new(
                "-ERR source and destination objects are the same\r\n",
            ),
        );
        shared.outofrangeerr = Object.create(
            .string,
            sds.new("-ERR index out of range\r\n"),
        );
        shared.noautherr = Object.create(
            .string,
            sds.new("-NOAUTH Authentication required.\r\n"),
        );
        shared.oomerr = Object.create(
            .string,
            sds.new(
                "-OOM command not allowed when used memory > 'maxmemory'.\r\n",
            ),
        );
        shared.execaborterr = Object.create(
            .string,
            sds.new(
                "-EXECABORT Transaction discarded because of previous errors.\r\n",
            ),
        );
        shared.busykeyerr = Object.create(
            .string,
            sds.new("-BUSYKEY Target key name already exists.\r\n"),
        );
        shared.space = Object.create(
            .string,
            sds.new(" "),
        );
        shared.colon = Object.create(
            .string,
            sds.new(":"),
        );
        shared.plus = Object.create(
            .string,
            sds.new("+"),
        );
        for (0..OBJ_SHARED_INTEGERS) |i| {
            var obj = Object.createInt(@intCast(i));
            shared.integers[i] = obj.makeShared();
        }
        for (0..OBJ_SHARED_BULKHDR_LEN) |i| {
            shared.bulkhdr[i] = Object.create(
                .string,
                sds.catPrintf(sds.empty(), "${}\r\n", .{i}),
            );
            shared.mbulkhdr[i] = Object.create(
                .string,
                sds.catPrintf(sds.empty(), "*{}\r\n", .{i}),
            );
        }
        shared.minstring = sds.new("minstring");
        shared.maxstring = sds.new("maxstring");
    }

    fn destroy() void {
        shared.crlf.decrRefCount();
        shared.ok.decrRefCount();
        shared.err.decrRefCount();
        shared.emptybulk.decrRefCount();
        shared.czero.decrRefCount();
        shared.cone.decrRefCount();
        shared.cnegone.decrRefCount();
        shared.nullbulk.decrRefCount();
        shared.nullmultibulk.decrRefCount();
        shared.emptymultibulk.decrRefCount();
        shared.pong.decrRefCount();
        shared.queued.decrRefCount();
        shared.emptyscan.decrRefCount();
        shared.wrongtypeerr.decrRefCount();
        shared.nokeyerr.decrRefCount();
        shared.syntaxerr.decrRefCount();
        shared.sameobjecterr.decrRefCount();
        shared.outofrangeerr.decrRefCount();
        shared.noautherr.decrRefCount();
        shared.oomerr.decrRefCount();
        shared.execaborterr.decrRefCount();
        shared.busykeyerr.decrRefCount();
        shared.space.decrRefCount();
        shared.colon.decrRefCount();
        shared.plus.decrRefCount();
        for (shared.integers) |obj| obj.free();
        for (shared.bulkhdr) |obj| obj.decrRefCount();
        for (shared.mbulkhdr) |obj| obj.decrRefCount();
        sds.free(shared.minstring);
        sds.free(shared.maxstring);
        shared = undefined;
    }
};

const commandTableDictVTable: *const Dict.VTable = &.{
    .hash = dictSdsCaseHash,
    .eql = dictSdsCaseEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = dictSdsFree,
    .freeVal = null,
};

// Database.dict, keys are sds strings, vals are Redis objects.
pub const dbDictVTable: *const Dict.VTable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = dictSdsFree,
    .freeVal = dictObjectFree,
};

// Database.expires
pub const expireDictVTable: *const Dict.VTable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = null,
    .freeVal = null,
};

/// Set dictionary type. Keys are SDS strings, values are ot used.
pub const setDictVTable: *const Dict.VTable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = dictSdsFree,
    .freeVal = null,
};

/// Sorted sets hash (note: a skiplist is used in addition to the hash table)
pub const zsetDictVTable: *const Dict.VTable = &.{
    .hash = dictSdsHash,
    .eql = dictSdsEql,
    .dupeKey = null,
    .dupeVal = null,
    .freeKey = null, //  sds.String shared & freed by skiplist
    .freeVal = null,
};

pub var instance: Server = undefined;

pub const ClientList = List(*Client, *Client);

const Server = @This();
// General
configfile: ?sds.String, // Absolute config file path.
el: *ae.EventLoop,
lruclock: atomic.Value(u32), // Clock for LRU eviction
shutdown_asap: bool, // SHUTDOWN needed ASAP
dynamic_hz: bool, // Change hz value depending on # of clients.
config_hz: u32, // Configured HZ value. May be different than the actual 'hz' field value if dynamic-hz is enabled.
hz: u32, // serverCron() calls frequency in hertz
arch_bits: i32, // 32 or 64 depending on @bitSizeOf(usize)
requirepass: ?[]u8, // Pass for AUTH command, or null
db: []Database,
commands: *Dict, // Command table
// Networking
clients: *ClientList, // List of active clients
clients_index: Rax, // Active clients dictionary by client ID.
clients_to_close: *ClientList, // Clients to close asynchronously
clients_pending_write: *ClientList, // There is to write or install handler.
next_client_id: atomic.Value(usize), // Next client unique ID. Incremental.
port: u16,
tcp_backlog: u32, // TCP listen() backlog
bindaddr: [CONFIG_BINDADDR_MAX][]const u8, // Addresses we should bind to
bindaddr_count: usize, // Number of addresses in []server.bindaddr
ipfd: [CONFIG_BINDADDR_MAX]i32, // TCP socket file descriptors
ipfd_count: usize, // Used slots in []ipfd
unixsocket: ?[]u8, // UNIX socket path
sofd: i32, // Unix socket file descriptor
unixsocketperm: std.posix.mode_t, // UNIX socket permission
protected_mode: bool, // Don't accept external connections.
fixed_time_expire: i64, // If > 0, expire keys against server.mstime.
// Configuration
active_expire_enabled: bool, // Can be disabled for testing purposes.
maxidletime: u32, // Client timeout in seconds
dbnum: u32, // Total number of configured DBs
tcpkeepalive: i32, // Set SO_KEEPALIVE if non-zero.
client_max_querybuf_len: usize, // Limit for client query buffer length
// Limits
maxclients: u32, // Max number of simultaneous clients
maxmemory: u64, // Max number of memory bytes to use
maxmemory_policy: i32, // Policy for key eviction
maxmemory_samples: i32, // Pricision of random sampling
proto_max_bulk_len: usize, // Protocol bulk length maximum size.
lfu_log_factor: i32, // LFU logarithmic counter factor.
lfu_decay_time: i32, // LFU counter decay factor.
// time cache
unixtime: atomic.Value(i64), // Unix time sampled every cron cycle.
mstime: i64, // 'unixtime' in milliseconds.
ustime: i64, // 'unixtime' in microseconds.
// Lazy free
lazyfree_lazy_expire: bool,

pub fn create(configfile: ?sds.String, options: ?sds.String) !void {
    server.configfile = configfile;
    server.config_hz = CONFIG_DEFAULT_HZ;
    server.arch_bits = @bitSizeOf(usize);
    server.requirepass = null;
    errdefer if (server.requirepass) |passwd| allocator.free(passwd);
    server.commands = Dict.create(commandTableDictVTable, null);
    errdefer server.commands.destroy();
    server.populateCommandTable();
    server.shutdown_asap = false;
    server.dynamic_hz = CONFIG_DEFAULT_DYNAMIC_HZ;
    server.port = CONFIG_DEFAULT_SERVER_PORT;
    server.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
    server.bindaddr = undefined;
    server.bindaddr_count = 0;
    errdefer {
        for (0..server.bindaddr_count) |i| {
            allocator.free(server.bindaddr[i]);
        }
    }
    server.ipfd = undefined;
    server.ipfd_count = 0;
    server.unixsocket = null;
    errdefer if (server.unixsocket) |unixsocket| allocator.free(unixsocket);
    server.sofd = -1;
    server.unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    server.protected_mode = CONFIG_DEFAULT_PROTECTED_MODE;
    server.fixed_time_expire = 0;
    server.active_expire_enabled = true;
    server.maxidletime = CONFIG_DEFAULT_CLIENT_TIMEOUT;
    server.dbnum = CONFIG_DEFAULT_DBNUM;
    server.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
    server.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;
    server.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    server.maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    server.maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    server.maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
    server.proto_max_bulk_len = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN;
    server.lfu_log_factor = CONFIG_DEFAULT_LFU_LOG_FACTOR;
    server.lfu_decay_time = CONFIG_DEFAULT_LFU_DECAY_TIME;
    server.lruclock = .init(0);
    server.lruclock.store(evict.getLRUClock());
    server.unixtime = .init(0);
    server.updateCachedTime();
    server.lazyfree_lazy_expire = CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE;

    try config.load(server, configfile, options);
    server.hz = server.config_hz;

    setupSignalHandlers();

    server.clients = ClientList.create(&.{});
    errdefer server.clients.release();
    server.clients_index = Rax.new();
    server.clients_pending_write = ClientList.create(&.{});
    errdefer server.clients_pending_write.release();
    server.clients_to_close = ClientList.create(&.{});
    errdefer server.clients_to_close.release();
    server.next_client_id = .init(1);

    SharedObjects.create();
    errdefer SharedObjects.destroy();

    try server.adjustOpenFilesLimit();
    server.el = try ae.EventLoop.create(
        @intCast(server.maxclients + CONFIG_FDSET_INCR),
    );
    errdefer server.el.destroy();

    server.db = allocator.alloc(Database, server.dbnum);
    errdefer allocator.free(server.db);
    var dbi: usize = 0;
    errdefer for (0..dbi + 1) |j| server.db[j].destroy();
    for (0..server.dbnum) |i| {
        server.db[i] = Database.create(i);
        dbi = i;
    }

    // Open the TCP listening socket for the user commands.
    if (server.port != 0) try server.listenToPort();

    // Open the listening Unix domain socket.
    if (server.unixsocket) |unixsocket| {
        posix.unlink(unixsocket) catch {};
        server.sofd = anet.unixServer(
            unixsocket,
            server.unixsocketperm,
            server.tcp_backlog,
        ) catch |err| {
            log.warn("Openning Unix socket: {}", .{err});
            return err;
        };
        try anet.nonBlock(server.sofd);
    }

    if (server.ipfd_count == 0 and server.sofd < 0) {
        log.warn("Configured to not listen anywhere.", .{});
        return error.NoListenSockets;
    }

    // 32 bit instances are limited to 4GB of address space, so if there is
    // no explicit limit in the user provided configuration we set a limit
    // at 3 GB using maxmemory with 'noeviction' policy'. This avoids
    // useless crashes of the Redis instance for out of memory.
    if (server.arch_bits == 32 and server.maxmemory == 0) {
        log.warn(
            "32 bit instance detected but no memory limit set. " ++
                "Setting 3 GB maxmemory limit with 'noeviction' policy now.",
            .{},
        );
        server.maxmemory = 3 * 1024 * 1024 * 1024; // 3GB
        server.maxmemory_policy = MAXMEMORY_NO_EVICTION;
    }

    try bio.init();
}

/// This function will try to raise the max number of open files accordingly to
/// the configured max number of clients. It also reserves a number of file
/// descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
/// persistence, listening sockets, log files and so forth.
///
/// If it will not be possible to set the limit accordingly to the configured
/// max number of clients, the function will do the reverse setting
/// server.maxclients to the value that we can actually handle.
fn adjustOpenFilesLimit(self: *Server) !void {
    const maxfiles: posix.rlim_t = self.maxclients + CONFIG_MIN_RESERVED_FDS;
    if (posix.getrlimit(posix.rlimit_resource.NOFILE)) |limit| {
        const oldlimit: posix.rlim_t = limit.cur;
        // Set the max number of files if the current limit is not enough
        // for our needs.
        if (oldlimit < maxfiles) {
            var setrlimit_error: anyerror = undefined;
            var bestlimit = maxfiles;
            while (bestlimit > oldlimit) {
                const decr_step: posix.rlim_t = 16;
                var rlimit = limit;
                rlimit.cur = bestlimit;
                rlimit.max = bestlimit;
                posix.setrlimit(posix.rlimit_resource.NOFILE, rlimit) catch |err| {
                    setrlimit_error = err;
                    if (bestlimit < decr_step) break;
                    bestlimit -= decr_step;
                    continue;
                };
                break;
            }
            if (bestlimit < oldlimit) bestlimit = oldlimit;
            if (bestlimit < maxfiles) {
                const old_maxclients = self.maxclients;
                self.maxclients = @intCast(bestlimit -| CONFIG_MIN_RESERVED_FDS);
                if (bestlimit <= CONFIG_MIN_RESERVED_FDS) {
                    log.warn(
                        "Your current 'ulimit -n' " ++
                            "of {} is not enough for the server to start. " ++
                            "Please increase your open file limit to at least to {}.",
                        .{ oldlimit, maxfiles },
                    );
                    return error.MaxOpenFilesLimitNotEnough;
                }
                log.warn(
                    "You requested maxclients of {} " ++
                        "requiring at least {} max file descriptors.",
                    .{ old_maxclients, maxfiles },
                );
                log.warn(
                    "Server can't set maximum open files to {} because of OS error: {}.",
                    .{ maxfiles, setrlimit_error },
                );
                log.warn("Current maximum open files is {}. " ++
                    "maxclients has been reduced to {} to compensate for " ++
                    "low ulimit. " ++
                    "If you need higher maxclients increase 'ulimit -n'.", .{
                    bestlimit,
                    self.maxclients,
                });
            } else {
                log.info(
                    "Increased maximum number of open files " ++
                        "to {} (it was originally set to {}).",
                    .{ maxfiles, oldlimit },
                );
            }
        }
    } else |err| {
        log.warn(
            "Unable to obtain the current NOFILE limit ({}), " ++
                "assuming 1024 and setting the max clients configuration accordingly.",
            .{err},
        );
        self.maxclients = 1024 - CONFIG_MIN_RESERVED_FDS;
    }
}

/// We take a cached value of the unix time in the global state because with
/// virtual memory and aging there is to store the current time in objects at
/// every object access, and accuracy is not needed. To access a global var is
/// a lot faster than calling std.time.microTimestamp().
fn updateCachedTime(self: *Server) void {
    self.ustime = std.time.microTimestamp();
    self.mstime = @divFloor(self.ustime, std.time.us_per_ms);
    const unixtime = @divFloor(self.mstime, std.time.ms_per_s);
    self.unixtime.store(unixtime);
}

/// Initialize a set of file descriptors to listen to the specified 'port'
/// binding the addresses specified in the Redis server configuration.
fn listenToPort(self: *Server) !void {
    // Force binding of 0.0.0.0/:: if no bind address is specified.
    var bindaddr = self.bindaddr;
    var bindaddr_count = self.bindaddr_count;
    if (bindaddr_count == 0) {
        bindaddr[0] = "0.0.0.0";
        bindaddr[1] = "::";
        bindaddr_count = 2;
    }
    var i: usize = 0;
    for (bindaddr[0..bindaddr_count]) |addr| {
        const fd = anet.tcpServer(
            self.port,
            addr,
            self.tcp_backlog,
        ) catch |err| {
            log.warn(
                "Could not create server TCP listening socket {s}:{}: {}",
                .{ addr, self.port, err },
            );
            continue;
        };
        self.ipfd[i] = fd;
        self.ipfd_count += 1;
        anet.nonBlock(fd) catch {};
        i += 1;
    }
    if (self.ipfd_count == 0) return error.NoListeningSockets;
}

fn setupSignalHandlers() void {
    // Zig can't compile c.SIG_IGN
    const SIG_IGN: *const fn (c_int) callconv(.c) void = @ptrFromInt(1);
    _ = c.signal(c.SIGHUP, SIG_IGN);
    _ = c.signal(c.SIGPIPE, SIG_IGN);

    var act: posix.Sigaction = .{
        .handler = .{
            .handler = sigShutdownHandler,
        },
        .mask = posix.sigemptyset(),
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.TERM, &act, null);
}

fn sigShutdownHandler(sig: i32) callconv(.c) void {
    switch (sig) {
        posix.SIG.INT => log.warn("Received SIGINT scheduling shutdown...", .{}),
        posix.SIG.TERM => log.warn("Received SIGTERM scheduling shutdown...", .{}),
        else => log.warn("Received shutdown signal, scheduling shutdown...", .{}),
    }
    if (server.shutdown_asap and sig == posix.SIG.INT) {
        log.warn("You insist... exiting now.", .{});
        //Exit with an error since this was not a clean shutdown.
        std.process.exit(1);
    }
    server.shutdown_asap = true;
}

pub fn up(self: *Server) !void {
    // Create an event handler for accepting new connections in TCP and Unix
    // domain sockets.
    for (self.ipfd[0..self.ipfd_count]) |fd| {
        self.el.createFileEvent(
            fd,
            ae.READABLE,
            networking.acceptHandler,
            null,
        ) catch |err| {
            log.err("Unrecoverable error creating server.ipfd file event.", .{});
            return err;
        };
    }
    if (self.sofd > 0) {
        self.el.createFileEvent(
            self.sofd,
            ae.READABLE,
            networking.acceptHandler,
            null,
        ) catch |err| {
            log.err("Unrecoverable error creating server.sofd file event.", .{});
            return err;
        };
    }

    _ = self.el.createTimeEvent(
        1,
        serverCron,
        null,
        null,
    );

    self.el.setBeforeSleepProc(beforeSleep);

    try self.el.main();
}

/// This function gets called every time Redis is entering the
/// main loop of the event driven library, that is, before to sleep
/// for ready file descriptors.
fn beforeSleep(el: *ae.EventLoop) !void {
    _ = el;
    // Handle write with pending output buffers.
    _ = try networking.handleClientsWithPendingWrites();
}

/// This is our timer interrupt, called server.hz times per second.
fn serverCron(
    el: *ae.EventLoop,
    id: i64,
    clicent_data: ae.ClientData,
) !i32 {
    _ = el;
    _ = id;
    _ = clicent_data;

    // Update the time cache.
    server.updateCachedTime();

    // Adapt the server.hz value to the number of configured clients. If we have
    // many clients, we want to call serverCron() with an higher frequency.
    if (server.dynamic_hz) {
        while (server.clients.len / server.hz > MAX_CLIENTS_PER_CLOCK_TICK) {
            server.hz *= 2;
            if (server.hz > CONFIG_MAX_HZ) {
                server.hz = CONFIG_MAX_HZ;
                break;
            }
        }
    }

    // We have just LRU_BITS bits per object for LRU information.
    // So we use an (eventually wrapping) LRU clock.
    //
    // Note that even if the counter wraps it's not a big problem,
    // everything will still work but some object will appear younger
    // to Redis. However for this to happen a given object should never be
    // touched for all the time needed to the counter to wrap, which is
    // not likely.
    //
    // Note that you can change the resolution altering the
    // LRU_CLOCK_RESOLUTION define.
    server.lruclock.store(evict.getLRUClock());

    // We received a SIGTERM, shutting down here in a safe way, as it is
    // not ok doing so inside the signal handler.
    if (server.shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS)) {
            std.process.exit(0);
        }
        log.warn(
            "SIGTERM received but errors trying to shut down the server, " ++
                "check the logs for more information",
            .{},
        );
        server.shutdown_asap = false;
    }

    // We need to do a few operations on clients asynchronously.
    clientsCron();

    // Handle background operations on Redis databases.
    databaseCron();

    // Close clients that need to be closed asynchronous.
    networking.freeClientsInAsyncFreeQueue();

    return @intCast(@divFloor(1000, server.hz));
}

fn prepareForShutdown(flags: i32) bool {
    _ = flags;
    log.warn("User requested shutdown..", .{});

    // Close the listening sockets. Apparently this allows faster restarts.
    closeListeningSockets(true);

    log.warn("Redis is now ready to exit, bye bye...", .{});
    return true;
}

fn closeListeningSockets(unlink_unix_socket: bool) void {
    for (0..server.ipfd_count) |i| {
        posix.close(server.ipfd[i]);
    }

    if (server.sofd != -1) posix.close(server.sofd);

    if (unlink_unix_socket and server.unixsocket != null) {
        log.info("Removing the unix socket file.", .{});
        // don't care if this fails
        posix.unlink(server.unixsocket.?) catch {};
    }
}

///  This function is called by serverCron() and is used in order to perform
/// operations on clients that are important to perform constantly. For instance
/// we use this function in order to disconnect clients after a timeout, including
/// clients blocked in some blocking command with a non-zero timeout.
///
/// The function makes some effort to process all the clients every second, even
/// if this cannot be strictly guaranteed, since serverCron() may be called with
/// an actual frequency lower than server.hz in case of latency events like slow
/// commands.
///
/// It is very important for this function, and the functions it calls, to be
/// very fast: sometimes Redis has tens of hundreds of connected clients, and the
/// default server.hz value is 10, so sometimes here we need to process thousands
/// of clients per second, turning this function into a source of latency.
fn clientsCron() void {
    // Try to process at least numclients/server.hz of clients
    // per call. Since normally (if there are no big latency events) this
    // function is called server.hz times per second, in the average case we
    // process all the clients in 1 second.
    const numclients = server.clients.len;
    var iterations: usize = @divFloor(numclients, server.hz);
    const now_ms = std.time.milliTimestamp();
    const CLIENTS_CRON_MIN_ITERATIONS = 5;

    // Process at least a few clients while we are at it, even if we need
    // to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
    // of processing each client once per second.
    if (iterations < CLIENTS_CRON_MIN_ITERATIONS) {
        iterations = if (numclients < CLIENTS_CRON_MIN_ITERATIONS)
            numclients
        else
            CLIENTS_CRON_MIN_ITERATIONS;
    }
    while (server.clients.len > 0 and iterations > 0) : (iterations -= 1) {
        // Rotate the list, take the current head, process.
        // This way if the client must be removed from the list it's the
        // first element and we don't incur into O(N) computation.
        server.clients.rotateTailToHead();
        const head = server.clients.first.?;
        const cli = head.value;

        // The following functions do different service checks on the client.
        // The protocol is that they return true if the client was
        // terminated.
        if (clientsCronHandleTimeout(cli, now_ms)) continue;
        if (clientsCronResizeQueryBuffer(cli)) continue;
    }
}

/// Check for timeouts. Returns true if the client was terminated.
fn clientsCronHandleTimeout(cli: *Client, now_ms: i64) bool {
    const now_sec: i64 = @divFloor(now_ms, std.time.ms_per_s);
    if (server.maxidletime > 0 and
        (now_sec - cli.lastinteraction > server.maxidletime))
    {
        log.debug("Closing idle client", .{});
        cli.free();
        return true;
    }
    return false;
}

/// The client query buffer is an sds.String that can end with a lot of
/// free space not used, this function reclaims space if needed.
///
/// The function always returns false as it never terminates the client.
fn clientsCronResizeQueryBuffer(cli: *Client) bool {
    const querybuf_size = sds.getAlloc(cli.querybuf);
    const idletime = server.unixtime.get() - cli.lastinteraction;

    // There are two conditions to resize the query buffer:
    // 1) Query buffer is > BIG_ARG and too big for latest peak.
    // 2) Query buffer is > BIG_ARG and client is idle.
    if (querybuf_size > PROTO_MBULK_BIG_ARG and
        (@divFloor(querybuf_size, cli.querybuf_peak + 1) > 2 or idletime > 2))
    {
        // Only resize the query buffer if it is actually wasting
        // at least a few kbytes.
        if (sds.getAvail(cli.querybuf) > 4 * 1024) {
            cli.querybuf = sds.removeAvailSpace(
                cli.querybuf,
            );
        }
    }

    // Reset the peak again to capture the peak memory usage in the next
    // cycle
    cli.querybuf_peak = 0;

    return false;
}

/// This function handles 'background' operations we are required to do
/// incrementally in Redis databases, such as active key expiring, resizing,
/// rehashing.
fn databaseCron() void {
    // TODO:
}

/// If this function gets called we already read a whole
/// command, arguments are in the client argv/argc fields.
/// processCommand() execute the command or prepare the
/// server for a bulk read from the client.
///
/// If true is returned the client is still alive and valid and
/// other operations can be performed by the caller. Otherwise
/// if false is returned the client was destroyed (i.e. after QUIT).
pub fn processCommand(self: *Server, cli: *Client) bool {
    const cmd: sds.String = sds.cast(cli.argv.?[0].data.ptr);
    if (std.ascii.eqlIgnoreCase(sds.asBytes(cmd), "quit")) {
        cli.addReply(shared.ok);
        cli.flags |= CLIENT_CLOSE_AFTER_REPLY;
        return false;
    }

    // Now lookup the command and check ASAP about trivial error conditions
    // such as wrong arity, bad command name and so forth.
    cli.cmd = self.lookupCommand(cmd);
    if (cli.cmd == null) {
        var args = sds.empty();
        defer sds.free(args);
        var i: usize = 1;
        while (i < cli.argc and sds.getLen(args) < 128) : (i += 1) {
            const arg: sds.String = @ptrCast(cli.argv.?[i].data.ptr);
            var remaining = 128 - sds.getLen(args);
            if (remaining > sds.getLen(arg)) {
                remaining = sds.getLen(arg);
            }
            args = sds.catPrintf(
                args,
                "`{s}`, ",
                .{arg[0..remaining]},
            );
        }
        cli.addReplyErrFormat(
            "unknown command: `{s}`, with args beginning with: {s}",
            .{
                sds.asBytes(cmd),
                sds.asBytes(args),
            },
        );
        return true;
    }
    const arity = cli.cmd.?.arity;
    if ((arity > 0 and arity != cli.argc) or (cli.argc < -arity)) {
        cli.addReplyErrFormat(
            "wrong number of arguments for '{s}' command",
            .{cli.cmd.?.name},
        );
        return true;
    }

    // Check if the user is authenticated
    if (server.requirepass != null and !cli.authenticated and
        cli.cmd.?.proc != authCommand)
    {
        cli.addReply(shared.noautherr);
        return true;
    }

    self.call(cli, 0);

    return true;
}

pub fn lookupCommand(self: *Server, cmd: sds.String) ?*Command {
    const command = self.commands.fetchValue(cmd);
    if (command) |ptr| {
        return @ptrCast(@alignCast(ptr));
    }
    return null;
}

fn populateCommandTable(self: *Server) void {
    for (0..commandTable.len) |i| {
        const command = &commandTable[i];
        const ok = self.commands.add(
            sds.new(command.name),
            @constCast(command),
        );
        std.debug.assert(ok);
    }
}

pub fn call(self: *Server, cli: *Client, flags: i32) void {
    _ = flags;
    self.fixed_time_expire +%= 1;
    self.updateCachedTime();
    cli.cmd.?.proc(cli);
    self.fixed_time_expire -%= 1;
}

pub fn destroy() void {
    server.el.destroy();

    if (server.requirepass) |passwd| {
        allocator.free(passwd);
    }

    for (server.bindaddr[0..server.bindaddr_count]) |addr| {
        allocator.free(addr);
    }

    for (server.db) |*db| {
        db.destroy();
    }
    allocator.free(server.db);

    server.commands.destroy();

    var clients_iter = server.clients.iterator(.forward);
    while (clients_iter.next()) |node| {
        var cli = node.value;
        cli.free();
    }
    server.clients.release();
    server.clients_index.free();
    server.clients_pending_write.release();
    server.clients_to_close.release();

    for (server.ipfd[0..server.ipfd_count]) |fd| {
        posix.close(fd);
    }

    if (server.unixsocket) |unixsocket| {
        posix.unlink(unixsocket) catch {};
        allocator.free(unixsocket);
    }
    if (server.sofd != -1) posix.close(server.sofd);

    bio.deinit();

    SharedObjects.destroy();

    instance = undefined;
}

fn dictSdsHash(_: Dict.PrivData, key: Dict.Key) Dict.Hash {
    return Dict.genHash(sds.asBytes(sds.cast(key)));
}

fn dictSdsCaseHash(_: Dict.PrivData, key: Dict.Key) Dict.Hash {
    var stack_impl = std.heap.stackFallback(
        1024,
        allocator.child,
    );
    const stack_allocator = stack_impl.get();
    const dupkey = sds.dupeAlloc(stack_allocator, sds.cast(key));
    defer sds.freeAlloc(stack_allocator, dupkey);
    sds.toLower(dupkey);
    return Dict.genHash(sds.asBytes(dupkey));
}

fn dictSdsEql(_: Dict.PrivData, key1: Dict.Key, key2: Dict.Key) bool {
    return sds.cmp(sds.cast(key1), sds.cast(key2)) == .eq;
}

fn dictSdsCaseEql(_: Dict.PrivData, key1: Dict.Key, key2: Dict.Key) bool {
    return sds.caseCmp(sds.cast(key1), sds.cast(key2)) == .eq;
}

fn dictSdsFree(_: Dict.PrivData, key: Dict.Key) void {
    sds.free(sds.cast(key));
}

fn dictObjectFree(_: Dict.PrivData, val: Dict.Value) void {
    // Lazy freeing will set value to null.
    if (val) |ptr| {
        const obj: *Object = @ptrCast(@alignCast(ptr));
        obj.decrRefCount();
    }
}

fn authCommand(cli: *Client) void {
    if (server.requirepass == null) {
        cli.addReplyErrFormat(
            "Client sent AUTH, but no password is set",
            .{},
        );
        return;
    }
    const password = sds.cast(cli.argv.?[1].data.ptr);
    if (util.timeIndependentEql(
        CONFIG_AUTHPASS_MAX_LEN,
        sds.asBytes(password),
        server.requirepass.?,
    )) {
        cli.authenticated = true;
        cli.addReply(shared.ok);
        return;
    }
    cli.authenticated = false;
    cli.addReplyErr("invalid password");
}

fn pingCommand(cli: *Client) void {
    // The command takes zero or one arguments.
    if (cli.argc > 2) {
        cli.addReplyErrFormat(
            "wrong number of arguments for '{s}' command",
            .{cli.cmd.?.name},
        );
        return;
    }

    if (cli.argc == 1) {
        cli.addReply(shared.pong);
    } else {
        cli.addReply(cli.argv.?[1]);
    }
}

const selectCommand = dbc.selectCommand;
const ttlCommand = expirec.ttlCommand;
const pttlCommand = expirec.pttlCommand;
const setCommand = stringc.setCommand;
const getCommand = stringc.getCommand;

const server = &instance;
const Object = @import("Object.zig");
const std = @import("std");
const allocator = @import("allocator.zig");
const sds = @import("sds.zig");
const Dict = @import("Dict.zig");
const random = @import("random.zig");
const config = @import("config.zig");
const ae = @import("ae.zig");
const util = @import("util.zig");
const List = @import("adlist.zig").List;
const networking = @import("networking.zig");
const anet = @import("anet.zig");
const log = std.log.scoped(.server);
const evict = @import("evict.zig");
const posix = std.posix;
const atomic = @import("atomic.zig");
const Client = networking.Client;
const Database = @import("db.zig").Database;
const Rax = @import("rax/Rax.zig");
const c = @cImport({
    @cInclude("sys/signal.h");
});
const stringc = @import("t_string.zig");
const dbc = @import("db.zig");
const bio = @import("bio.zig");
const expirec = @import("expire.zig");
